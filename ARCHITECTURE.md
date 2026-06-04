# DIYnamo Architecture

Internal working doc. Captures the current module structure, the design choices that make the system extensible, and the decisions made during implementation.

---

## Current Architecture

The system is a distributed key-value store with sloppy-quorum replication, hinted handoff, and event-driven hint delivery. All seven planned implementation steps are complete.

**`src/store/`** owns everything related to local storage. It defines the `StorageEngine` trait (`get`, `put`) and two concrete implementations:

- `RocksDbStore` — the on-disk store. Values are persisted as `StoredEntry` (timestamp + data bytes), serialized with bincode. Exposes `put_if_newer` for read-repair writes (only overwrites if the incoming timestamp is strictly greater than the stored one) and `iter_all` for full-scan reconciliation.
- `HintStore` — a separate RocksDB instance at `{data_dir}/hints`. Keys are `{target_node_id}/{original_key}`; values are raw bytes. Used by the coordinator to park writes for nodes that are temporarily offline.

**`src/cluster/`** owns cluster membership and gossip metadata.

- `delegate.rs` — `NodeMeta` (18-byte fixed encoding: 16-byte startup UUID + 2-byte HTTP port big-endian) and `DiynamoNodeDelegate`, a `NodeDelegate` impl that advertises `NodeMeta` in every alive gossip message. Each node generates a fresh UUID on process start; a UUID change signals a process restart.
- `membership.rs` — `GossipNode` wraps `memberlist` with a `CompositeDelegate` that combines `DiynamoNodeDelegate` (node metadata) and `SubscribleEventDelegate` (event fan-out). `node_state_to_member` decodes `NodeMeta` from gossip meta bytes to populate `MemberInfo.forward_port` and `MemberInfo.uuid`. `GossipNode::subscribe()` hands out the `EventSubscriber` (one-shot; panics if called twice).
- `ring.rs` — `CoordinatorRing` wraps `hashring_coordinator` and exposes `preference_list_for_key(key, n)` (top N nodes in ring order) and `ring_order_for_key(key)` (all roster nodes, so `result[n..]` are hint candidates).
- `types.rs` — `MemberInfo` carries `id`, `gossip_addr`, `forward_port`, and `uuid`.

**`src/coordinator/`** owns distributed coordination.

- `mod.rs` — `ReplicatedStore` implements `StorageEngine` and wraps `RocksDbStore`, `GossipNode`, `HintStore`, and `CoordinatorRing`. `put` runs sloppy-quorum writes: it attempts all N preferred nodes in parallel and, for any that fail, walks hint candidates in ring order until one accepts a hinted write; it returns success if `real_acks + hint_acks >= W`. `get` reads all N preferred nodes in parallel, requires R successful responses, picks the highest-timestamp winner (LWW), and fire-and-forgets `put_internal_versioned_bytes` to any stale replicas. `ReplicatedStore::new` spawns the `HandoffTask` and stores the `JoinHandle`.
- `handoff.rs` — `HandoffTask` runs a background Tokio task driven by `GossipNode::subscribe()`. On every `Join` or `Update` event: (1) deliver all pending hints for that node via `KvClient::put_internal_bytes` and delete them on success; (2) if the node's UUID differs from the last-seen value, iterate `RocksDbStore::iter_all` and push every key whose preference list includes that node (UUID-change reconciliation for restarts and new nodes).

**`src/client/`** contains `KvClient`, a reqwest-based HTTP client used by both the CLI and the coordinator for all peer-to-peer calls. Internal endpoints (`/internal/kv/{key}`, `/internal/kv-versioned/{key}`, `/internal/hint/{target_id}/{key}`) bypass quorum and operate directly on the local store or hint store.

**`src/bin/`** contains the three entry points. `server.rs` generates a startup UUID, constructs `NodeMeta`, starts `GossipNode`, opens the data and hint stores, builds `ReplicatedStore` (which spawns `HandoffTask`), and serves four routes. `cli.rs` is an interactive client. `gossip.rs` is a standalone binary for exercising gossip in isolation.

```
src/
├── store/
│   ├── mod.rs           StorageEngine trait, VersionedValue, StoreConfig
│   ├── rocksdb_store.rs RocksDbStore: get, put, put_if_newer, iter_all
│   ├── hints.rs         HintStore: store_hint, hints_for_node, delete_hint
│   └── timestamp.rs     TimestampSource trait + SystemTimestamp
├── cluster/
│   ├── delegate.rs      NodeMeta (18-byte encoding), DiynamoNodeDelegate
│   ├── membership.rs    GossipNode (CompositeDelegate), MembershipView trait
│   ├── ring.rs          CoordinatorRing: preference_list_for_key, ring_order_for_key
│   ├── types.rs         MemberInfo (id, gossip_addr, forward_port, uuid)
│   └── printer.rs       run_live_set_printer, format_live_set
├── coordinator/
│   ├── mod.rs           ReplicatedStore: quorum put/get, sloppy quorum, read repair
│   └── handoff.rs       HandoffTask: hint delivery + UUID-change reconciliation
├── client/
│   └── http.rs          KvClient (put, get, put_internal_bytes, put_hint_bytes, …)
├── api/
│   └── types.rs         PutBody, PutVersionedBody, GetResponse (with timestamp)
├── config/
│   └── …                ResolvedServerConfig with n, w, r, data_dir, cluster.members
└── bin/
    ├── server.rs        HTTP server: /kv, /internal/kv, /internal/kv-versioned, /internal/hint
    ├── cli.rs           Interactive CLI client
    └── gossip.rs        Standalone gossip node (for testing)
```

---

## The Three Extension Points

These traits separate concerns and make the system testable at every layer.

**`StorageEngine`** is the seam between the HTTP layer and everything below it. `server.rs` holds an `Arc<ReplicatedStore>` (which implements `StorageEngine`) and a separate `Arc<RocksDbStore>` for direct local writes on the internal endpoints. The public HTTP handlers never touch RocksDB directly.

**`MembershipView`** is the seam between coordinator logic and the gossip implementation. `ReplicatedStore` calls `gossip.online_members()` on every quorum operation to filter the preference list to reachable nodes. The trait is implemented for both `GossipNode` and `Arc<GossipNode>`, so tests can substitute a mock without a live cluster.

**`TimestampSource`** decouples timestamp generation from wall-clock time. `RocksDbStore` takes a `Box<dyn TimestampSource>` at construction time. `SystemTimestamp` is used in production; a mock can return controlled values for deterministic LWW testing.

---

## How the Pieces Fit Together

```
HTTP client
    │
    ▼
server.rs ──► Arc<ReplicatedStore> ──────────────────────────────────┐
                    │                                                  │
              StorageEngine::put / get                         _handoff: JoinHandle
                    │                                                  │
          ┌─────────┼──────────────┐                      HandoffTask (background)
          │         │              │                       ├── EventSubscriber ◄── GossipNode
    local store   ring          hints                      ├── hints: Arc<HintStore>
  RocksDbStore  CoordinatorRing  HintStore                 └── iter_all on RocksDbStore
          │         │
        RocksDB   hashring_coordinator
                    │
              MembershipView
                    │
               GossipNode
              (CompositeDelegate:
               DiynamoNodeDelegate    ← broadcasts NodeMeta {uuid, http_port}
               SubscribleEventDelegate ← feeds HandoffTask)
```

**Write path (put):**
1. `preference_list_for_key(key, N)` → N nodes in ring order.
2. Parallel `put_internal_bytes` to each preferred node (or local `put` if self).
3. For each failed preferred node, walk hint candidates in ring order; store a hint on the first candidate that accepts it.
4. Return `Ok(())` if `real_acks + hint_acks >= W`; else `Err(QuorumFailed)` → HTTP 503.

**Read path (get):**
1. `preference_list_for_key(key, N)` → parallel `get_internal_versioned` from all N.
2. Require R successful responses; else `Err(QuorumFailed)`.
3. LWW: take the `Some(v)` with the highest `v.timestamp`.
4. Fire-and-forget `put_internal_versioned_bytes` (write-if-newer) to any replica that returned a stale or missing value.

**Hint delivery (HandoffTask):**
- Driven by gossip `Join` / `Update` events.
- On each event: deliver all pending hints for that node; if UUID changed, push every locally-held key in that node's preference list.

---

## Design Decisions

**Hint key layout uses a flat prefix, not a column family.** `{target_id}/{original_key}` in a single RocksDB instance is simpler to iterate by prefix and requires no column family management. The hint DB lives at `{data_dir}/hints`, separate from the main data DB, so iteration is already isolated.

**Sloppy quorum counts hints toward W.** A hinted write on a substitute node counts as an acknowledgement. This keeps availability high under partial failures at the cost of weaker consistency — the hint may be delayed in reaching its intended target. Dynamo calls this "sloppy quorum."

**Read repair uses write-if-newer, not unconditional put.** The receiving node's `/internal/kv-versioned` endpoint calls `put_if_newer`, which only writes if the incoming timestamp exceeds the stored one. This prevents a stale repair that was delayed in-flight from overwriting a newer external write that arrived in the interim.

**UUID-change detection replaces explicit join events for reconciliation.** The `HandoffTask` treats any UUID change (new value or first appearance) as a signal to push the full key range for that node. This handles both new nodes joining and nodes that restarted with empty storage, without requiring explicit "I lost my data" signalling.

**Any node can coordinate.** There is no dedicated coordinator role. `server.rs` wraps `ReplicatedStore` directly, so whichever node receives the client request coordinates that operation. This matches Dynamo's design more closely than the seed-only alternative.

---

## Out of Scope

**Merkle tree anti-entropy.** Read repair on `get` handles short-term divergence; hinted handoff and UUID-change reconciliation handle node-failure cases. Merkle trees would detect long-term silent divergence but are out of scope for this project.

**Vector clocks.** We use physical timestamps and last-write-wins. Silent data loss is possible when two writes land on different replicas within the same millisecond, but this greatly simplifies the implementation and the HTTP interface.

**Virtual node placement strategies.** Virtual nodes are distributed using `hashring_coordinator`'s default behavior. Dynamo's per-strategy placement policies are not implemented.

**Custom gossip implementation.** We use the `memberlist` crate. A hand-rolled gossip protocol remains a stretch goal.

**Clock skew.** `SystemTime` is used for LWW timestamps. Nodes with significantly skewed clocks may discard writes in a non-intuitive order. This is acceptable for evaluation; a production system would use hybrid logical clocks or NTP with bounded skew.
