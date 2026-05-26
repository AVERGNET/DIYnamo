# DIYnamo Architecture

Internal working doc. Captures the current module structure, the design choices that make the system extensible, and the planned path toward a distributed implementation.

---

## Current Architecture

The codebase is split into three areas of responsibility:

**`src/store/`** owns everything related to local storage. It defines the `StorageEngine` trait (`get`, `put`, `delete`) and the `RocksDbStore` type that implements it against RocksDB. Values are stored as versioned records — each write is stamped with a physical timestamp by an injected `TimestampSource`. The HTTP layer has no dependency on RocksDB directly; it only sees the trait.

**`src/cluster/`** owns cluster membership. `GossipNode` wraps the `memberlist` crate and exposes the live member set as a `Vec<MemberInfo>`. The `MembershipView` trait abstracts this so anything that needs a view of the cluster (future coordinator logic, tests) can depend on the trait rather than the concrete gossip implementation. `run_live_set_printer` is a thin utility for debugging cluster state.

**`src/bin/` and `src/client/`** are the entry points. `server.rs` is the HTTP server. It currently holds an `Arc<dyn StorageEngine>` and serves `GET /kv/{key}` and `PUT /kv/{key}`. `cli.rs` is an interactive client that talks to a running server over HTTP. `gossip.rs` is a standalone binary for exercising the gossip layer in isolation. The `client/` module contains `KvClient`, an async HTTP client that both the CLI and (eventually) the coordinator will use to talk to peer nodes.

```
src/
├── store/
│   ├── mod.rs           StorageEngine trait, VersionedValue, StoreConfig
│   ├── rocksdb_store.rs RocksDbStore: impl StorageEngine
│   └── timestamp.rs     TimestampSource trait + SystemTimestamp
├── cluster/
│   ├── membership.rs    GossipNode, MembershipView trait
│   ├── types.rs         MemberInfo
│   └── printer.rs       run_live_set_printer, format_live_set
├── client/
│   └── http.rs          KvClient (reqwest-based)
├── api/
│   └── types.rs         PutBody, GetResponse (shared HTTP types)
└── bin/
    ├── server.rs        HTTP server entry point
    ├── cli.rs           Interactive CLI client
    └── gossip.rs        Standalone gossip node (for testing)
```

---

## The Three Extension Points

These are the traits that let us evolve the system without rewriting the HTTP layer.

**`StorageEngine`** is the seam between the HTTP layer and everything below it. Today `server.rs` holds an `Arc<dyn StorageEngine>` backed by `RocksDbStore`. When we build the distributed layer, we will introduce a `ReplicatedStore` that also implements `StorageEngine`. Swapping it in requires changing one line in `server.rs` — the HTTP handlers stay untouched.

**`MembershipView`** is the seam between anything that needs to know about cluster topology and the gossip implementation. The coordinator will need to ask "who are the live nodes?" to build a preference list. By depending on `MembershipView` rather than `GossipNode` directly, the coordinator is testable without a running memberlist cluster — a mock that returns a fixed list of members is a straightforward substitute.

**`TimestampSource`** decouples timestamp generation from wall-clock time. `RocksDbStore` takes a `Box<dyn TimestampSource>` at construction time. In production, `SystemTimestamp` is used. In tests, a mock that returns controlled values makes last-write-wins behavior deterministic without any time manipulation.

---

## Planned Extension Path

Each step below is independent enough to be a separate PR. The key property in each case is that the HTTP layer (`server.rs`) does not need to change until the final coordinator step.

### Step 1 — Gossip integration into the server

`AppState` in `server.rs` gains a second field: `Arc<GossipNode>` alongside `Arc<dyn StorageEngine>`. At this point the server can see who is in the cluster but does not yet use that information for routing. This step is mostly infrastructure: ensuring the server starts a gossip node on a configurable bind address, joins seeds on startup, and shuts it down cleanly. It also establishes the pattern — `AppState` is the place where all shared cluster state lives.

### Step 2 — Consistent hash ring

We introduce a `RingView` component (backed by the `hash_ring` crate) that takes a `Vec<MemberInfo>` from gossip and answers "for this key, what is the ordered preference list of nodes?" The ring is not a long-lived stateful object — it is built from the current member list on each request. This keeps the implementation simple: no background ring-maintenance task, no cache invalidation problem. The preference list is just a sorted slice of `MemberInfo` used by the coordinator.

### Step 3 — `ReplicatedStore`

This is the core of the distributed implementation. `ReplicatedStore` implements `StorageEngine` and wraps three things: the local `RocksDbStore`, a `MembershipView` for the ring, and a `KvClient` for forwarding operations to peer nodes. When `put` is called, the coordinator computes the preference list for the key, issues writes to the top W nodes in parallel (including itself if it is in the preference list), and waits for W acknowledgements before returning success. `get` does the same for R reads, applies last-write-wins using `VersionedValue::timestamp` to pick the winner, and writes the winner back to any stale replicas (read repair). Swapping `RocksDbStore` for `ReplicatedStore` in `server.rs` is the only change the HTTP layer sees.

### Step 4 — Hinted handoff

When a write's preference list includes a node that is offline (not in `MembershipView::online_members`), the coordinator stores a *hint* locally: the intended target node id and the key/value pair. Hints live in a dedicated RocksDB column family rather than the main data column family, so they are easy to enumerate and delete without interfering with normal reads. A background Tokio task runs on each node and periodically checks gossip for nodes that have come back online. When a previously-offline node reappears, the task replays any hints destined for it via `KvClient::put` and deletes them from the hints store on success.

### Step 5 — Node reconciliation

When a node that was fully offline (data lost) or a brand-new node joins the ring, it starts with an empty local store. The gossip join event is visible via `MembershipView`. The new node (or a seed node on its behalf) needs to pull the key range it now owns. The mechanism is: iterate over all keys in the local RocksDB, compute the preference list for each key, and push any key where the new node appears in the preference list. This is a background process and does not affect availability for ongoing requests.

```
Current:
  server.rs  ──►  Arc<dyn StorageEngine>  ──►  RocksDbStore  ──►  RocksDB

After step 3:
  server.rs  ──►  Arc<dyn StorageEngine>
                        │
                        ▼
                  ReplicatedStore
                 /       |        \
      RocksDbStore   RingView   KvClient(s)
            │            │
          RocksDB    MembershipView
                          │
                       GossipNode
```

---

## Out of Scope

**Merkle tree anti-entropy.** The Dynamo paper uses Merkle trees to efficiently detect and repair long-term divergence between replicas. We are not implementing this. Read repair on `get` handles short-term divergence, and hinted handoff handles the node-failure case. Merkle trees would be the right next step for production hardening but are out of scope for this project.

**Vector clocks.** The paper uses vector clocks to track causal history and surfaces concurrent writes to the client for application-level resolution. We replace this with physical timestamps and last-write-wins. This means silent data loss is possible when two writes happen to different replicas within the same millisecond, but it greatly simplifies the implementation and the HTTP interface.

**Virtual node placement strategies.** Dynamo implements multiple strategies for distributing virtual nodes around the ring to control load balance. We distribute virtual nodes randomly using `hash_ring`'s default behavior.

**Custom gossip implementation.** We use `memberlist` for gossip. Replacing it with a hand-rolled implementation is a stretch goal noted in the proposal but not planned.

---

## Open Questions

- **Hint storage format.** Should hints be stored as a separate RocksDB column family, or as a separate key prefix in the default column family? Column family is cleaner isolation; prefix is simpler to implement now.

- **Coordinator identity.** The proposal says seed nodes act as coordinators for requests they service. Do we route all client writes to a seed, or can any node coordinate for its own key range? The former is simpler; the latter matches Dynamo more closely.

- **Quorum failure behavior.** If we cannot reach W nodes for a write, do we fail the request or accept it and rely on repair? The proposal implies failing the write when W can't be reached, but hinted handoff complicates this — a hint counts as a write to an unavailable node in sloppy quorum.

- **Clock skew.** We use `SystemTime` for timestamps. If two nodes have significantly different wall clocks, LWW will silently discard writes in a non-intuitive order. Is this acceptable for evaluation, or should we add a warning/note to the evaluation section?
