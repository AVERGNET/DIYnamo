use std::collections::HashMap;
use std::sync::Arc;

use memberlist::delegate::EventSubscriber;
use memberlist::proto::NodeState;
use smol_str::SmolStr;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

use crate::client::KvClient;
use crate::cluster::delegate::NodeMeta;
use crate::cluster::ring::CoordinatorRing;
use crate::cluster::GossipNode;
use crate::config::ClusterMember;
use crate::store::rocksdb_store::RocksDbStore;
use crate::store::HintStore;

/// Background task that listens for gossip membership events and drives
/// hinted handoff delivery.
///
/// On every `Join` or `Update` event:
///   1. Deliver all pending hints for that node.
///   2. If the node's UUID has changed (process restart / data loss), push
///      every locally-held key that belongs in that node's preference list.
pub struct HandoffTask {
    pub hints: Arc<HintStore>,
    pub gossip: Arc<GossipNode>,
    pub local: Arc<RocksDbStore>,
    pub self_id: String,
    pub roster: Vec<ClusterMember>,
    pub n: usize,
    pub events: EventSubscriber<SmolStr, SocketAddr>,
}

impl HandoffTask {
    /// Spawn the background event loop. The returned `JoinHandle` should be
    /// stored by the caller to keep the task alive for the process lifetime.
    pub fn start(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(self) {
        let Self { hints, local, self_id: _, roster, n, events, gossip: _ } = self;

        let ring = match CoordinatorRing::from_roster(&roster, n) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("handoff: failed to build ring, task exiting: {e}");
                return;
            }
        };

        // node_id → last-seen startup UUID
        let mut known_uuids: HashMap<String, [u8; 16]> = HashMap::new();

        loop {
            let event = match events.recv().await {
                Ok(e) => e,
                Err(_) => break, // channel closed on shutdown
            };

            use memberlist::delegate::EventKind;
            match event.kind() {
                EventKind::Leave => continue,
                EventKind::Join | EventKind::Update => {}
                _ => continue,
            }

            let state: &NodeState<SmolStr, SocketAddr> = event.node_state();
            let node_id = state.id.to_string();

            let Some(meta) = NodeMeta::from_bytes(state.meta().as_bytes()) else {
                // Node hasn't gossiped meta yet; skip until it does.
                continue;
            };

            let node_url = format!("http://{}:{}", state.addr.ip(), meta.http_port);

            // --- Phase 1: deliver all pending hints for this node ---
            deliver_hints(&hints, &node_id, &node_url).await;

            // --- Phase 2: UUID-change reconciliation ---
            // A different UUID means the node restarted and lost its data, or
            // this is a brand-new node. Push all keys we hold that belong in
            // its preference list so it can rebuild its local state.
            let current_uuid = meta.uuid;
            if known_uuids.get(&node_id) != Some(&current_uuid) {
                reconcile_keys(&local, &ring, &node_id, &node_url, n).await;
            }

            known_uuids.insert(node_id, current_uuid);
        }
    }
}

/// Attempt to deliver every pending hint destined for `node_id` via its HTTP
/// endpoint. Successful deliveries are deleted from the hint store immediately.
///
/// Each hint carries the coordinator timestamp from the original write so the
/// target node stores the value with the same timestamp as every other replica.
async fn deliver_hints(hints: &HintStore, node_id: &str, node_url: &str) {
    let pending = match hints.hints_for_node(node_id) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("handoff: failed to read hints for {node_id}: {e}");
            return;
        }
    };

    for (key, value, timestamp) in pending {
        let client = match KvClient::new(node_url) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("handoff: failed to build client for {node_url}: {e}");
                return;
            }
        };
        if client.put_internal_versioned_bytes(&key, &value, timestamp).await.is_ok() {
            let _ = hints.delete_hint(node_id, &key);
        }
    }
}

/// Iterate the local RocksDB and push every key whose preference list includes
/// `node_id`. Called when a node's UUID changes (it restarted with empty data).
async fn reconcile_keys(
    local: &RocksDbStore,
    ring: &CoordinatorRing,
    node_id: &str,
    node_url: &str,
    n: usize,
) {
    for item in local.iter_all() {
        let (key, vv) = match item {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("handoff: iter_all error during reconciliation: {e}");
                continue;
            }
        };

        let in_pref_list = ring
            .preference_list_for_key(&key, n)
            .map(|list| list.iter().any(|m| m.id == node_id))
            .unwrap_or(false);

        if in_pref_list {
            if let Ok(client) = KvClient::new(node_url) {
                let _ = client.put_internal_versioned_bytes(&key, &vv.data, vv.timestamp).await;
            }
        }
    }
}
