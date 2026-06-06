use std::collections::{HashMap, HashSet};
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
///   1. Spawn a task to deliver all pending hints for that node.
///   2. If the node's UUID has changed (process restart / data loss), abort
///      any in-progress reconciliation for that node and spawn a fresh one.
///
/// Reconciliation snapshots online membership once at task start, then for
/// each locally-held key pushes it to the returning node only if this node is
/// the designated sender — the first online preferred replica that is not the
/// returning node itself. This avoids every peer hammering the recovering node
/// simultaneously while remaining robust to primary failures: if the primary
/// is offline and gossip has converged, the secondary takes over automatically.
pub struct HandoffTask {
    pub hints: Arc<HintStore>,
    pub gossip: Arc<GossipNode>,
    pub local: Arc<RocksDbStore>,
    pub self_id: String,
    pub roster: Vec<ClusterMember>,
    pub n: usize,
    pub vnodes: usize,
    pub events: EventSubscriber<SmolStr, SocketAddr>,
}

impl HandoffTask {
    /// Spawn the background event loop. The returned `JoinHandle` should be
    /// stored by the caller to keep the task alive for the process lifetime.
    pub fn start(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    async fn run(self) {
        let Self { hints, local, self_id, roster, n, vnodes, events, gossip } = self;

        let ring = match CoordinatorRing::from_roster(&roster, n, vnodes) {
            Ok(r) => Arc::new(r),
            Err(e) => {
                eprintln!("handoff: failed to build ring, task exiting: {e}");
                return;
            }
        };

        // node_id → last-seen startup UUID
        let mut known_uuids: HashMap<String, [u8; 16]> = HashMap::new();
        // node_id → handle of any in-progress reconciliation task for that node
        let mut active_reconciliations: HashMap<String, JoinHandle<()>> = HashMap::new();

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

            // --- Phase 1: spawn hint delivery ---
            // Spawned so a large hint backlog does not block the event loop.
            {
                let hints = Arc::clone(&hints);
                let nid = node_id.clone();
                let url = node_url.clone();
                tokio::spawn(async move {
                    deliver_hints(hints, nid, url).await;
                });
            }

            // --- Phase 2: UUID-change reconciliation ---
            // A different UUID means the node restarted, or this is a new node.
            // Abort any stale reconciliation task for this node and start a
            // fresh one so double-restarts are handled correctly.
            let current_uuid = meta.uuid;
            if known_uuids.get(&node_id) != Some(&current_uuid) {
                if let Some(handle) = active_reconciliations.remove(&node_id) {
                    handle.abort();
                }
                let handle = tokio::spawn(reconcile_keys(
                    Arc::clone(&local),
                    Arc::clone(&ring),
                    Arc::clone(&gossip),
                    self_id.clone(),
                    node_id.clone(),
                    node_url,
                    n,
                ));
                active_reconciliations.insert(node_id.clone(), handle);
            }

            known_uuids.insert(node_id, current_uuid);

            // Prune completed handles so the map does not grow without bound.
            active_reconciliations.retain(|_, h| !h.is_finished());
        }
    }
}

/// Attempt to deliver every pending hint destined for `node_id` via its HTTP
/// endpoint. Successful deliveries are deleted from the hint store immediately.
///
/// Each hint carries the coordinator timestamp from the original write so the
/// target node stores the value with the same timestamp as every other replica.
async fn deliver_hints(hints: Arc<HintStore>, node_id: String, node_url: String) {
    let pending = match hints.hints_for_node(&node_id) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("handoff: failed to read hints for {node_id}: {e}");
            return;
        }
    };

    if pending.is_empty() {
        return;
    }

    let client = match KvClient::new(&node_url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("handoff: failed to build client for {node_url}: {e}");
            return;
        }
    };

    for (key, value, timestamp) in pending {
        if client
            .put_internal_versioned_bytes(&key, &value, timestamp)
            .await
            .is_ok()
        {
            let _ = hints.delete_hint(&node_id, &key);
        }
    }
}

/// Iterate the local RocksDB and push keys to the returning node where this
/// node is the designated sender.
///
/// Designated sender for key K returning to node C: the first node in K's
/// preference list (excluding C) that is online at the time this task starts.
/// The online set is snapshotted once at the top — per-key queries would be
/// too expensive and would produce inconsistent decisions within a single scan.
///
/// If the primary for K is offline but gossip has not yet converged, this node
/// may compute an incorrect designated sender and skip K. In that case the next
/// quorum read for K will fire-and-forget a read repair to bring C up to date.
async fn reconcile_keys(
    local: Arc<RocksDbStore>,
    ring: Arc<CoordinatorRing>,
    gossip: Arc<GossipNode>,
    self_id: String,
    node_id: String,
    node_url: String,
    n: usize,
) {
    let online_ids: HashSet<String> = gossip
        .online_members()
        .await
        .into_iter()
        .map(|m| m.id)
        .collect();

    let client = match KvClient::new(&node_url) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("handoff: failed to build client for {node_url}: {e}");
            return;
        }
    };

    for item in local.iter_all() {
        let (key, vv) = match item {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("handoff: iter_all error during reconciliation: {e}");
                continue;
            }
        };

        let pref_list = match ring.preference_list_for_key(&key, n) {
            Ok(list) => list,
            Err(_) => continue,
        };

        // Designated sender: first preferred node that is neither the returning
        // node nor absent from the online snapshot.
        let designated = pref_list
            .iter()
            .find(|m| m.id != node_id && online_ids.contains(&m.id))
            .map(|m| m.id.as_str());

        if designated != Some(self_id.as_str()) {
            continue;
        }

        let _ = client
            .put_internal_versioned_bytes(&key, &vv.data, vv.timestamp)
            .await;
    }
}
