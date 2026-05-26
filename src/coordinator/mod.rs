pub mod handoff;

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use tokio::task::{JoinHandle, JoinSet};

use crate::client::KvClient;
use crate::cluster::ring::CoordinatorRing;
use crate::cluster::GossipNode;
use crate::config::ClusterMember;
use crate::store::rocksdb_store::RocksDbStore;
use crate::store::{HintStore, StorageEngine, VersionedValue};

/// Returned when a quorum of acks (real writes + hints) could not be reached.
#[derive(Debug)]
pub struct QuorumFailed {
    pub acks: usize,
    pub required: usize,
}

impl std::fmt::Display for QuorumFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "quorum failed: got {}/{} acks",
            self.acks, self.required
        )
    }
}

impl std::error::Error for QuorumFailed {}

/// Coordinates KV operations across the cluster.
///
/// `put` — sloppy-quorum writes: attempt all N preferred nodes in parallel,
/// supplement failures with hinted writes on extra ring nodes; succeed if
/// real_writes + hints >= W.
///
/// `get` — quorum reads with LWW and read repair: read all N preferred nodes
/// in parallel, require at least R successful responses, pick the
/// highest-timestamp winner, fire-and-forget repairs to stale replicas.
pub struct ReplicatedStore {
    pub local: Arc<RocksDbStore>,
    pub gossip: Arc<GossipNode>,
    pub hints: Arc<HintStore>,
    self_id: String,
    ring: CoordinatorRing,
    pub n: usize,
    pub w: usize,
    pub r: usize,
    _handoff: JoinHandle<()>,
}

impl ReplicatedStore {
    pub fn new(
        local: Arc<RocksDbStore>,
        gossip: Arc<GossipNode>,
        hints: Arc<HintStore>,
        self_id: String,
        roster: Vec<ClusterMember>,
        n: usize,
        w: usize,
        r: usize,
    ) -> anyhow::Result<Self> {
        let ring = CoordinatorRing::from_roster(&roster, n)?;

        let events = gossip.subscribe();
        let handoff_task = handoff::HandoffTask {
            hints: Arc::clone(&hints),
            gossip: Arc::clone(&gossip),
            local: Arc::clone(&local),
            self_id: self_id.clone(),
            roster: roster.clone(),
            n,
            events,
        };
        let _handoff = handoff_task.start();

        Ok(Self {
            local,
            gossip,
            hints,
            self_id,
            ring,
            n,
            w,
            r,
            _handoff,
        })
    }
}

impl StorageEngine for ReplicatedStore {
    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        let pref_list = self.ring.preference_list_for_key(key, self.n)?;

        // --- Phase 1: parallel reads from all N preferred nodes ---
        // Each task returns the node's base URL and its read result.
        // Ok(None)  = node responded, key absent (counts toward quorum, repair candidate)
        // Ok(Some)  = node responded, key present
        // Err       = timeout or network failure (does not count toward quorum)
        let mut js: JoinSet<(String, Result<Option<VersionedValue>>)> = JoinSet::new();

        for member in &pref_list {
            let url = member.internal_base_url();
            if member.id == self.self_id {
                let local = Arc::clone(&self.local);
                let k = key.to_vec();
                js.spawn(async move { (url, local.get(&k).await) });
            } else {
                let k = key.to_vec();
                js.spawn(async move {
                    let r = async {
                        let client = KvClient::new(&url)?;
                        client.get_internal_versioned(&k).await
                    }
                    .await;
                    (url, r)
                });
            }
        }

        // Collect successful responses; discard errors (timeout / unreachable).
        let mut responses: Vec<(String, Option<VersionedValue>)> = Vec::new();
        while let Some(join_res) = js.join_next().await {
            if let Ok((url, Ok(opt))) = join_res {
                responses.push((url, opt));
            }
        }

        // --- Phase 2: quorum check ---
        if responses.len() < self.r {
            return Err(anyhow::anyhow!(QuorumFailed {
                acks: responses.len(),
                required: self.r,
            }));
        }

        // --- Phase 3: LWW — pick the Some response with the highest timestamp ---
        let winner: Option<VersionedValue> = responses
            .iter()
            .filter_map(|(_, opt)| opt.as_ref())
            .max_by_key(|v| v.timestamp)
            .cloned();

        // --- Phase 4: read repair (fire-and-forget) ---
        // Push the winner to any replica that responded with missing or stale data.
        // Uses put_internal_versioned_bytes which maps to put_if_newer on the
        // receiving node: the write only lands if no fresher data has arrived
        // since the read was issued. This prevents a delayed repair from
        // overwriting a concurrent external write with a newer timestamp.
        // Only nodes that actually responded are repaired; unreachable nodes are
        // left for hinted handoff / reconciliation.
        if let Some(ref winner_val) = winner {
            let winner_ts = winner_val.timestamp;
            let repair_data = winner_val.data.clone();
            let k = key.to_vec();

            for (url, opt) in &responses {
                let needs_repair = match opt {
                    None => true,
                    Some(v) => v.timestamp < winner_ts,
                };
                if needs_repair {
                    let url = url.clone();
                    let k = k.clone();
                    let data = repair_data.clone();
                    tokio::spawn(async move {
                        if let Ok(client) = KvClient::new(&url) {
                            let _ = client
                                .put_internal_versioned_bytes(&k, &data, winner_ts)
                                .await;
                        }
                    });
                }
            }
        }

        Ok(winner)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let all_in_order = self.ring.ring_order_for_key(key)?;
        let pref_list = &all_in_order[..self.n];
        let hint_candidates = &all_in_order[self.n..];

        // Build a set of preferred node IDs for O(1) exclusion when scanning candidates.
        let pref_ids: HashSet<&str> = pref_list.iter().map(|m| m.id.as_str()).collect();

        // --- Phase 1: parallel writes to all N preferred nodes ---
        // Each task returns (node_id, Result<()>). A timed-out or errored write
        // is treated as a failure; the 1s timeout is baked into KvClient.
        let mut js: JoinSet<(String, Result<()>)> = JoinSet::new();

        for member in pref_list {
            let node_id = member.id.clone();
            if member.id == self.self_id {
                let local = Arc::clone(&self.local);
                let k = key.to_vec();
                let v = value.to_vec();
                js.spawn(async move {
                    let r = local.put(&k, &v).await;
                    (node_id, r)
                });
            } else {
                let url = member.internal_base_url();
                let k = key.to_vec();
                let v = value.to_vec();
                js.spawn(async move {
                    let r = async {
                        let client = KvClient::new(&url)?;
                        client.put_internal_bytes(&k, &v).await
                    }
                    .await;
                    (node_id, r)
                });
            }
        }

        let mut real_acks: usize = 0;
        let mut failed_node_ids: Vec<String> = Vec::new();

        while let Some(join_res) = js.join_next().await {
            match join_res {
                Ok((_node_id, Ok(()))) => real_acks += 1,
                Ok((node_id, Err(_))) => failed_node_ids.push(node_id),
                Err(_) => {} // task panicked — count as failure, node_id unknown
            }
        }

        // --- Phase 2: hinted handoff for failed preferred writes ---
        // Walk hint candidates in ring order. For each failed preferred node,
        // try candidates sequentially until one accepts the hint write.
        // A single candidate_idx cursor advances across all failure slots so
        // each candidate is tried at most once across the whole Phase 2.
        let mut candidate_idx = 0;
        let mut hint_acks: usize = 0;

        'slots: for target_id in &failed_node_ids {
            while candidate_idx < hint_candidates.len() {
                let candidate = hint_candidates[candidate_idx];
                candidate_idx += 1;

                // Candidates beyond the preference list should never be in pref_ids,
                // but guard defensively against ring implementations that might repeat nodes.
                if pref_ids.contains(candidate.id.as_str()) {
                    continue;
                }

                let url = candidate.internal_base_url();
                let result = async {
                    let client = KvClient::new(&url)?;
                    client.put_hint_bytes(target_id, key, value).await
                }
                .await;

                if result.is_ok() {
                    hint_acks += 1;
                    continue 'slots;
                }
                // candidate failed — try the next one for this same slot
            }
            // no candidates left; this slot goes unacknowledged
        }

        let acks = real_acks + hint_acks;
        if acks >= self.w {
            Ok(())
        } else {
            Err(anyhow::anyhow!(QuorumFailed {
                acks,
                required: self.w,
            }))
        }
    }
}
