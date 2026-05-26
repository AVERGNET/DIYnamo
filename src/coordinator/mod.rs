use std::sync::Arc;

use anyhow::{anyhow, Result};
use crate::client::KvClient;
use crate::cluster::ring::CoordinatorRing;
use crate::cluster::GossipNode;
use crate::config::ClusterMember;
use crate::store::rocksdb_store::RocksDbStore;
use crate::store::{HintStore, StorageEngine, VersionedValue};

/// Owner is in the ring but gossip reports it as unreachable.
#[derive(Debug)]
pub struct OwnerUnavailable {
    pub owner_id: String,
}

impl std::fmt::Display for OwnerUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "owner {} is not available", self.owner_id)
    }
}

impl std::error::Error for OwnerUnavailable {}

/// Coordinates KV operations: hash-ring owner lookup, forward to peers via internal HTTP.
pub struct ReplicatedStore {
    pub local: Arc<RocksDbStore>,
    pub gossip: Arc<GossipNode>,
    pub hints: Arc<HintStore>,
    self_id: String,
    ring: CoordinatorRing,
    pub n: usize,
    pub w: usize,
    pub r: usize,
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
        Ok(Self {
            local,
            gossip,
            hints,
            self_id,
            ring,
            n,
            w,
            r,
        })
    }

    async fn is_online(&self, node_id: &str) -> bool {
        self.gossip
            .online_members()
            .await
            .iter()
            .any(|m| m.id == node_id)
    }
}

impl StorageEngine for ReplicatedStore {
    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        let plist = self.ring.preference_list_for_key(key, self.n)?;
        let owner = plist
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("preference list is empty"))?;

        if owner.id == self.self_id {
            return self.local.get(key).await;
        }

        if !self.is_online(&owner.id).await {
            return Err(anyhow!(OwnerUnavailable {
                owner_id: owner.id.clone(),
            }));
        }

        let client = KvClient::new(owner.internal_base_url())?;
        let versioned = client.get_internal_versioned(key).await?;
        Ok(Some(versioned))
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let plist = self.ring.preference_list_for_key(key, self.n)?;
        let owner = plist
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("preference list is empty"))?;

        if owner.id == self.self_id {
            return self.local.put(key, value).await;
        }

        if !self.is_online(&owner.id).await {
            return Err(anyhow!(OwnerUnavailable {
                owner_id: owner.id.clone(),
            }));
        }

        let client = KvClient::new(owner.internal_base_url())?;
        client.put_internal_bytes(key, value).await
    }
}
