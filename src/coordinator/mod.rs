use std::sync::Arc;

use crate::cluster::GossipNode;
use crate::store::rocksdb_store::RocksDbStore;
use crate::store::{StorageEngine, VersionedValue};

/// The central coordinator struct. Owns the local RocksDB store and the gossip
/// node, and exposes them behind a single `StorageEngine` implementation.
///
/// Currently delegates all storage operations directly to the local store.
/// Future work extends this type with quorum reads/writes, hinted handoff,
/// and key migration — without changing the interface seen by `server.rs`.
pub struct ReplicatedStore {
    pub local: Arc<RocksDbStore>,
    pub gossip: Arc<GossipNode>,
}

impl ReplicatedStore {
    pub fn new(local: Arc<RocksDbStore>, gossip: Arc<GossipNode>) -> Self {
        Self { local, gossip }
    }
}

impl StorageEngine for ReplicatedStore {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<VersionedValue>> {
        self.local.get(key).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.local.put(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.local.delete(key).await
    }
}
