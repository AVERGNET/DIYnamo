pub mod rocksdb_store;
pub mod timestamp;

use std::path::PathBuf;

/// A value retrieved from the store, paired with the physical timestamp of the write
/// that produced it. The timestamp is used for last-write-wins conflict resolution
/// when comparing replicas.
#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub timestamp: u64,
    pub data: Vec<u8>,
}

/// Configuration for opening a storage engine instance.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub path: PathBuf,
    pub create_if_missing: bool,
}

/// The core storage abstraction. All access to the underlying store goes through
/// this trait, allowing the HTTP layer to depend on it without knowing about RocksDB,
/// and allowing a ReplicatedStore to wrap it in a future iteration.
pub trait StorageEngine: Send + Sync {
    fn get(&self, key: &[u8]) -> anyhow::Result<Option<VersionedValue>>;
    fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    fn delete(&self, key: &[u8]) -> anyhow::Result<()>;
}
