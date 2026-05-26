use anyhow::Context;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};

use super::timestamp::TimestampSource;
use super::{StoreConfig, StorageEngine, VersionedValue};

/// On-disk representation of a stored value. Serialized with bincode before writing
/// to RocksDB and deserialized on read. Kept separate from VersionedValue so that
/// the serialization format is an internal detail of this module.
#[derive(Serialize, Deserialize)]
struct StoredEntry {
    timestamp: u64,
    data: Vec<u8>,
}

pub struct RocksDbStore {
    db: DB,
    clock: Box<dyn TimestampSource>,
}

impl RocksDbStore {
    pub fn open(config: StoreConfig, clock: Box<dyn TimestampSource>) -> anyhow::Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(config.create_if_missing);

        let db = DB::open(&opts, &config.path)
            .with_context(|| format!("failed to open RocksDB at {:?}", config.path))?;

        Ok(Self { db, clock })
    }
}

impl StorageEngine for RocksDbStore {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<VersionedValue>> {
        let raw = self
            .db
            .get(key)
            .with_context(|| "RocksDB get failed")?;

        match raw {
            None => Ok(None),
            Some(bytes) => {
                let entry: StoredEntry = bincode::deserialize(&bytes)
                    .with_context(|| "failed to deserialize stored entry")?;
                Ok(Some(VersionedValue {
                    timestamp: entry.timestamp,
                    data: entry.data,
                }))
            }
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let entry = StoredEntry {
            timestamp: self.clock.now_millis(),
            data: value.to_vec(),
        };
        let bytes = bincode::serialize(&entry)
            .with_context(|| "failed to serialize stored entry")?;
        self.db
            .put(key, bytes)
            .with_context(|| "RocksDB put failed")?;
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.db
            .delete(key)
            .with_context(|| "RocksDB delete failed")?;
        Ok(())
    }
}
