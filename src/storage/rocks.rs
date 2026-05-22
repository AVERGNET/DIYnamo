use anyhow::{Context, Result};
use rocksdb::DB;
use std::path::Path;

/// String key-value store backed by RocksDB.
pub struct RocksStorage {
    db: DB,
}

impl RocksStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("failed to create data directory")?;
        }
        let db = DB::open_default(path).context("failed to open RocksDB")?;
        Ok(Self { db })
    }

    pub fn put(&self, key: &str, value: &str) -> Result<()> {
        self.db
            .put(key.as_bytes(), value.as_bytes())
            .context("rocksdb put failed")
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        match self.db.get(key.as_bytes()).context("rocksdb get failed")? {
            Some(bytes) => {
                let value = String::from_utf8(bytes).context("stored value is not valid UTF-8")?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }
}
