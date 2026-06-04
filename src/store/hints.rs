use std::path::Path;

use anyhow::{bail, Context, Result};
use rocksdb::{Direction, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};

/// On-disk representation of a hinted write. Serialized with bincode.
///
/// Carries the coordinator timestamp so that delivery via `put_internal_versioned`
/// preserves the same timestamp as the original write on every replica.
#[derive(Serialize, Deserialize)]
struct HintEntry {
    timestamp: u64,
    data: Vec<u8>,
}

/// Persistent store for hinted handoff entries.
///
/// Each hint records that a write (key, value, timestamp) was accepted locally
/// on behalf of a target node that was offline at write time. When the target
/// comes back online the handoff task reads these hints, delivers them via the
/// target's internal HTTP endpoint with the original coordinator timestamp, and
/// deletes them here on success.
///
/// Key layout:   `{target_node_id}/{original_key}` (raw bytes)
/// Value layout: bincode-encoded `HintEntry { timestamp, data }`
pub struct HintStore {
    db: DB,
}

/// Builds the compound RocksDB key for a hint.
fn hint_key(target_id: &str, key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(target_id.len() + 1 + key.len());
    k.extend_from_slice(target_id.as_bytes());
    k.push(b'/');
    k.extend_from_slice(key);
    k
}

impl HintStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)
            .with_context(|| format!("failed to open HintStore at {:?}", path))?;
        Ok(Self { db })
    }

    /// Persist a hint: `key` was written with `value` at coordinator `timestamp`
    /// but `target_id` was offline.
    pub fn store_hint(
        &self,
        target_id: &str,
        key: &[u8],
        value: &[u8],
        timestamp: u64,
    ) -> Result<()> {
        if target_id.contains('/') {
            bail!("target_id must not contain '/': got {:?}", target_id);
        }
        let entry = HintEntry {
            timestamp,
            data: value.to_vec(),
        };
        let bytes =
            bincode::serialize(&entry).context("failed to serialize hint entry")?;
        let compound = hint_key(target_id, key);
        self.db
            .put(&compound, bytes)
            .with_context(|| format!("failed to store hint for node '{target_id}'"))
    }

    /// Return all pending hints for `target_id` as `(original_key, value, timestamp)` triples.
    pub fn hints_for_node(&self, target_id: &str) -> Result<Vec<(Vec<u8>, Vec<u8>, u64)>> {
        let prefix = format!("{target_id}/");
        let prefix_bytes = prefix.as_bytes();

        let iter = self
            .db
            .iterator(IteratorMode::From(prefix_bytes, Direction::Forward));

        let mut results = Vec::new();
        for entry in iter {
            let (k, v) = entry.context("HintStore iterator error")?;
            if !k.starts_with(prefix_bytes) {
                break;
            }
            let original_key = k[prefix_bytes.len()..].to_vec();
            let hint: HintEntry =
                bincode::deserialize(&v).context("failed to deserialize hint entry")?;
            results.push((original_key, hint.data, hint.timestamp));
        }
        Ok(results)
    }

    /// Delete a hint after successful delivery.
    pub fn delete_hint(&self, target_id: &str, key: &[u8]) -> Result<()> {
        let compound = hint_key(target_id, key);
        self.db
            .delete(&compound)
            .with_context(|| format!("failed to delete hint for node '{target_id}'"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_tmp() -> (HintStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = HintStore::open(dir.path()).unwrap();
        (store, dir)
    }

    #[test]
    fn round_trip_single_hint() {
        let (store, _dir) = open_tmp();
        store.store_hint("n2", b"banana", b"yellow", 100).unwrap();
        let hints = store.hints_for_node("n2").unwrap();
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].0, b"banana");
        assert_eq!(hints[0].1, b"yellow");
        assert_eq!(hints[0].2, 100);
    }

    #[test]
    fn hints_for_node_only_returns_that_nodes_hints() {
        let (store, _dir) = open_tmp();
        store.store_hint("n1", b"apple", b"red", 1).unwrap();
        store.store_hint("n2", b"banana", b"yellow", 2).unwrap();
        store.store_hint("n2", b"cherry", b"red", 3).unwrap();

        let n1_hints = store.hints_for_node("n1").unwrap();
        assert_eq!(n1_hints.len(), 1);
        assert_eq!(n1_hints[0].0, b"apple");

        let n2_hints = store.hints_for_node("n2").unwrap();
        assert_eq!(n2_hints.len(), 2);
    }

    #[test]
    fn delete_hint_removes_entry() {
        let (store, _dir) = open_tmp();
        store.store_hint("n2", b"banana", b"yellow", 42).unwrap();
        store.delete_hint("n2", b"banana").unwrap();
        let hints = store.hints_for_node("n2").unwrap();
        assert!(hints.is_empty());
    }

    #[test]
    fn store_hint_rejects_slash_in_target_id() {
        let (store, _dir) = open_tmp();
        assert!(store.store_hint("n1/evil", b"key", b"val", 0).is_err());
    }

    #[test]
    fn hints_for_node_empty_when_none_stored() {
        let (store, _dir) = open_tmp();
        let hints = store.hints_for_node("n99").unwrap();
        assert!(hints.is_empty());
    }
}
