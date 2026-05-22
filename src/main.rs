mod store;

use std::sync::Arc;

use store::rocksdb_store::RocksDbStore;
use store::timestamp::SystemTimestamp;
use store::{StoreConfig, StorageEngine};

fn main() -> anyhow::Result<()> {
    let config = StoreConfig {
        path: "./data/db".into(),
        create_if_missing: true,
    };

    let store: Arc<dyn StorageEngine> = Arc::new(
        RocksDbStore::open(config, Box::new(SystemTimestamp))?,
    );

    // Hand `store` to the HTTP layer here. The HTTP layer receives
    // Arc<dyn StorageEngine> and never imports anything from store::rocksdb_store.
    let _ = store;

    Ok(())
}
