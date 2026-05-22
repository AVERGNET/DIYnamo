use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, put},
    Json, Router,
};
use clap::Parser;
use diynamo::api::types::{GetResponse, PutBody};
use diynamo::store::rocksdb_store::RocksDbStore;
use diynamo::store::timestamp::SystemTimestamp;
use diynamo::store::{StoreConfig, StorageEngine};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    store: Arc<dyn StorageEngine>,
}

#[derive(Parser)]
#[command(name = "diynamo-server", about = "DIYnamo KV HTTP server")]
struct Args {
    #[arg(long, default_value = "8080")]
    port: u16,

    #[arg(long, default_value = "./data/db")]
    data_dir: String,
}

async fn put_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PutBody>,
) -> Result<StatusCode, StatusCode> {
    let store = state.store.clone();
    let key_bytes = key.into_bytes();
    let value_bytes = body.value.into_bytes();
    tokio::task::spawn_blocking(move || store.put(&key_bytes, &value_bytes))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn get_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, StatusCode> {
    let store = state.store.clone();
    let key_bytes = key.into_bytes();
    let versioned = tokio::task::spawn_blocking(move || store.get(&key_bytes))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match versioned {
        Some(v) => {
            let value = String::from_utf8(v.data).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Ok(Json(GetResponse { value }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let data_dir = args.data_dir.clone();

    let config = StoreConfig {
        path: args.data_dir.into(),
        create_if_missing: true,
    };
    let store: Arc<dyn StorageEngine> = Arc::new(
        RocksDbStore::open(config, Box::new(SystemTimestamp))?,
    );
    let state = AppState { store };

    let addr = format!("0.0.0.0:{}", args.port);
    let app = Router::new()
        .route("/kv/{key}", get(get_kv).put(put_kv))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("listening on http://{addr}");
    println!("RocksDB data dir: {data_dir}");
    axum::serve(listener, app).await?;

    Ok(())
}
