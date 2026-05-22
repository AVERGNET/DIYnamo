use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, put},
    Json, Router,
};
use clap::Parser;
use diynamo::api::types::{GetResponse, PutBody};
use diynamo::storage::RocksStorage;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    store: Arc<RocksStorage>,
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
    tokio::task::spawn_blocking(move || store.put(&key, &body.value))
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
    let value = tokio::task::spawn_blocking(move || store.get(&key))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match value {
        Some(value) => Ok(Json(GetResponse { value })),
        None => Err(StatusCode::NOT_FOUND),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let store = Arc::new(RocksStorage::open(&args.data_dir)?);
    let state = AppState { store };

    let addr = format!("0.0.0.0:{}", args.port);
    let app = Router::new()
        .route("/kv/{key}", put(put_kv).get(get_kv))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("listening on http://{addr}");
    println!("RocksDB data dir: {}", args.data_dir);
    axum::serve(listener, app).await?;

    Ok(())
}
