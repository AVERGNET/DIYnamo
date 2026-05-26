use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use clap::Parser;
use diynamo::api::types::{GetResponse, PutBody};
use diynamo::cluster::GossipNode;
use diynamo::coordinator::ReplicatedStore;
use diynamo::store::rocksdb_store::RocksDbStore;
use diynamo::store::timestamp::SystemTimestamp;
use diynamo::store::{StoreConfig, StorageEngine};
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    store: Arc<ReplicatedStore>,
}

#[derive(Parser)]
#[command(name = "diynamo-server", about = "DIYnamo KV HTTP server")]
struct Args {
    #[arg(long, default_value = "8080")]
    port: u16,

    #[arg(long, default_value = "./data/db")]
    data_dir: String,

    /// Unique node name in the cluster
    #[arg(long, default_value = "node0")]
    node_id: String,

    /// Gossip bind address (memberlist TCP/UDP), e.g. 127.0.0.1:7946
    #[arg(long, default_value = "127.0.0.1:7946")]
    gossip_bind: SocketAddr,

    /// Seed node(s) to join (repeat flag or comma-separated). Omit on the first node.
    #[arg(long = "join", value_delimiter = ',')]
    join: Vec<SocketAddr>,
}

async fn put_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PutBody>,
) -> Result<StatusCode, StatusCode> {
    state
        .store
        .put(key.as_bytes(), body.value.as_bytes())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn get_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, StatusCode> {
    let versioned = state
        .store
        .get(key.as_bytes())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match versioned {
        Some(v) => {
            let value =
                String::from_utf8(v.data).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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
    let local = Arc::new(RocksDbStore::open(config, Box::new(SystemTimestamp))?);

    let gossip = GossipNode::start(&args.node_id, args.gossip_bind, &args.join).await?;

    let store = Arc::new(ReplicatedStore::new(local, gossip));
    let state = AppState { store };

    let addr = format!("0.0.0.0:{}", args.port);
    let app = Router::new()
        .route("/kv/{key}", get(get_kv).put(put_kv))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("listening on http://{addr}");
    println!("RocksDB data dir: {data_dir}");
    println!(
        "gossip node_id={} bind={}",
        args.node_id, args.gossip_bind
    );

    tokio::select! {
        result = axum::serve(listener, app) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\nshutting down...");
        }
    }

    state.store.gossip.shutdown().await?;
    Ok(())
}
