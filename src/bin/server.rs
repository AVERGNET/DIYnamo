use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use clap::Parser;
use diynamo::api::types::{GetResponse, PutBody};
use diynamo::cluster::{run_live_set_printer, GossipNode};
use diynamo::config::resolve;
use diynamo::coordinator::{OwnerUnavailable, ReplicatedStore};
use diynamo::store::rocksdb_store::RocksDbStore;
use diynamo::store::timestamp::SystemTimestamp;
use diynamo::store::{StoreConfig, StorageEngine};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
struct AppState {
    store: Arc<ReplicatedStore>,
    local: Arc<RocksDbStore>,
}

/// CLI arguments. Values from the config file are overridden when the same flag is set.
#[derive(Parser)]
#[command(name = "diynamo-server", about = "DIYnamo KV HTTP server")]
struct CliArgs {
    /// Path to TOML config (e.g. config/node1.toml)
    #[arg(long, short = 'c')]
    config: Option<PathBuf>,

    #[arg(long)]
    port: Option<u16>,

    #[arg(long)]
    data_dir: Option<String>,

    #[arg(long)]
    node_id: Option<String>,

    #[arg(long)]
    gossip_bind: Option<SocketAddr>,

    /// Seed gossip address(es). Overrides config join list when set.
    #[arg(long = "join", value_delimiter = ',')]
    join: Option<Vec<SocketAddr>>,

    /// Delete the RocksDB data directory before starting (fresh empty store).
    #[arg(long, default_value_t = false)]
    wipe_data: bool,
}

fn maybe_wipe_data_dir(data_dir: &str, wipe: bool) -> Result<()> {
    if !wipe {
        return Ok(());
    }
    let path = PathBuf::from(data_dir);
    if path.exists() {
        std::fs::remove_dir_all(&path)
            .map_err(|e| anyhow::anyhow!("failed to wipe data dir {}: {e}", path.display()))?;
        println!("wiped RocksDB data dir: {}", path.display());
    }
    Ok(())
}

fn map_store_error(err: anyhow::Error) -> StatusCode {
    if err.downcast_ref::<OwnerUnavailable>().is_some() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
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
        .map_err(map_store_error)?;
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
        .map_err(map_store_error)?;

    match versioned {
        Some(v) => {
            let value =
                String::from_utf8(v.data).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Ok(Json(GetResponse { value }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn put_kv_internal(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PutBody>,
) -> Result<StatusCode, StatusCode> {
    state
        .local
        .put(key.as_bytes(), body.value.as_bytes())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn get_kv_internal(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, StatusCode> {
    let versioned = state
        .local
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
    let cli = CliArgs::parse();
    let cfg = resolve(
        cli.config.as_deref(),
        cli.port,
        cli.data_dir,
        cli.node_id,
        cli.gossip_bind,
        cli.join,
    )?;

    if cfg.cluster_members.is_empty() {
        anyhow::bail!("config must define [[cluster.members]] for hash ring routing");
    }

    let data_dir = cfg.data_dir.clone();
    maybe_wipe_data_dir(&data_dir, cli.wipe_data)?;

    let store_config = StoreConfig {
        path: cfg.data_dir.clone().into(),
        create_if_missing: true,
    };
    let local = Arc::new(RocksDbStore::open(store_config, Box::new(SystemTimestamp))?);

    let gossip = GossipNode::start(&cfg.node_id, cfg.gossip_bind, &cfg.join).await?;
    tokio::spawn(run_live_set_printer(gossip.clone(), Duration::from_secs(1)));

    let roster_size = cfg.cluster_members.len();
    let store = Arc::new(ReplicatedStore::new(
        local.clone(),
        gossip,
        cfg.node_id.clone(),
        cfg.cluster_members,
    )?);
    let state = AppState { store, local };

    let addr = format!("0.0.0.0:{}", cfg.port);
    let app = Router::new()
        .route("/kv/{key}", get(get_kv).put(put_kv))
        .route(
            "/internal/kv/{key}",
            get(get_kv_internal).put(put_kv_internal),
        )
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("listening on http://{addr}");
    println!("RocksDB data dir: {data_dir}");
    println!(
        "gossip node_id={} bind={} join={:?} roster_size={}",
        cfg.node_id,
        cfg.gossip_bind,
        cfg.join,
        roster_size
    );
    if !cfg.seeds.is_empty() {
        println!("CLI seeds (use one): {:?}", cfg.seeds);
    }

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
