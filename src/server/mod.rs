//! HTTP server routes shared by the `server` binary and integration test harness.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use crate::api::types::{GetResponse, PutBody, PutVersionedBody};
use crate::coordinator::{QuorumFailed, ReplicatedStore};
use crate::store::rocksdb_store::RocksDbStore;
use crate::store::StorageEngine;

/// Per-node fault injection flags for integration tests.
#[derive(Clone, Default)]
pub struct FaultFlags {
    pub block_internal: Arc<AtomicBool>,
    pub block_public: Arc<AtomicBool>,
    pub block_hints: Arc<AtomicBool>,
}

impl FaultFlags {
    pub fn kill_rpc(&self) {
        self.block_internal.store(true, Ordering::Relaxed);
        self.block_public.store(true, Ordering::Relaxed);
        self.block_hints.store(true, Ordering::Relaxed);
    }

    pub fn block_internal(&self, on: bool) {
        self.block_internal.store(on, Ordering::Relaxed);
    }

    pub fn block_hints(&self, on: bool) {
        self.block_hints.store(on, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<ReplicatedStore>,
    pub local: Arc<RocksDbStore>,
    pub faults: FaultFlags,
}

pub fn map_store_error(err: anyhow::Error) -> StatusCode {
    if err.downcast_ref::<QuorumFailed>().is_some() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/kv/{key}", get(get_kv).put(put_kv))
        .route(
            "/internal/kv/{key}",
            get(get_kv_internal).put(put_kv_internal),
        )
        .route(
            "/internal/kv-versioned/{key}",
            axum::routing::put(put_kv_internal_versioned),
        )
        .route(
            "/internal/hint/{target_id}/{key}",
            axum::routing::put(put_hint_internal),
        )
        .with_state(state)
}

async fn put_kv(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PutBody>,
) -> Result<StatusCode, StatusCode> {
    if state.faults.block_public.load(Ordering::Relaxed) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
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
    if state.faults.block_public.load(Ordering::Relaxed) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let versioned = state
        .store
        .get(key.as_bytes())
        .await
        .map_err(map_store_error)?;

    match versioned {
        Some(v) => {
            let value =
                String::from_utf8(v.data).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Ok(Json(GetResponse {
                value,
                timestamp: v.timestamp,
            }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}

async fn put_kv_internal(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PutBody>,
) -> Result<StatusCode, StatusCode> {
    if state.faults.block_internal.load(Ordering::Relaxed) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    state
        .local
        .put(key.as_bytes(), body.value.as_bytes())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn put_kv_internal_versioned(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(body): Json<PutVersionedBody>,
) -> Result<StatusCode, StatusCode> {
    if state.faults.block_internal.load(Ordering::Relaxed) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    state
        .local
        .put_if_newer(key.as_bytes(), body.value.as_bytes(), body.timestamp)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn put_hint_internal(
    State(state): State<AppState>,
    Path((target_id, key)): Path<(String, String)>,
    Json(body): Json<PutVersionedBody>,
) -> Result<StatusCode, StatusCode> {
    if state.faults.block_hints.load(Ordering::Relaxed) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    state
        .store
        .hints
        .store_hint(&target_id, key.as_bytes(), body.value.as_bytes(), body.timestamp)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn get_kv_internal(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<GetResponse>, StatusCode> {
    if state.faults.block_internal.load(Ordering::Relaxed) {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let versioned = state
        .local
        .get(key.as_bytes())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match versioned {
        Some(v) => {
            let value =
                String::from_utf8(v.data).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Ok(Json(GetResponse {
                value,
                timestamp: v.timestamp,
            }))
        }
        None => Err(StatusCode::NOT_FOUND),
    }
}
