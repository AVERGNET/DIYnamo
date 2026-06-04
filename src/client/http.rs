use anyhow::{bail, Context, Result};
use reqwest::StatusCode;
use std::time::Duration;

use crate::api::types::{GetResponse, PutBody, PutVersionedBody};
use crate::store::VersionedValue;

/// Per-request timeout for all internal cluster calls.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

/// HTTP client for a single node's KV HTTP API.
#[derive(Clone)]
pub struct KvClient {
    base_url: String,
    http: reqwest::Client,
}

impl KvClient {
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self {
            base_url: base_url.as_ref().trim_end_matches('/').to_string(),
            http,
        })
    }

    fn kv_url(&self, key: &str) -> String {
        format!("{}/kv/{key}", self.base_url)
    }

    fn internal_kv_url(&self, key: &str) -> String {
        format!("{}/internal/kv/{key}", self.base_url)
    }

    fn internal_kv_versioned_url(&self, key: &str) -> String {
        format!("{}/internal/kv-versioned/{key}", self.base_url)
    }

    fn hint_url(&self, target_id: &str, key: &str) -> String {
        format!("{}/internal/hint/{target_id}/{key}", self.base_url)
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<()> {
        let response = self
            .http
            .put(self.kv_url(key))
            .json(&PutBody {
                value: value.to_string(),
            })
            .send()
            .await
            .context("put request failed")?;

        if response.status().is_success() {
            Ok(())
        } else {
            bail!(
                "put failed with status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
    }

    pub async fn get(&self, key: &str) -> Result<GetResponse> {
        let response = self
            .http
            .get(self.kv_url(key))
            .send()
            .await
            .context("get request failed")?;

        match response.status() {
            StatusCode::OK => response
                .json::<GetResponse>()
                .await
                .context("failed to decode get response"),
            StatusCode::NOT_FOUND => bail!("key not found: {key}"),
            status => bail!(
                "get failed with status {}: {}",
                status,
                response.text().await.unwrap_or_default()
            ),
        }
    }

    /// Local-only put via `/internal/kv/{key}` (no coordinator re-routing).
    pub async fn put_internal_bytes(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key = std::str::from_utf8(key).context("key must be UTF-8")?;
        let value = std::str::from_utf8(value).context("value must be UTF-8")?;
        let response = self
            .http
            .put(self.internal_kv_url(key))
            .json(&PutBody {
                value: value.to_string(),
            })
            .send()
            .await
            .context("internal put request failed")?;

        if response.status().is_success() {
            Ok(())
        } else {
            bail!(
                "internal put failed with status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
    }

    /// Local-only versioned put via `/internal/kv-versioned/{key}`.
    ///
    /// Sends `value` with an explicit `timestamp` so the receiving node writes
    /// it with that exact timestamp rather than generating a fresh one. Used by
    /// read repair to preserve LWW correctness across concurrent writes.
    pub async fn put_internal_versioned_bytes(
        &self,
        key: &[u8],
        value: &[u8],
        timestamp: u64,
    ) -> Result<()> {
        let key = std::str::from_utf8(key).context("key must be UTF-8")?;
        let value = std::str::from_utf8(value).context("value must be UTF-8")?;
        let response = self
            .http
            .put(self.internal_kv_versioned_url(key))
            .json(&PutVersionedBody {
                value: value.to_string(),
                timestamp,
            })
            .send()
            .await
            .context("versioned internal put request failed")?;

        if response.status().is_success() {
            Ok(())
        } else {
            bail!(
                "versioned internal put failed with status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
    }

    /// Store a hinted write on this node on behalf of `target_id`.
    ///
    /// The receiving node keeps the hint (including `timestamp`) in its local
    /// HintStore and delivers it to `target_id` with the same coordinator
    /// timestamp once that node is back online.
    pub async fn put_hint_versioned_bytes(
        &self,
        target_id: &str,
        key: &[u8],
        value: &[u8],
        timestamp: u64,
    ) -> Result<()> {
        let key = std::str::from_utf8(key).context("key must be UTF-8")?;
        let value = std::str::from_utf8(value).context("value must be UTF-8")?;
        let response = self
            .http
            .put(self.hint_url(target_id, key))
            .json(&PutVersionedBody {
                value: value.to_string(),
                timestamp,
            })
            .send()
            .await
            .context("hint put request failed")?;

        if response.status().is_success() {
            Ok(())
        } else {
            bail!(
                "hint put failed with status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            );
        }
    }

    /// Local-only get via `/internal/kv/{key}`.
    ///
    /// Returns `Ok(Some(v))` on success, `Ok(None)` when the key does not exist
    /// on that replica, and `Err` only on network/timeout failures. Callers can
    /// distinguish "missing key" from "unreachable node", which is required for
    /// correct LWW selection and read repair.
    pub async fn get_internal_versioned(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        let key = std::str::from_utf8(key).context("key must be UTF-8")?;
        let response = self
            .http
            .get(self.internal_kv_url(key))
            .send()
            .await
            .context("internal get request failed")?;

        match response.status() {
            StatusCode::OK => {
                let body: GetResponse = response
                    .json()
                    .await
                    .context("failed to decode internal get response")?;
                Ok(Some(VersionedValue {
                    timestamp: body.timestamp,
                    data: body.value.into_bytes(),
                }))
            }
            StatusCode::NOT_FOUND => Ok(None),
            status => bail!(
                "internal get failed with status {}: {}",
                status,
                response.text().await.unwrap_or_default()
            ),
        }
    }

}
