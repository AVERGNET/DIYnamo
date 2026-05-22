use anyhow::{bail, Context, Result};
use reqwest::StatusCode;

use crate::api::types::{GetResponse, PutBody};

/// HTTP client for a single node's `/kv/{key}` API.
#[derive(Clone)]
pub struct KvClient {
    base_url: String,
    http: reqwest::Client,
}

impl KvClient {
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            base_url: base_url.as_ref().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        })
    }

    fn kv_url(&self, key: &str) -> String {
        format!("{}/kv/{key}", self.base_url)
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
}
