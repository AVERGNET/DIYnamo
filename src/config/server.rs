use anyhow::{Context, Result};
use serde::Deserialize;
use std::{net::SocketAddr, path::Path};

/// On-disk server configuration (TOML).
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfigFile {
    pub node: NodeSection,
    #[serde(default)]
    pub cluster: ClusterSection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeSection {
    pub id: String,
    pub http_port: u16,
    pub gossip_bind: SocketAddr,
    pub data_dir: String,
}

/// Static roster entry used to build the consistent hash ring.
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterMember {
    pub id: String,
    pub gossip_addr: SocketAddr,
    pub forward_port: u16,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ClusterSection {
    #[serde(default)]
    pub join: Vec<SocketAddr>,
    #[serde(default)]
    pub members: Vec<ClusterMember>,
    #[serde(default)]
    pub seeds: Vec<String>,
}

/// Fully resolved settings used to start the server.
#[derive(Debug, Clone)]
pub struct ResolvedServerConfig {
    pub node_id: String,
    pub port: u16,
    pub gossip_bind: SocketAddr,
    pub data_dir: String,
    pub join: Vec<SocketAddr>,
    pub cluster_members: Vec<ClusterMember>,
    pub seeds: Vec<String>,
}

impl From<ServerConfigFile> for ResolvedServerConfig {
    fn from(file: ServerConfigFile) -> Self {
        Self {
            node_id: file.node.id,
            port: file.node.http_port,
            gossip_bind: file.node.gossip_bind,
            data_dir: file.node.data_dir,
            join: file.cluster.join,
            cluster_members: file.cluster.members,
            seeds: file.cluster.seeds,
        }
    }
}

impl Default for ResolvedServerConfig {
    fn default() -> Self {
        Self {
            node_id: "node0".into(),
            port: 8080,
            gossip_bind: "127.0.0.1:7946".parse().expect("valid addr"),
            data_dir: "./data/db".into(),
            join: Vec::new(),
            cluster_members: Vec::new(),
            seeds: Vec::new(),
        }
    }
}

impl ResolvedServerConfig {
    /// Apply CLI overrides: only set fields that were explicitly passed.
    pub fn apply_overrides(
        mut self,
        port: Option<u16>,
        data_dir: Option<String>,
        node_id: Option<String>,
        gossip_bind: Option<SocketAddr>,
        join: Option<Vec<SocketAddr>>,
    ) -> Self {
        if let Some(port) = port {
            self.port = port;
        }
        if let Some(data_dir) = data_dir {
            self.data_dir = data_dir;
        }
        if let Some(node_id) = node_id {
            self.node_id = node_id;
        }
        if let Some(gossip_bind) = gossip_bind {
            self.gossip_bind = gossip_bind;
        }
        if let Some(join) = join {
            self.join = join;
        }
        self
    }
}

pub fn load_server_config(path: impl AsRef<Path>) -> Result<ServerConfigFile> {
    let path = path.as_ref();
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    toml::from_str(&contents).with_context(|| format!("failed to parse config {}", path.display()))
}

pub fn resolve(
    config_path: Option<&Path>,
    port: Option<u16>,
    data_dir: Option<String>,
    node_id: Option<String>,
    gossip_bind: Option<SocketAddr>,
    join: Option<Vec<SocketAddr>>,
) -> Result<ResolvedServerConfig> {
    let base = match config_path {
        Some(path) => ResolvedServerConfig::from(load_server_config(path)?),
        None => ResolvedServerConfig::default(),
    };
    Ok(base.apply_overrides(port, data_dir, node_id, gossip_bind, join))
}
