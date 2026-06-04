//! In-process multi-node cluster harness for integration tests.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::cluster::delegate::NodeMeta;
use crate::cluster::GossipNode;
use crate::config::ClusterMember;
use crate::coordinator::ReplicatedStore;
use crate::server::{router, AppState, FaultFlags};
use crate::store::rocksdb_store::RocksDbStore;
use crate::store::timestamp::SystemTimestamp;
use crate::store::{HintStore, StoreConfig};

/// A single node in a test cluster.
pub struct TestNode {
    pub id: String,
    pub http_addr: SocketAddr,
    pub http_url: String,
    pub gossip_addr: SocketAddr,
    pub store: Arc<ReplicatedStore>,
    pub hints: Arc<HintStore>,
    pub local: Arc<RocksDbStore>,
    pub faults: FaultFlags,
    _data_dir: TempDir,
    _server: JoinHandle<()>,
    gossip: Arc<GossipNode>,
}

/// Multi-node cluster for integration tests.
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
    pub roster: Vec<ClusterMember>,
    pub n: usize,
    pub w: usize,
    pub r: usize,
}

impl TestCluster {
    /// Spawn a cluster with the given node ids (e.g. `["n1","n2",...]`).
    pub async fn spawn(node_ids: &[&str], n: usize, w: usize, r: usize) -> Result<Self> {
        let mut bindings = Vec::new();
        for id in node_ids {
            let http_listener = TcpListener::bind("127.0.0.1:0")
                .await
                .with_context(|| format!("bind http for {id}"))?;
            let http_addr = http_listener.local_addr()?;
            let gossip_listener = TcpListener::bind("127.0.0.1:0")
                .await
                .with_context(|| format!("bind gossip for {id}"))?;
            let gossip_addr = gossip_listener.local_addr()?;
            drop(gossip_listener);
            bindings.push((id.to_string(), http_listener, http_addr, gossip_addr));
        }

        let roster: Vec<ClusterMember> = bindings
            .iter()
            .map(|(id, _, http_addr, gossip_addr)| ClusterMember {
                id: id.clone(),
                gossip_addr: *gossip_addr,
                forward_port: http_addr.port(),
            })
            .collect();

        let seed_gossip = roster[0].gossip_addr;
        let mut nodes = Vec::new();

        for (i, (id, http_listener, http_addr, gossip_addr)) in bindings.into_iter().enumerate() {
            let join = if i == 0 {
                vec![]
            } else {
                vec![seed_gossip]
            };

            let data_dir = TempDir::new().context("temp data dir")?;
            let store_config = StoreConfig {
                path: data_dir.path().to_path_buf(),
                create_if_missing: true,
            };
            let local = Arc::new(RocksDbStore::open(store_config, Box::new(SystemTimestamp))?);

            let hints_path = data_dir.path().join("hints");
            let hints = Arc::new(HintStore::open(&hints_path)?);

            let node_meta = NodeMeta {
                uuid: uuid::Uuid::new_v4().into_bytes(),
                http_port: http_addr.port(),
            };
            let gossip =
                GossipNode::start(&id, gossip_addr, &join, node_meta).await?;

            let store = Arc::new(ReplicatedStore::new(
                local.clone(),
                gossip.clone(),
                hints.clone(),
                id.clone(),
                roster.clone(),
                n,
                w,
                r,
            )?);

            let faults = FaultFlags::default();
            let state = AppState {
                store: store.clone(),
                local: local.clone(),
                faults: faults.clone(),
            };
            let app = router(state);

            let server = tokio::spawn(async move {
                let _ = axum::serve(http_listener, app).await;
            });

            let http_url = format!("http://{http_addr}");
            nodes.push(TestNode {
                id,
                http_addr,
                http_url,
                gossip_addr,
                store,
                hints,
                local,
                faults,
                _data_dir: data_dir,
                _server: server,
                gossip,
            });
        }

        wait_for_cluster(&nodes, node_ids.len()).await?;

        Ok(Self {
            nodes,
            roster,
            n,
            w,
            r,
        })
    }

    pub fn node(&self, id: &str) -> &TestNode {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .unwrap_or_else(|| panic!("unknown node id: {id}"))
    }
}

async fn wait_for_cluster(nodes: &[TestNode], expected: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut ok = true;
        for node in nodes {
            let online = node.gossip.online_members().await.len();
            if online < expected {
                ok = false;
                break;
            }
        }
        if ok {
            tokio::time::sleep(Duration::from_millis(200)).await;
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("cluster did not converge to {expected} members within 10s");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl TestNode {
    pub async fn shutdown(&self) -> Result<()> {
        self.gossip.shutdown().await
    }
}
