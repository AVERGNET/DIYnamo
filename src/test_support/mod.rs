//! In-process multi-node cluster harness for integration tests.

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
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
    gossip: Arc<RwLock<Arc<GossipNode>>>,
}

/// Multi-node cluster for integration tests.
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
    pub roster: Vec<ClusterMember>,
    pub n: usize,
    pub w: usize,
    pub r: usize,
}

async fn reserve_gossip_addr(id: &str) -> Result<SocketAddr> {
    const EADDRINUSE: i32 = 98;
    for attempt in 0..8 {
        match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => {
                let addr = listener.local_addr()?;
                drop(listener);
                if attempt > 0 {
                    tokio::time::sleep(Duration::from_millis(50 * attempt as u64)).await;
                }
                return Ok(addr);
            }
            Err(e) if e.raw_os_error() == Some(EADDRINUSE) => {
                tokio::time::sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
            }
            Err(e) => {
                return Err(e).with_context(|| format!("bind gossip for {id}"));
            }
        }
    }
    anyhow::bail!("could not reserve gossip port for {id} after retries");
}

async fn start_gossip_with_retry(
    id: &str,
    gossip_addr: SocketAddr,
    join: &[SocketAddr],
    node_meta: NodeMeta,
) -> Result<Arc<GossipNode>> {
    let mut last_err = None;
    for attempt in 0..5 {
        match GossipNode::start(id, gossip_addr, join, node_meta).await {
            Ok(node) => return Ok(node),
            Err(e) => {
                let msg = format!("{e:#}");
                last_err = Some(e);
                if msg.contains("Address already in use") || msg.contains("os error 98") {
                    tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64)).await;
                    continue;
                }
                return Err(last_err.unwrap());
            }
        }
    }
    Err(last_err.unwrap())
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
            let gossip_addr = reserve_gossip_addr(id).await?;
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
            let gossip = Arc::new(RwLock::new(
                start_gossip_with_retry(&id, gossip_addr, &join, node_meta).await?,
            ));

            let store = Arc::new(ReplicatedStore::new(
                local.clone(),
                gossip.read().unwrap().clone(),
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

    /// Stop HTTP servers and gossip on every node (call at end of tests or on drop).
    pub async fn shutdown_all(&self) {
        for node in &self.nodes {
            node._server.abort();
            let _ = node.gossip_handle().shutdown().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            node._server.abort();
        }
    }
}

async fn wait_for_cluster(nodes: &[TestNode], expected: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut ok = true;
        for node in nodes {
            let online = node.gossip.read().unwrap().online_members().await.len();
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

/// Poll `condition` until it returns true or the timeout elapses.
pub async fn poll_until<F, Fut>(timeout: Duration, interval: Duration, mut condition: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if condition().await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("poll_until timed out after {:?}", timeout);
        }
        tokio::time::sleep(interval).await;
    }
}

impl TestNode {
    fn gossip_handle(&self) -> Arc<GossipNode> {
        self.gossip.read().unwrap().clone()
    }

    /// Block public and internal HTTP (replica RPCs), leave hint endpoint up.
    pub fn kill_http(&self) {
        self.faults.kill_http();
    }

    /// Unblock public and internal HTTP.
    pub fn recover_http(&self) {
        self.faults.recover_http();
    }

    /// Full outage: block HTTP and shut down gossip (peers stop seeing this node).
    pub async fn kill_node(&self) -> Result<()> {
        self.kill_http();
        self.suspend_gossip().await
    }

    /// Stop gossip (peers should eventually drop this node from the live set).
    pub async fn suspend_gossip(&self) -> Result<()> {
        self.gossip_handle().shutdown().await
    }

    /// Bring HTTP and gossip back after `kill_node`.
    pub async fn recover_node(&self, seed: SocketAddr) -> Result<()> {
        self.restart_gossip(seed).await?;
        self.recover_http();
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    }

    /// Start a fresh gossip instance on the same bind address and rejoin the cluster.
    pub async fn restart_gossip(&self, seed: SocketAddr) -> Result<()> {
        let join = if self.gossip_addr == seed {
            vec![]
        } else {
            vec![seed]
        };
        let meta = NodeMeta {
            uuid: uuid::Uuid::new_v4().into_bytes(),
            http_port: self.http_addr.port(),
        };
        let node = start_gossip_with_retry(&self.id, self.gossip_addr, &join, meta).await?;
        *self.gossip.write().unwrap() = node;
        Ok(())
    }

    /// Whether this node sees `target_id` in its gossip live set.
    pub async fn peer_sees_node(&self, target_id: &str) -> bool {
        self.gossip_handle()
            .online_members()
            .await
            .iter()
            .any(|m| m.id == target_id)
    }

    /// Read `target_id`'s startup UUID as seen by this node's gossip live set.
    pub async fn peer_uuid(&self, target_id: &str) -> Option<[u8; 16]> {
        self.gossip_handle()
            .online_members()
            .await
            .into_iter()
            .find(|m| m.id == target_id)
            .map(|m| m.uuid)
    }

    /// Simulate total data loss: wipe the local RocksDB store.
    pub fn wipe_local_data(&self) -> Result<()> {
        self.local.clear_all()
    }

    /// Recover after data loss: wait until `observer` drops this node, wipe local
    /// storage, restart gossip with a new UUID, and restore HTTP.
    pub async fn recover_after_data_loss(
        &self,
        seed: SocketAddr,
        observer: &TestNode,
        wait_gone: Duration,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + wait_gone;
        while observer.peer_sees_node(&self.id).await {
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "peer {} still sees {} after {:?}",
                    observer.id,
                    self.id,
                    wait_gone
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        self.wipe_local_data()?;
        self.restart_gossip(seed).await?;
        self.recover_http();
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    }

    /// Take gossip down, wait until `observer` no longer sees this node, then
    /// restart gossip and unblock HTTP (triggers peer `Join` for handoff).
    pub async fn recover_after_outage(
        &self,
        seed: SocketAddr,
        observer: &TestNode,
        wait_gone: Duration,
    ) -> Result<()> {
        self.suspend_gossip().await?;
        let deadline = tokio::time::Instant::now() + wait_gone;
        while observer.peer_sees_node(&self.id).await {
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "peer {} still sees {} after {:?}",
                    observer.id,
                    self.id,
                    wait_gone
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        self.restart_gossip(seed).await?;
        self.recover_http();
        // Let peers process Join and run HandoffTask.
        tokio::time::sleep(Duration::from_millis(300)).await;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.gossip_handle().shutdown().await
    }
}
