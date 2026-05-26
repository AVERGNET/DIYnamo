use crate::cluster::delegate::{DiynamoNodeDelegate, NodeMeta};
use crate::cluster::types::MemberInfo;
use anyhow::{Context, Result};
use memberlist::{
    agnostic::tokio::TokioRuntime,
    delegate::{CompositeDelegate, SubscribleEventDelegate, VoidDelegate, EventSubscriber},
    net::{stream_layer::tcp::Tcp, NetTransportOptions},
    proto::{MaybeResolvedAddress, NodeState},
    tokio::{TokioSocketAddrResolver, TokioTcpMemberlist},
    Options,
};
use smol_str::SmolStr;
use std::{net::SocketAddr, sync::{Arc, Mutex}};

type DiynamoDelegate = CompositeDelegate<
    SmolStr,
    SocketAddr,
    VoidDelegate<SmolStr, SocketAddr>,            // alive
    VoidDelegate<SmolStr, SocketAddr>,            // conflict
    SubscribleEventDelegate<SmolStr, SocketAddr>, // event
    VoidDelegate<SmolStr, SocketAddr>,            // merge
    DiynamoNodeDelegate,                          // node
    VoidDelegate<SmolStr, SocketAddr>,            // ping
>;

type ClusterMemberlist =
    TokioTcpMemberlist<SmolStr, TokioSocketAddrResolver, DiynamoDelegate>;

/// Handle to a running memberlist node.
pub struct GossipNode {
    inner: Arc<ClusterMemberlist>,
    node_id: String,
    gossip_bind: SocketAddr,
    event_sub: Mutex<Option<EventSubscriber<SmolStr, SocketAddr>>>,
}

impl GossipNode {
    pub async fn start(
        node_id: impl Into<String>,
        gossip_bind: SocketAddr,
        join_seeds: &[SocketAddr],
        node_meta: NodeMeta,
    ) -> Result<Arc<Self>> {
        let node_id = node_id.into();
        let id = SmolStr::new(&node_id);

        let (event_delegate, event_sub) = SubscribleEventDelegate::bounded(256);
        let delegate = CompositeDelegate::new()
            .with_event_delegate(event_delegate)
            .with_node_delegate(DiynamoNodeDelegate { local_meta: node_meta });

        let mut net_opts =
            NetTransportOptions::<SmolStr, TokioSocketAddrResolver, Tcp<TokioRuntime>>::new(id);
        net_opts.add_bind_address(gossip_bind);

        let memberlist = TokioTcpMemberlist::with_delegate(delegate, net_opts, Options::lan())
            .await
            .map_err(|e| anyhow::anyhow!("failed to start memberlist: {e:?}"))?;

        if !join_seeds.is_empty() {
            let targets = join_seeds
                .iter()
                .copied()
                .map(MaybeResolvedAddress::Resolved);
            match memberlist.join_many(targets).await {
                Ok(joined) => {
                    println!(
                        "joined cluster via {} seed(s): {:?}",
                        joined.len(),
                        joined
                    );
                }
                Err((joined, err)) => {
                    if joined.is_empty() {
                        anyhow::bail!("failed to join any seed: {err}");
                    }
                    eprintln!(
                        "warning: partial seed join ({}/{}): {err}",
                        joined.len(),
                        join_seeds.len()
                    );
                }
            }
        } else {
            println!("starting as seed (no --join addresses)");
        }

        Ok(Arc::new(Self {
            inner: Arc::new(memberlist),
            node_id,
            gossip_bind,
            event_sub: Mutex::new(Some(event_sub)),
        }))
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn gossip_bind(&self) -> SocketAddr {
        self.gossip_bind
    }

    /// Take the event subscriber out of this node. Panics if called more than once.
    pub fn subscribe(&self) -> EventSubscriber<SmolStr, SocketAddr> {
        self.event_sub
            .lock()
            .unwrap()
            .take()
            .expect("GossipNode::subscribe() called more than once")
    }

    pub async fn online_members(&self) -> Vec<MemberInfo> {
        let members = self.inner.online_members().await;
        members
            .iter()
            .map(|s| node_state_to_member(s.as_ref()))
            .collect()
    }

    pub async fn all_members(&self) -> Vec<MemberInfo> {
        let members = self.inner.members().await;
        members
            .iter()
            .map(|s| node_state_to_member(s.as_ref()))
            .collect()
    }

    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.inner.leave(std::time::Duration::from_secs(3)).await;
        self.inner.shutdown().await.context("memberlist shutdown")?;
        Ok(())
    }
}

fn node_state_to_member(state: &NodeState<SmolStr, SocketAddr>) -> MemberInfo {
    let meta_bytes = state.meta().as_bytes();
    let (uuid, forward_port) = crate::cluster::delegate::NodeMeta::from_bytes(meta_bytes)
        .map(|m| (m.uuid, m.http_port))
        .unwrap_or(([0u8; 16], 0));
    MemberInfo {
        id: state.id.to_string(),
        gossip_addr: state.addr,
        forward_port,
        uuid,
    }
}

/// Pollable view of cluster membership (easy to mock in tests).
pub trait MembershipView: Send + Sync {
    fn online_members(
        &self,
    ) -> impl std::future::Future<Output = Vec<MemberInfo>> + Send;
}

impl MembershipView for GossipNode {
    async fn online_members(&self) -> Vec<MemberInfo> {
        GossipNode::online_members(self).await
    }
}

impl MembershipView for Arc<GossipNode> {
    async fn online_members(&self) -> Vec<MemberInfo> {
        GossipNode::online_members(self).await
    }
}
