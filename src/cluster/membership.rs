use crate::cluster::types::MemberInfo;
use anyhow::{Context, Result};
use memberlist::{
    agnostic::tokio::TokioRuntime,
    delegate::VoidDelegate,
    net::{stream_layer::tcp::Tcp, NetTransportOptions},
    proto::{MaybeResolvedAddress, NodeState},
    tokio::{TokioSocketAddrResolver, TokioTcpMemberlist},
    Options,
};
use smol_str::SmolStr;
use std::{net::SocketAddr, sync::Arc};

type ClusterMemberlist = TokioTcpMemberlist<SmolStr, TokioSocketAddrResolver, VoidDelegate<SmolStr, SocketAddr>>;
/// Handle to a running memberlist node.
pub struct GossipNode {
    inner: Arc<ClusterMemberlist>,
    node_id: String,
    gossip_bind: SocketAddr,
}

impl GossipNode {
    pub async fn start(
        node_id: impl Into<String>,
        gossip_bind: SocketAddr,
        join_seeds: &[SocketAddr],
    ) -> Result<Arc<Self>> {
        let node_id = node_id.into();
        let id = SmolStr::new(&node_id);

        let mut net_opts =
            NetTransportOptions::<SmolStr, TokioSocketAddrResolver, Tcp<TokioRuntime>>::new(id);
        net_opts.add_bind_address(gossip_bind);

        let memberlist = TokioTcpMemberlist::new(net_opts, Options::lan())
            .await
            .context("failed to start memberlist")?;

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
        }))
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn gossip_bind(&self) -> SocketAddr {
        self.gossip_bind
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
    MemberInfo {
        id: state.id.to_string(),
        gossip_addr: state.addr,
        forward_port: 0,
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
