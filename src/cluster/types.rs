use crate::config::ClusterMember;
use std::fmt;
use std::net::SocketAddr;

/// A member in the cluster roster (config + gossip views).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MemberInfo {
    pub id: String,
    pub gossip_addr: SocketAddr,
    pub forward_port: u16,
    /// Startup UUID gossiped via `NodeMeta`. Zero for roster entries built from
    /// static config (before gossip meta has been received).
    pub uuid: [u8; 16],
}

impl From<ClusterMember> for MemberInfo {
    fn from(m: ClusterMember) -> Self {
        Self {
            id: m.id,
            gossip_addr: m.gossip_addr,
            forward_port: m.forward_port,
            uuid: [0u8; 16],
        }
    }
}

impl MemberInfo {
    pub fn http_base_url(&self) -> String {
        format!("http://{}:{}", self.gossip_addr.ip(), self.forward_port)
    }

    pub fn internal_base_url(&self) -> String {
        self.http_base_url()
    }
}

impl fmt::Display for MemberInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}@{} (http:{})",
            self.id, self.gossip_addr, self.forward_port
        )
    }
}
