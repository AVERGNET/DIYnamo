use crate::cluster::types::MemberInfo;
use crate::config::ClusterMember;
use anyhow::{bail, Context, Result};
use hashring_coordinator::HashRing;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Ring identity — hash on node id only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RingNode {
    pub id: String,
}

impl Hash for RingNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

const RING_REPLICAS: usize = 0;
const RING_VNODES: usize = 1;

/// Consistent hash ring built from the static config roster.
pub struct CoordinatorRing {
    members_by_id: HashMap<String, MemberInfo>,
    ring: HashRing<RingNode>,
}

impl CoordinatorRing {
    pub fn from_roster(roster: &[ClusterMember]) -> Result<Self> {
        if roster.is_empty() {
            bail!("cluster roster is empty");
        }

        let members_by_id: HashMap<String, MemberInfo> = roster
            .iter()
            .cloned()
            .map(MemberInfo::from)
            .map(|m| (m.id.clone(), m))
            .collect();

        let mut ring = HashRing::new(RING_REPLICAS, RING_VNODES);
        let nodes: Vec<RingNode> = members_by_id
            .keys()
            .map(|id| RingNode { id: id.clone() })
            .collect();
        ring.batch_add(nodes);

        Ok(Self {
            members_by_id,
            ring,
        })
    }

    pub fn owner_for_key(&self, key: &[u8]) -> Result<&MemberInfo> {
        let key_str = std::str::from_utf8(key).context("key must be valid UTF-8")?;
        let owners = self.ring.get(&key_str);
        match owners.len() {
            0 => bail!("hash ring returned no owner for key"),
            1 => self
                .members_by_id
                .get(&owners[0].id)
                .context("owner missing from roster"),
            n => bail!("expected exactly one owner, got {n}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn member(id: &str, gossip_port: u16, forward_port: u16) -> ClusterMember {
        ClusterMember {
            id: id.into(),
            gossip_addr: format!("127.0.0.1:{gossip_port}").parse().unwrap(),
            forward_port,
        }
    }

    #[test]
    fn same_key_same_owner() {
        let roster = vec![
            member("n1", 7946, 8081),
            member("n2", 7947, 8082),
            member("n3", 7948, 8083),
        ];
        let ring1 = CoordinatorRing::from_roster(&roster).unwrap();
        let ring2 = CoordinatorRing::from_roster(&roster).unwrap();
        let o1 = ring1.owner_for_key(b"apple").unwrap().id.clone();
        let o2 = ring2.owner_for_key(b"apple").unwrap().id.clone();
        assert_eq!(o1, o2);
    }

    #[test]
    fn owners_are_cluster_members() {
        let roster = vec![
            member("n1", 7946, 8081),
            member("n2", 7947, 8082),
            member("n3", 7948, 8083),
        ];
        let ring = CoordinatorRing::from_roster(&roster).unwrap();
        let owner = ring.owner_for_key(b"banana").unwrap();
        assert!(["n1", "n2", "n3"].contains(&owner.id.as_str()));
    }
}
