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

const RING_VNODES: usize = 1;

/// Consistent hash ring built from the static config roster.
pub struct CoordinatorRing {
    members_by_id: HashMap<String, MemberInfo>,
    ring: HashRing<RingNode>,
    n: usize,
}

impl CoordinatorRing {
    /// Build the ring from a static roster.
    ///
    /// Errors if the roster is empty or smaller than `n` — a cluster with fewer
    /// nodes than the replication factor can never satisfy quorum.
    pub fn from_roster(roster: &[ClusterMember], n: usize) -> Result<Self> {
        if roster.is_empty() {
            bail!("cluster roster is empty");
        }
        if roster.len() < n {
            bail!(
                "roster has {} members but n={}, roster must have at least n members",
                roster.len(),
                n
            );
        }

        let members_by_id: HashMap<String, MemberInfo> = roster
            .iter()
            .cloned()
            .map(MemberInfo::from)
            .map(|m| (m.id.clone(), m))
            .collect();

        // replicas = n - 1 so ring.get(key) returns n nodes (1 primary + n-1 replicas).
        let mut ring = HashRing::new(n - 1, RING_VNODES);
        let nodes: Vec<RingNode> = members_by_id
            .keys()
            .map(|id| RingNode { id: id.clone() })
            .collect();
        ring.batch_add(nodes);

        Ok(Self {
            members_by_id,
            ring,
            n,
        })
    }

    /// Return up to `n` nodes in ring order for the given key.
    ///
    /// Truncates to `n` if the caller requests fewer than the ring was built for.
    /// Returns an error if any ring node id is missing from `members_by_id` (should
    /// not happen in normal operation).
    pub fn preference_list_for_key(&self, key: &[u8], n: usize) -> Result<Vec<&MemberInfo>> {
        let key_str = std::str::from_utf8(key).context("key must be valid UTF-8")?;
        let owners = self.ring.get(&key_str);
        if owners.is_empty() {
            bail!("hash ring returned no nodes for key");
        }
        owners
            .iter()
            .take(n)
            .map(|node| {
                self.members_by_id
                    .get(&node.id)
                    .with_context(|| format!("ring node '{}' missing from roster", node.id))
            })
            .collect()
    }

    pub fn n(&self) -> usize {
        self.n
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

    fn three_node_roster() -> Vec<ClusterMember> {
        vec![
            member("n1", 7946, 8081),
            member("n2", 7947, 8082),
            member("n3", 7948, 8083),
        ]
    }

    #[test]
    fn same_key_same_preference_list() {
        let roster = three_node_roster();
        let ring1 = CoordinatorRing::from_roster(&roster, 3).unwrap();
        let ring2 = CoordinatorRing::from_roster(&roster, 3).unwrap();
        let list1: Vec<_> = ring1
            .preference_list_for_key(b"apple", 3)
            .unwrap()
            .iter()
            .map(|m| m.id.clone())
            .collect();
        let list2: Vec<_> = ring2
            .preference_list_for_key(b"apple", 3)
            .unwrap()
            .iter()
            .map(|m| m.id.clone())
            .collect();
        assert_eq!(list1, list2);
    }

    #[test]
    fn preference_list_members_are_cluster_members() {
        let roster = three_node_roster();
        let ring = CoordinatorRing::from_roster(&roster, 3).unwrap();
        let list = ring.preference_list_for_key(b"banana", 3).unwrap();
        assert!(!list.is_empty());
        for member in &list {
            assert!(["n1", "n2", "n3"].contains(&member.id.as_str()));
        }
    }

    #[test]
    fn preference_list_truncated_to_requested_n() {
        let roster = three_node_roster();
        let ring = CoordinatorRing::from_roster(&roster, 3).unwrap();
        let list = ring.preference_list_for_key(b"cherry", 1).unwrap();
        assert_eq!(list.len(), 1);
    }

    #[test]
    fn from_roster_rejects_smaller_than_n() {
        let roster = vec![member("n1", 7946, 8081), member("n2", 7947, 8082)];
        assert!(CoordinatorRing::from_roster(&roster, 3).is_err());
    }
}
