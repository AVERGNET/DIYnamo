use std::fmt;

/// A member in the gossip cluster view.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MemberInfo {
    pub id: String,
    pub addr: String,
}

impl fmt::Display for MemberInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.addr)
    }
}
