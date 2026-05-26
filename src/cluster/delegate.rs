use memberlist::bytes::Bytes;
use memberlist::proto::Meta;
use memberlist::delegate::NodeDelegate;

/// Metadata gossiped by every node in its alive messages.
///
/// Encoded as a fixed 18-byte array so no external serialization crate is needed
/// and the format stays well within memberlist's META_MAX_SIZE (512 bytes).
///
/// Layout: `[uuid (16 bytes)] ++ [http_port big-endian (2 bytes)]`
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeMeta {
    pub uuid: [u8; 16],
    pub http_port: u16,
}

impl NodeMeta {
    pub fn to_bytes(self) -> [u8; 18] {
        let mut buf = [0u8; 18];
        buf[..16].copy_from_slice(&self.uuid);
        buf[16..18].copy_from_slice(&self.http_port.to_be_bytes());
        buf
    }

    /// Returns `None` if `bytes` is shorter than 18 bytes (node hasn't gossiped
    /// meta yet — safe to skip).
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 18 {
            return None;
        }
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&bytes[..16]);
        let http_port = u16::from_be_bytes([bytes[16], bytes[17]]);
        Some(Self { uuid, http_port })
    }
}

/// Memberlist `NodeDelegate` that advertises this node's `NodeMeta` in every
/// alive gossip message.
pub struct DiynamoNodeDelegate {
    pub local_meta: NodeMeta,
}

impl NodeDelegate for DiynamoNodeDelegate {
    async fn node_meta(&self, _limit: usize) -> Meta {
        let bytes = self.local_meta.to_bytes();
        Meta::try_from(Bytes::copy_from_slice(&bytes)).unwrap_or_default()
    }
}
