use serde::{Deserialize, Serialize};

/// JSON body for `PUT /kv/{key}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutBody {
    pub value: String,
}

/// JSON body for `PUT /internal/kv-versioned/{key}`.
///
/// Carries an explicit timestamp so read repair writes preserve the original
/// write timestamp rather than generating a fresh one. This prevents a delayed
/// repair from silently overwriting a concurrent write that has a newer timestamp.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutVersionedBody {
    pub value: String,
    pub timestamp: u64,
}

/// JSON body for `GET /kv/{key}` and `GET /internal/kv/{key}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetResponse {
    pub value: String,
    pub timestamp: u64,
}
