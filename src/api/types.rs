use serde::{Deserialize, Serialize};

/// JSON body for `PUT /kv/{key}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutBody {
    pub value: String,
}

/// JSON body for `GET /kv/{key}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetResponse {
    pub value: String,
}
