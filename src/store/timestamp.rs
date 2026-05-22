use std::time::{SystemTime, UNIX_EPOCH};

pub trait TimestampSource: Send + Sync {
    fn now_millis(&self) -> u64;
}

pub struct SystemTimestamp;

impl TimestampSource for SystemTimestamp {
    fn now_millis(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}
