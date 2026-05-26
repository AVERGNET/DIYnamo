use crate::cluster::membership::MembershipView;
use crate::cluster::types::MemberInfo;
use std::{sync::Arc, time::Duration};

/// Format members for display (sorted for stable output).
pub fn format_live_set(members: &[MemberInfo]) -> String {
    let mut sorted: Vec<_> = members.to_vec();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));
    if sorted.is_empty() {
        return "[live] 0 members".to_string();
    }
    let list = sorted
        .iter()
        .map(|m| m.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!("[live] {} members: {}", sorted.len(), list)
}

/// Print the online member set every `interval`.
pub async fn run_live_set_printer<V: MembershipView>(
    view: Arc<V>,
    interval: Duration,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        let members = view.online_members().await;
        println!("{}", format_live_set(&members));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_live_set_sorted() {
        let members = vec![
            MemberInfo {
                id: "n2".into(),
                gossip_addr: "127.0.0.1:7947".parse().unwrap(),
                forward_port: 8082,
            },
            MemberInfo {
                id: "n1".into(),
                gossip_addr: "127.0.0.1:7946".parse().unwrap(),
                forward_port: 8081,
            },
        ];
        let s = format_live_set(&members);
        assert!(s.contains("n1@127.0.0.1:7946"));
        assert!(s.contains("n2@127.0.0.1:7947"));
        assert!(s.find("n1").unwrap() < s.find("n2").unwrap());
    }
}
