//! Integration tests for hinted handoff when preferred nodes fail.

use std::time::Duration;

use serial_test::serial;

use diynamo::client::KvClient;
use diynamo::cluster::CoordinatorRing;
use diynamo::store::StorageEngine;
use diynamo::test_support::{poll_until, TestCluster};

const TEST_KEY: &str = "apple";
const TEST_VALUE: &str = "test-value";

fn five_node_ids() -> Vec<&'static str> {
    vec!["n1", "n2", "n3", "n4", "n5"]
}

fn ring_order_ids(cluster: &TestCluster, key: &str) -> Vec<String> {
    let ring = CoordinatorRing::from_roster(&cluster.roster, cluster.n).unwrap();
    ring.ring_order_for_key(key.as_bytes())
        .unwrap()
        .into_iter()
        .map(|m| m.id.clone())
        .collect()
}

#[tokio::test]
#[serial]
async fn hint_stored_on_first_ring_successor() {
    let cluster = TestCluster::spawn(&five_node_ids(), 3, 2, 2)
        .await
        .expect("spawn cluster");

    let order = ring_order_ids(&cluster, TEST_KEY);
    assert_eq!(order.len(), 5);

    let pref_list: Vec<_> = order.iter().take(3).cloned().collect();
    let dead_id = &pref_list[0];
    let expected_candidate_id = &order[3];

    assert!(
        !pref_list.contains(expected_candidate_id),
        "hint candidate must not be in preference list"
    );

    cluster.node(dead_id).faults.block_internal(true);

    let coordinator = cluster
        .nodes
        .iter()
        .find(|n| n.id != *dead_id)
        .expect("live coordinator");
    let client = KvClient::new(&coordinator.http_url).expect("client");
    client
        .put(TEST_KEY, TEST_VALUE)
        .await
        .expect("put should succeed with W=2");

    let candidate_hints = cluster
        .node(expected_candidate_id)
        .hints
        .hints_for_node(dead_id)
        .expect("read hints");
    assert_eq!(candidate_hints.len(), 1);
    assert_eq!(candidate_hints[0].0, TEST_KEY.as_bytes());
    assert_eq!(
        String::from_utf8(candidate_hints[0].1.clone()).unwrap(),
        TEST_VALUE
    );

    let dead_hints = cluster
        .node(dead_id)
        .hints
        .hints_for_node(dead_id)
        .expect("read dead hints");
    assert!(dead_hints.is_empty());
    cluster.shutdown_all().await;
}

#[tokio::test]
#[serial]
async fn hint_fails_over_to_second_candidate() {
    let cluster = TestCluster::spawn(&five_node_ids(), 3, 2, 2)
        .await
        .expect("spawn cluster");

    let order = ring_order_ids(&cluster, TEST_KEY);
    let dead_id = &order[0];
    let first_candidate_id = &order[3];
    let second_candidate_id = &order[4];

    cluster.node(dead_id).faults.block_internal(true);
    cluster.node(first_candidate_id).faults.block_hints(true);

    let coordinator = cluster
        .nodes
        .iter()
        .find(|n| n.id != *dead_id && n.id != *first_candidate_id)
        .expect("live coordinator");
    let client = KvClient::new(&coordinator.http_url).expect("client");
    client
        .put(TEST_KEY, TEST_VALUE)
        .await
        .expect("put should succeed");

    let first_hints = cluster
        .node(first_candidate_id)
        .hints
        .hints_for_node(dead_id)
        .expect("read first candidate hints");
    assert!(first_hints.is_empty());

    let second_hints = cluster
        .node(second_candidate_id)
        .hints
        .hints_for_node(dead_id)
        .expect("read second candidate hints");
    assert_eq!(second_hints.len(), 1);
    assert_eq!(second_hints[0].0, TEST_KEY.as_bytes());
    cluster.shutdown_all().await;
}


/// Hint delivery: kill node (HTTP + gossip), write with hint, recover, assert handoff.
#[tokio::test]
#[serial]
async fn hint_delivered_after_http_ports_recover_slow() {
    const GOSSIP_GONE_TIMEOUT: Duration = Duration::from_secs(45);
    const HANDOFF_TIMEOUT: Duration = Duration::from_secs(45);
    const POLL_INTERVAL: Duration = Duration::from_millis(500);

    let cluster = TestCluster::spawn(&five_node_ids(), 3, 2, 2)
        .await
        .expect("spawn cluster");

    let order = ring_order_ids(&cluster, TEST_KEY);
    let dead_id = order[0].clone();
    let candidate_id = order[3].clone();
    let observer_id = order[1].clone();
    let seed_gossip = cluster.roster[0].gossip_addr;

    let dead = cluster.node(&dead_id);
    dead.kill_node().await.expect("kill node (http + gossip)");

    let observer_id_poll = observer_id.clone();
    let dead_id_gone = dead_id.clone();
    poll_until(GOSSIP_GONE_TIMEOUT, POLL_INTERVAL, || {
        let cluster = &cluster;
        let observer_id = observer_id_poll.clone();
        let dead_id = dead_id_gone.clone();
        async move {
            !cluster
                .node(&observer_id)
                .peer_sees_node(&dead_id)
                .await
        }
    })
    .await
    .expect("peer should stop seeing dead node after gossip suspend");

    let coordinator = cluster
        .nodes
        .iter()
        .find(|n| n.id == observer_id)
        .expect("live coordinator");
    let client = KvClient::new(&coordinator.http_url).expect("client");
    client
        .put(TEST_KEY, TEST_VALUE)
        .await
        .expect("put should succeed while dead node is HTTP-down");

    let candidate_hints = cluster
        .node(&candidate_id)
        .hints
        .hints_for_node(&dead_id)
        .expect("read hints");
    assert_eq!(candidate_hints.len(), 1, "hint should exist before recovery");

    cluster
        .node(&dead_id)
        .recover_node(seed_gossip)
        .await
        .expect("recover node (http + gossip)");

    let dead_id_poll = dead_id.clone();
    let candidate_id_poll = candidate_id.clone();
    poll_until(HANDOFF_TIMEOUT, POLL_INTERVAL, || {
        let cluster = &cluster;
        let dead_id = dead_id_poll.clone();
        let candidate_id = candidate_id_poll.clone();
        async move {
            let hints = cluster
                .node(&candidate_id)
                .hints
                .hints_for_node(&dead_id)
                .unwrap_or_default();
            if !hints.is_empty() {
                return false;
            }
            let local = cluster.node(&dead_id).local.get(TEST_KEY.as_bytes()).await;
            matches!(local, Ok(Some(v)) if v.data == TEST_VALUE.as_bytes())
        }
    })
    .await
    .expect("hint delivered after HTTP + gossip recovery");

    let stored = cluster
        .node(&dead_id)
        .local
        .get(TEST_KEY.as_bytes())
        .await
        .expect("read local")
        .expect("key on recovered node");
    assert_eq!(stored.data, TEST_VALUE.as_bytes());
    cluster.shutdown_all().await;
}
