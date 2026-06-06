//! Integration tests for node recovery after total data loss (new UUID + empty store).

use std::time::Duration;

use serial_test::serial;

use diynamo::client::KvClient;
use diynamo::cluster::CoordinatorRing;
use diynamo::store::StorageEngine;
use diynamo::test_support::{poll_until, TestCluster};

const KEY_BEFORE: &str = "apple";
const VALUE_BEFORE: &str = "value-before";
const VALUE_DURING: &str = "value-during";

const GOSSIP_GONE_TIMEOUT: Duration = Duration::from_secs(45);
const MIGRATION_TIMEOUT: Duration = Duration::from_secs(45);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

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

fn pref_list(cluster: &TestCluster, key: &str) -> Vec<String> {
    ring_order_ids(cluster, key)
        .into_iter()
        .take(cluster.n)
        .collect()
}

async fn local_value(cluster: &TestCluster, node_id: &str, key: &str) -> Option<Vec<u8>> {
    cluster
        .node(node_id)
        .local
        .get(key.as_bytes())
        .await
        .ok()
        .flatten()
        .map(|v| v.data)
}

async fn local_is_empty(cluster: &TestCluster, node_id: &str, keys: &[&str]) -> bool {
    for key in keys {
        if local_value(cluster, node_id, key).await.is_some() {
            return false;
        }
    }
    true
}

async fn assert_node_has_keys(
    cluster: &TestCluster,
    node_id: &str,
    keys: &[String],
    expected_value: &str,
) {
    for key in keys {
        let expected = expected_value.as_bytes();
        let actual = local_value(cluster, node_id, key)
            .await
            .unwrap_or_else(|| panic!("key {key} missing on {node_id}"));
        assert_eq!(actual, expected, "wrong value for key {key} on {node_id}");
    }
}

fn live_coordinator<'a>(cluster: &'a TestCluster, exclude: &str) -> &'a diynamo::test_support::TestNode {
    cluster
        .nodes
        .iter()
        .find(|n| n.id != exclude)
        .expect("live coordinator")
}

async fn wait_until_peer_sees_node(observer: &diynamo::test_support::TestNode, target_id: &str) {
    poll_until(MIGRATION_TIMEOUT, POLL_INTERVAL, || {
        let observer = observer;
        let target_id = target_id.to_string();
        async move { observer.peer_sees_node(&target_id).await }
    })
    .await
    .expect("peer should see recovered node");
}

async fn assert_uuid_changed(
    observer: &diynamo::test_support::TestNode,
    dead_id: &str,
    uuid_before: [u8; 16],
) {
    wait_until_peer_sees_node(observer, dead_id).await;
    let uuid_after = observer
        .peer_uuid(dead_id)
        .await
        .expect("recovered node should appear in gossip");
    assert_ne!(
        uuid_before, uuid_after,
        "recovered node must advertise a new startup UUID"
    );
}

fn total_hints_for_node(cluster: &TestCluster, dead_id: &str) -> usize {
    cluster
        .nodes
        .iter()
        .map(|n| {
            n.hints
                .hints_for_node(dead_id)
                .map(|h| h.len())
                .unwrap_or(0)
        })
        .sum()
}

async fn wait_until_node_has_keys(
    cluster: &TestCluster,
    node_id: &str,
    keys: &[String],
    expected_value: &str,
) {
    let node_id = node_id.to_string();
    let keys = keys.to_vec();
    let expected_value = expected_value.to_string();
    poll_until(MIGRATION_TIMEOUT, POLL_INTERVAL, || {
        let cluster = cluster;
        let node_id = node_id.clone();
        let keys = keys.clone();
        let expected_value = expected_value.clone();
        async move {
            for key in &keys {
                let expected = expected_value.as_bytes();
                match local_value(cluster, &node_id, key).await {
                    Some(ref data) if data == expected => {}
                    _ => return false,
                }
            }
            true
        }
    })
    .await
    .expect("migration should restore all expected keys");
}

#[tokio::test]
#[serial]
async fn reconciliation_restores_keys_after_data_loss() {
    let cluster = TestCluster::spawn(&five_node_ids(), 3, 2, 2)
        .await
        .expect("spawn cluster");

    let dead_id = pref_list(&cluster, KEY_BEFORE)[0].clone();
    let observer_id = pref_list(&cluster, KEY_BEFORE)[1].clone();
    let seed_gossip = cluster.roster[0].gossip_addr;

    let observer = cluster.node(&observer_id);
    let uuid_before = observer
        .peer_uuid(&dead_id)
        .await
        .expect("dead node visible before outage");

    let coordinator = live_coordinator(&cluster, &dead_id);
    let client = KvClient::new(&coordinator.http_url).expect("client");
    client
        .put(KEY_BEFORE, VALUE_BEFORE)
        .await
        .expect("put before outage");

    cluster
        .node(&dead_id)
        .kill_node()
        .await
        .expect("kill node");

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
    .expect("peer should stop seeing dead node");

    cluster
        .node(&dead_id)
        .recover_after_data_loss(seed_gossip, observer, GOSSIP_GONE_TIMEOUT)
        .await
        .expect("recover after data loss");

    assert!(
        local_is_empty(&cluster, &dead_id, &[KEY_BEFORE]).await,
        "recovered node should start with empty store"
    );

    assert_uuid_changed(observer, &dead_id, uuid_before).await;

    wait_until_node_has_keys(
        &cluster,
        &dead_id,
        &[KEY_BEFORE.to_string()],
        VALUE_BEFORE,
    )
    .await;
    assert_node_has_keys(
        &cluster,
        &dead_id,
        &[KEY_BEFORE.to_string()],
        VALUE_BEFORE,
    )
    .await;
    assert_eq!(total_hints_for_node(&cluster, &dead_id), 0);

    cluster.shutdown_all().await;
}

#[tokio::test]
#[serial]
async fn hints_and_reconciliation_restore_all_keys_after_data_loss() {
    let cluster = TestCluster::spawn(&five_node_ids(), 3, 2, 2)
        .await
        .expect("spawn cluster");

    let dead_id = pref_list(&cluster, KEY_BEFORE)[0].clone();
    let observer_id = pref_list(&cluster, KEY_BEFORE)[1].clone();
    let seed_gossip = cluster.roster[0].gossip_addr;

    let observer = cluster.node(&observer_id);
    let uuid_before = observer
        .peer_uuid(&dead_id)
        .await
        .expect("dead node visible before outage");

    let coordinator = live_coordinator(&cluster, &dead_id);
    let client = KvClient::new(&coordinator.http_url).expect("client");
    client
        .put(KEY_BEFORE, VALUE_BEFORE)
        .await
        .expect("put before outage");

    cluster
        .node(&dead_id)
        .kill_node()
        .await
        .expect("kill node");

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
    .expect("peer should stop seeing dead node");

    client
        .put(KEY_BEFORE, VALUE_DURING)
        .await
        .expect("put while dead node is down");

    let order = ring_order_ids(&cluster, KEY_BEFORE);
    let candidate_id = order[cluster.n].clone();
    let candidate_hints = cluster
        .node(&candidate_id)
        .hints
        .hints_for_node(&dead_id)
        .expect("read hints");
    assert!(
        !candidate_hints.is_empty(),
        "hint should exist before recovery"
    );

    cluster
        .node(&dead_id)
        .recover_after_data_loss(seed_gossip, observer, GOSSIP_GONE_TIMEOUT)
        .await
        .expect("recover after data loss");

    assert!(
        local_is_empty(&cluster, &dead_id, &[KEY_BEFORE]).await,
        "recovered node should start with empty store"
    );

    assert_uuid_changed(observer, &dead_id, uuid_before).await;

    wait_until_node_has_keys(
        &cluster,
        &dead_id,
        &[KEY_BEFORE.to_string()],
        VALUE_DURING,
    )
    .await;
    assert_node_has_keys(
        &cluster,
        &dead_id,
        &[KEY_BEFORE.to_string()],
        VALUE_DURING,
    )
    .await;
    assert_eq!(total_hints_for_node(&cluster, &dead_id), 0);

    cluster.shutdown_all().await;
}
