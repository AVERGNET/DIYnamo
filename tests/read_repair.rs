//! Integration tests for quorum read + read repair.

use std::time::Duration;

use serial_test::serial;

use diynamo::client::KvClient;
use diynamo::cluster::CoordinatorRing;
use diynamo::store::StorageEngine;
use diynamo::test_support::{poll_until, TestCluster};

const TEST_KEY: &str = "apple";
const OLD_VALUE: &str = "old";
const NEW_VALUE: &str = "new";
const OLD_TS: u64 = 100;
const NEW_TS: u64 = 200;

const REPAIR_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

fn five_node_ids() -> Vec<&'static str> {
    vec!["n1", "n2", "n3", "n4", "n5"]
}

fn ring_order_ids(cluster: &TestCluster, key: &str) -> Vec<String> {
    let ring = CoordinatorRing::from_roster(&cluster.roster, cluster.n, cluster.vnodes).unwrap();
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

async fn local_at(cluster: &TestCluster, node_id: &str, key: &str) -> Option<(Vec<u8>, u64)> {
    cluster
        .node(node_id)
        .local
        .get(key.as_bytes())
        .await
        .ok()
        .flatten()
        .map(|v| (v.data, v.timestamp))
}

fn live_coordinator<'a>(cluster: &'a TestCluster, exclude: &[String]) -> &'a diynamo::test_support::TestNode {
    cluster
        .nodes
        .iter()
        .find(|n| !exclude.contains(&n.id))
        .unwrap_or_else(|| panic!("no live coordinator outside exclude list"))
}

fn seed_fresh_replicas(cluster: &TestCluster, pref: &[String]) {
    for id in &pref[1..] {
        cluster
            .node(id)
            .local
            .put_if_newer(TEST_KEY.as_bytes(), NEW_VALUE.as_bytes(), NEW_TS)
            .expect("seed fresh replica");
    }
}

fn seed_stale_replica(cluster: &TestCluster, node_id: &str) {
    cluster
        .node(node_id)
        .local
        .put_if_newer(TEST_KEY.as_bytes(), OLD_VALUE.as_bytes(), OLD_TS)
        .expect("seed stale replica");
}

#[tokio::test]
#[serial]
async fn read_repair_updates_stale_replica() {
    let cluster = TestCluster::spawn_with_vnodes(&five_node_ids(), 3, 2, 2, 3)
        .await
        .expect("spawn cluster");

    let pref = pref_list(&cluster, TEST_KEY);
    assert_eq!(pref.len(), 3);

    seed_stale_replica(&cluster, &pref[0]);
    seed_fresh_replicas(&cluster, &pref);

    let coordinator = live_coordinator(&cluster, &pref);
    let client = KvClient::new(&coordinator.http_url).expect("client");
    let resp = client.get(TEST_KEY).await.expect("get should succeed");
    assert_eq!(resp.value, NEW_VALUE);
    assert_eq!(resp.timestamp, NEW_TS);

    let stale_id = pref[0].clone();
    poll_until(REPAIR_TIMEOUT, POLL_INTERVAL, || {
        let cluster = &cluster;
        let stale_id = stale_id.clone();
        async move {
            matches!(
                local_at(cluster, &stale_id, TEST_KEY).await,
                Some((ref data, ts)) if data == NEW_VALUE.as_bytes() && ts == NEW_TS
            )
        }
    })
    .await
    .expect("stale replica should be repaired");

    for id in &pref[1..] {
        assert_eq!(
            local_at(&cluster, id, TEST_KEY).await,
            Some((NEW_VALUE.as_bytes().to_vec(), NEW_TS))
        );
    }

    cluster.shutdown_all().await;
}

#[tokio::test]
#[serial]
async fn read_repair_fills_missing_replica() {
    let cluster = TestCluster::spawn_with_vnodes(&five_node_ids(), 3, 2, 2, 3)
        .await
        .expect("spawn cluster");

    let pref = pref_list(&cluster, TEST_KEY);
    assert_eq!(pref.len(), 3);
    assert!(
        local_at(&cluster, &pref[0], TEST_KEY).await.is_none(),
        "missing replica should start empty"
    );

    seed_fresh_replicas(&cluster, &pref);

    let coordinator = live_coordinator(&cluster, &pref);
    let client = KvClient::new(&coordinator.http_url).expect("client");
    let resp = client.get(TEST_KEY).await.expect("get should succeed");
    assert_eq!(resp.value, NEW_VALUE);
    assert_eq!(resp.timestamp, NEW_TS);

    let missing_id = pref[0].clone();
    poll_until(REPAIR_TIMEOUT, POLL_INTERVAL, || {
        let cluster = &cluster;
        let missing_id = missing_id.clone();
        async move {
            matches!(
                local_at(cluster, &missing_id, TEST_KEY).await,
                Some((ref data, ts)) if data == NEW_VALUE.as_bytes() && ts == NEW_TS
            )
        }
    })
    .await
    .expect("missing replica should be filled");

    cluster.shutdown_all().await;
}

#[tokio::test]
#[serial]
async fn read_repair_skips_unresponsive_replica() {
    let cluster = TestCluster::spawn_with_vnodes(&five_node_ids(), 3, 2, 2, 3)
        .await
        .expect("spawn cluster");

    let pref = pref_list(&cluster, TEST_KEY);
    assert_eq!(pref.len(), 3);

    seed_stale_replica(&cluster, &pref[0]);
    seed_fresh_replicas(&cluster, &pref);

    let coordinator = live_coordinator(&cluster, &pref);
    let client = KvClient::new(&coordinator.http_url).expect("client");

    cluster.node(&pref[0]).faults.block_internal(true);
    let resp = client.get(TEST_KEY).await.expect("get should succeed with R=2");
    cluster.node(&pref[0]).faults.block_internal(false);

    assert_eq!(resp.value, NEW_VALUE);
    assert_eq!(resp.timestamp, NEW_TS);
    assert_eq!(
        local_at(&cluster, &pref[0], TEST_KEY).await,
        Some((OLD_VALUE.as_bytes().to_vec(), OLD_TS)),
        "unresponsive replica should not be repaired"
    );

    cluster.shutdown_all().await;
}

#[tokio::test]
#[serial]
async fn put_if_newer_rejects_stale_repair() {
    let cluster = TestCluster::spawn_with_vnodes(&five_node_ids(), 3, 2, 2, 3)
        .await
        .expect("spawn cluster");

    let pref = pref_list(&cluster, TEST_KEY);
    let target_id = &pref[0];
    cluster
        .node(target_id)
        .local
        .put_if_newer(TEST_KEY.as_bytes(), NEW_VALUE.as_bytes(), NEW_TS)
        .expect("seed fresh value");

    let client = KvClient::new(&cluster.node(target_id).http_url).expect("client");
    client
        .put_internal_versioned_bytes(TEST_KEY.as_bytes(), OLD_VALUE.as_bytes(), OLD_TS)
        .await
        .expect("stale repair RPC should succeed at HTTP level");

    assert_eq!(
        local_at(&cluster, target_id, TEST_KEY).await,
        Some((NEW_VALUE.as_bytes().to_vec(), NEW_TS)),
        "stale repair must not regress fresher data"
    );

    cluster.shutdown_all().await;
}
