//! Integration tests for hinted handoff when preferred nodes fail.

use diynamo::client::KvClient;
use diynamo::cluster::CoordinatorRing;
use diynamo::test_support::TestCluster;

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
}

#[tokio::test]
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
}
