//! Experiment 3 — Recovery speed after node outage.
//!
//!   3a — Hint delivery, data intact (same UUID, RocksDB preserved)
//!   3b — Reconciliation after data loss (new UUID, wiped store)
//!
//! Run with:
//!   cargo test --features test-utils --test eval_recovery -- --nocapture
//!
//! Outputs:
//!   eval_recovery_3a.csv   — hint_count vs recovery_ms
//!   eval_recovery_3b.csv   — key_count vs recovery_ms
//!
//! Plot:
//!   python eval/plot_recovery.py

use std::fs::File;
use std::io::Write as IoWrite;
use std::time::{Duration, Instant};

use serial_test::serial;

use diynamo::client::KvClient;
use diynamo::cluster::CoordinatorRing;
use diynamo::store::StorageEngine;
use diynamo::test_support::{poll_until, TestCluster};

const VALUE: &str = "diynamo-recovery-eval-padding-xxxxxxxxxxxxxxxx";
const GOSSIP_GONE_TIMEOUT: Duration = Duration::from_secs(45);
const RECOVERY_TIMEOUT: Duration = Duration::from_secs(180);
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const PUT_BATCH_SIZE: usize = 32;
const PUT_TIMEOUT: Duration = Duration::from_secs(10);

/// Hint counts to sweep in 3a.
const HINT_COUNTS: &[usize] = &[500, 1000, 2000, 5000];

/// Key counts to sweep in 3b.
const KEY_COUNTS: &[usize] = &[1000, 5000, 10000, 25000];

struct TrialResult {
    data_amount: usize,
    recovery_ms: u64,
    success: bool,
}

fn five_node_ids() -> [&'static str; 5] {
    ["n1", "n2", "n3", "n4", "n5"]
}

fn pref_list(cluster: &TestCluster, key: &str) -> Vec<String> {
    let ring = CoordinatorRing::from_roster(&cluster.roster, cluster.n, cluster.vnodes).unwrap();
    ring.ring_order_for_key(key.as_bytes())
        .unwrap()
        .into_iter()
        .take(cluster.n)
        .map(|m| m.id.clone())
        .collect()
}

/// Collect `count` keys whose preference list includes `dead_id`.
fn keys_for_dead_node(cluster: &TestCluster, dead_id: &str, count: usize) -> Vec<String> {
    let ring = CoordinatorRing::from_roster(&cluster.roster, cluster.n, cluster.vnodes).unwrap();
    let mut keys = Vec::with_capacity(count);
    let mut i = 0usize;
    while keys.len() < count {
        let key = format!("recovery-{dead_id}-{i}");
        let pref = ring
            .preference_list_for_key(key.as_bytes(), cluster.n)
            .unwrap();
        if pref.iter().any(|m| m.id == dead_id) {
            keys.push(key);
        }
        i += 1;
        if i > count * 50 {
            panic!("could not find {count} keys for dead node {dead_id}");
        }
    }
    keys
}

fn live_coordinator<'a>(
    cluster: &'a TestCluster,
    exclude: &str,
) -> &'a diynamo::test_support::TestNode {
    cluster
        .nodes
        .iter()
        .find(|n| n.id != exclude)
        .expect("live coordinator")
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

async fn wait_peer_sees(cluster: &TestCluster, observer_id: &str, dead_id: &str) {
    let observer_id = observer_id.to_string();
    let dead_id = dead_id.to_string();
    poll_until(RECOVERY_TIMEOUT, POLL_INTERVAL, || {
        let cluster = cluster;
        let observer_id = observer_id.clone();
        let dead_id = dead_id.clone();
        async move {
            cluster
                .node(&observer_id)
                .peer_sees_node(&dead_id)
                .await
        }
    })
    .await
    .expect("peer should see recovered node");
}

async fn wait_peer_gone(cluster: &TestCluster, observer_id: &str, dead_id: &str) {
    let observer_id = observer_id.to_string();
    let dead_id = dead_id.to_string();
    poll_until(GOSSIP_GONE_TIMEOUT, POLL_INTERVAL, || {
        let cluster = cluster;
        let observer_id = observer_id.clone();
        let dead_id = dead_id.clone();
        async move {
            !cluster
                .node(&observer_id)
                .peer_sees_node(&dead_id)
                .await
        }
    })
    .await
    .expect("peer should stop seeing dead node");
}

async fn batch_put_keys(base_url: &str, keys: &[String]) {
    let client = KvClient::new(base_url)
        .expect("client")
        .with_request_timeout(PUT_TIMEOUT);
    for batch_start in (0..keys.len()).step_by(PUT_BATCH_SIZE) {
        let batch_end = (batch_start + PUT_BATCH_SIZE).min(keys.len());
        let mut tasks = tokio::task::JoinSet::new();
        for key in &keys[batch_start..batch_end] {
            let client = client.clone();
            let key = key.clone();
            tasks.spawn(async move {
                client.put(&key, VALUE).await.expect("put");
            });
        }
        while tasks.join_next().await.is_some() {}
    }
}

async fn all_keys_local(cluster: &TestCluster, node_id: &str, keys: &[String]) -> bool {
    for key in keys {
        match cluster.node(node_id).local.get(key.as_bytes()).await {
            Ok(Some(v)) if v.data == VALUE.as_bytes() => {}
            _ => return false,
        }
    }
    true
}

async fn wait_until_keys_local(
    cluster: &TestCluster,
    node_id: &str,
    keys: &[String],
) -> Result<(), anyhow::Error> {
    let node_id = node_id.to_string();
    let keys = keys.to_vec();
    poll_until(RECOVERY_TIMEOUT, POLL_INTERVAL, || {
        let cluster = cluster;
        let node_id = node_id.clone();
        let keys = keys.clone();
        async move { all_keys_local(cluster, &node_id, &keys).await }
    })
    .await
}

async fn wait_until_hints_drained_and_keys_local(
    cluster: &TestCluster,
    dead_id: &str,
    keys: &[String],
) -> Result<(), anyhow::Error> {
    let dead_id = dead_id.to_string();
    let keys = keys.to_vec();
    poll_until(RECOVERY_TIMEOUT, POLL_INTERVAL, || {
        let cluster = cluster;
        let dead_id = dead_id.clone();
        let keys = keys.clone();
        async move {
            total_hints_for_node(cluster, &dead_id) == 0
                && all_keys_local(cluster, &dead_id, &keys).await
        }
    })
    .await
}

// ---------------------------------------------------------------------------
// Experiment 3a
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn experiment_3a_hint_recovery_data_intact() {
    println!("\n┌─ Experiment 3a: Hint delivery (data intact) ──────────────────────────┐");
    println!("│  Cluster : 5 nodes, N=3, W=2, R=2, vnodes=3                          │");
    println!("│  Sweep   : {:?} hints during outage", HINT_COUNTS);
    println!("└────────────────────────────────────────────────────────────────────────┘");

    let mut results: Vec<TrialResult> = Vec::new();

    for &hint_target in HINT_COUNTS {
        let cluster = TestCluster::spawn_with_vnodes(&five_node_ids(), 3, 2, 2, 3)
            .await
            .expect("spawn cluster");

        let probe_key = "recovery-probe";
        let dead_id = pref_list(&cluster, probe_key)[0].clone();
        let observer_id = pref_list(&cluster, probe_key)[1].clone();
        let seed_gossip = cluster.roster[0].gossip_addr;
        let observer = cluster.node(&observer_id);

        let uuid_before = observer
            .peer_uuid(&dead_id)
            .await
            .expect("dead node visible before outage");

        cluster
            .node(&dead_id)
            .kill_node()
            .await
            .expect("kill node");
        wait_peer_gone(&cluster, &observer_id, &dead_id).await;

        let keys = keys_for_dead_node(&cluster, &dead_id, hint_target);
        let coordinator = live_coordinator(&cluster, &dead_id);
        batch_put_keys(&coordinator.http_url, &keys).await;

        let hint_count = total_hints_for_node(&cluster, &dead_id);
        assert!(
            hint_count > 0,
            "expected hints for dead node, got {hint_count}"
        );

        println!(
            "\n  target={hint_target}  hints_stored={hint_count}  recovering …"
        );
        let t0 = Instant::now();
        let recover_ok = cluster
            .node(&dead_id)
            .recover_after_outage_data_intact(seed_gossip, observer, uuid_before, GOSSIP_GONE_TIMEOUT)
            .await
            .is_ok();

        let wait_ok = if recover_ok {
            wait_until_hints_drained_and_keys_local(&cluster, &dead_id, &keys)
                .await
                .is_ok()
        } else {
            false
        };
        let success = recover_ok && wait_ok;
        let recovery_ms = t0.elapsed().as_millis() as u64;

        println!(
            "  done  recovery_ms={recovery_ms}  success={}",
            if success { "yes" } else { "no" }
        );

        results.push(TrialResult {
            data_amount: hint_count,
            recovery_ms,
            success,
        });

        cluster.shutdown_all().await;
    }

    write_csv(
        "eval_recovery_3a.csv",
        "hint_count,recovery_ms,success",
        &results,
        "3a",
    );
}

// ---------------------------------------------------------------------------
// Experiment 3b
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn experiment_3b_reconciliation_after_data_loss() {
    println!("\n┌─ Experiment 3b: Reconciliation (data loss) ───────────────────────────┐");
    println!("│  Cluster : 5 nodes, N=3, W=2, R=2, vnodes=3                          │");
    println!("│  Sweep   : {:?} keys to migrate", KEY_COUNTS);
    println!("└────────────────────────────────────────────────────────────────────────┘");

    let mut results: Vec<TrialResult> = Vec::new();

    for &key_target in KEY_COUNTS {
        let cluster = TestCluster::spawn_with_vnodes(&five_node_ids(), 3, 2, 2, 3)
            .await
            .expect("spawn cluster");

        let probe_key = "recovery-probe";
        let dead_id = pref_list(&cluster, probe_key)[0].clone();
        let observer_id = pref_list(&cluster, probe_key)[1].clone();
        let seed_gossip = cluster.roster[0].gossip_addr;
        let observer = cluster.node(&observer_id);

        let keys = keys_for_dead_node(&cluster, &dead_id, key_target);
        let coordinator = live_coordinator(&cluster, &dead_id);
        batch_put_keys(&coordinator.http_url, &keys).await;
        wait_until_keys_local(&cluster, &dead_id, &keys)
            .await
            .expect("keys replicated before outage");

        let uuid_before = observer
            .peer_uuid(&dead_id)
            .await
            .expect("dead node visible before outage");

        cluster
            .node(&dead_id)
            .kill_node()
            .await
            .expect("kill node");
        wait_peer_gone(&cluster, &observer_id, &dead_id).await;

        println!("\n  target={key_target}  keys_on_dead={}  recovering …", keys.len());
        let t0 = Instant::now();
        let recover_ok = cluster
            .node(&dead_id)
            .recover_after_data_loss(seed_gossip, observer, GOSSIP_GONE_TIMEOUT)
            .await
            .is_ok();

        if recover_ok {
            wait_peer_sees(&cluster, &observer_id, &dead_id).await;
        }
        let uuid_changed = observer
            .peer_uuid(&dead_id)
            .await
            .map(|u| u != uuid_before)
            .unwrap_or(false);
        let wait_ok = if recover_ok {
            wait_until_keys_local(&cluster, &dead_id, &keys).await.is_ok()
        } else {
            false
        };
        let success = recover_ok && uuid_changed && wait_ok;
        let recovery_ms = t0.elapsed().as_millis() as u64;

        println!(
            "  done  recovery_ms={recovery_ms}  success={}",
            if success { "yes" } else { "no" }
        );

        results.push(TrialResult {
            data_amount: keys.len(),
            recovery_ms,
            success,
        });

        cluster.shutdown_all().await;
    }

    write_csv(
        "eval_recovery_3b.csv",
        "key_count,recovery_ms,success",
        &results,
        "3b",
    );
}

fn write_csv(path: &str, header: &str, results: &[TrialResult], _experiment: &str) {
    let mut f = File::create(path).expect("create CSV");
    writeln!(f, "{header}").unwrap();
    for r in results {
        writeln!(
            f,
            "{},{},{}",
            r.data_amount, r.recovery_ms, r.success as u8
        )
        .unwrap();
    }
    println!("\nWrote {path}");
}
