//! Experiment 2 — Write/read success under node failure.
//!
//! Run with:
//!   cargo test --features test-utils --test eval_failure -- --nocapture
//!
//! Sweeps concurrency while 1/2/3 nodes are dead (spread on the ring, not
//! colocated in one preference list).  N=3, W=2, R=2 on 5 nodes.
//!
//! Outputs:
//!   eval_failure_samples.csv
//!   eval_failure_summary.csv
//!
//! Plot:
//!   python eval/plot_failure.py eval_failure_summary.csv

use std::collections::HashSet;
use std::fs::File;
use std::io::Write as IoWrite;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serial_test::serial;

use diynamo::client::KvClient;
use diynamo::cluster::CoordinatorRing;
use diynamo::test_support::{poll_until, TestCluster};

const CONCURRENCY_LEVELS: &[usize] = &[1, 2, 4, 8, 16, 32, 48];
const ALIVE_COUNTS: &[usize] = &[4, 3, 2];
const NODE_COUNT: usize = 5;

const WARMUP: Duration = Duration::from_secs(5);
const MEASURE: Duration = Duration::from_secs(10);
const GOSSIP_GONE_TIMEOUT: Duration = Duration::from_secs(45);
const GOSSIP_POLL: Duration = Duration::from_millis(200);

const VALUE: &str = "diynamo-eval-padding-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
const GET_POOL_SIZE: usize = 1_000;
const SEED_BATCH_SIZE: usize = 32;
const SEED_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const PROBE_KEY: &str = "failure-probe";

struct Sample {
    alive: usize,
    dead_count: usize,
    concurrency: usize,
    op: &'static str,
    success: bool,
}

fn five_node_ids() -> [&'static str; NODE_COUNT] {
    ["n1", "n2", "n3", "n4", "n5"]
}

/// Spread dead nodes on the ring: indices [0], [0,2], [0,2,4] for 1/2/3 dead.
fn spread_dead_ids(cluster: &TestCluster, dead_count: usize) -> Vec<String> {
    let ring = CoordinatorRing::from_roster(&cluster.roster, cluster.n, cluster.vnodes).unwrap();
    let order: Vec<String> = ring
        .ring_order_for_key(PROBE_KEY.as_bytes())
        .unwrap()
        .into_iter()
        .map(|m| m.id.clone())
        .collect();
    assert_eq!(order.len(), NODE_COUNT);

    let indices: &[usize] = match dead_count {
        1 => &[0],
        2 => &[0, 2],
        3 => &[0, 2, 4],
        n => panic!("unsupported dead_count {n}"),
    };
    indices.iter().map(|&i| order[i].clone()).collect()
}

fn live_base_url(cluster: &TestCluster, dead_ids: &HashSet<&str>) -> String {
    cluster
        .nodes
        .iter()
        .find(|n| !dead_ids.contains(n.id.as_str()))
        .expect("at least one live node")
        .http_url
        .clone()
}

async fn wait_all_dead_gone(cluster: &TestCluster, observer_id: &str, dead_ids: &[String]) {
    let observer_id = observer_id.to_string();
    let dead_ids = dead_ids.to_vec();
    poll_until(GOSSIP_GONE_TIMEOUT, GOSSIP_POLL, || {
        let cluster = cluster;
        let observer_id = observer_id.clone();
        let dead_ids = dead_ids.clone();
        async move {
            let observer = cluster.node(&observer_id);
            for dead_id in &dead_ids {
                if observer.peer_sees_node(dead_id).await {
                    return false;
                }
            }
            true
        }
    })
    .await
    .expect("observer should stop seeing all dead nodes");
}

async fn seed_get_pool(base_url: &str) {
    let seed_client = KvClient::new(base_url)
        .expect("seed client")
        .with_request_timeout(SEED_REQUEST_TIMEOUT);
    for batch_start in (0..GET_POOL_SIZE).step_by(SEED_BATCH_SIZE) {
        let batch_end = (batch_start + SEED_BATCH_SIZE).min(GET_POOL_SIZE);
        let mut seed = tokio::task::JoinSet::new();
        for i in batch_start..batch_end {
            let client = seed_client.clone();
            seed.spawn(async move {
                client
                    .put(&format!("get-pool-{i}"), VALUE)
                    .await
                    .expect("seed put");
            });
        }
        while seed.join_next().await.is_some() {}
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
}

async fn drive_load(
    base_url: String,
    op: &'static str,
    concurrency: usize,
    key_pool: Arc<Vec<String>>,
    warmup: Duration,
    measure: Duration,
) -> Vec<bool> {
    let recording = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let mut tasks = tokio::task::JoinSet::new();

    for worker_idx in 0..concurrency {
        let base_url = base_url.clone();
        let key_pool = Arc::clone(&key_pool);
        let recording = Arc::clone(&recording);
        let stop = Arc::clone(&stop);

        tasks.spawn(async move {
            let client = KvClient::new(&base_url).expect("build KvClient");
            let mut outcomes: Vec<bool> = Vec::new();
            let mut counter: u64 = 0;
            let mut err_count: u32 = 0;
            const MAX_ERRORS_PRINTED: u32 = 3;

            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }

                let key = if op == "put" {
                    format!("eval-f{worker_idx}-{counter}")
                } else {
                    let idx = (counter as usize) % key_pool.len().max(1);
                    key_pool[idx].clone()
                };
                counter += 1;

                let result = if op == "put" {
                    client.put(&key, VALUE).await.map(|_| ())
                } else {
                    client.get(&key).await.map(|_| ())
                };

                if let Err(ref e) = result {
                    if err_count < MAX_ERRORS_PRINTED {
                        eprintln!("[{op} c={concurrency}] {e:#}");
                        err_count += 1;
                    }
                }

                if recording.load(Ordering::Relaxed) {
                    outcomes.push(result.is_ok());
                }
            }

            outcomes
        });
    }

    tokio::time::sleep(warmup).await;
    recording.store(true, Ordering::Relaxed);
    tokio::time::sleep(measure).await;
    stop.store(true, Ordering::Relaxed);

    let mut all = Vec::new();
    while let Some(Ok(worker_outcomes)) = tasks.join_next().await {
        all.extend(worker_outcomes);
    }
    all
}

#[tokio::test(flavor = "multi_thread", worker_threads = 48)]
#[serial]
async fn experiment_2_write_read_success_under_failure() {
    println!("\n┌─ Experiment 2: Success Under Failure ─────────────────────────────────┐");
    println!("│  Cluster : 5 nodes, N=3, W=2, R=2, vnodes=3                          │");
    println!("│  Alive   : {:?}  (skip 5-alive healthy)                              │", ALIVE_COUNTS);
    println!("│  Concurrency: {:?}                                                    │", CONCURRENCY_LEVELS);
    println!("└────────────────────────────────────────────────────────────────────────┘");

    let mut all_samples: Vec<Sample> = Vec::new();

    for &alive in ALIVE_COUNTS {
        let dead_count = NODE_COUNT - alive;
        let cluster = TestCluster::spawn_with_vnodes(&five_node_ids(), 3, 2, 2, 3)
            .await
            .expect("spawn cluster");

        let dead_ids = spread_dead_ids(&cluster, dead_count);
        let dead_set: HashSet<&str> = dead_ids.iter().map(|s| s.as_str()).collect();

        let observer_id = cluster
            .nodes
            .iter()
            .find(|n| !dead_set.contains(n.id.as_str()))
            .expect("observer")
            .id
            .clone();

        let base_url_healthy = cluster.nodes[0].http_url.clone();
        print!("\n── alive={alive}  dead={dead_count}  targets={dead_ids:?}  seeding …");
        let _ = std::io::stdout().flush();
        seed_get_pool(&base_url_healthy).await;
        println!(" done.");

        for dead_id in &dead_ids {
            cluster
                .node(dead_id)
                .kill_node()
                .await
                .expect("kill node");
        }
        wait_all_dead_gone(&cluster, &observer_id, &dead_ids).await;

        let base_url = live_base_url(&cluster, &dead_set);
        let get_pool: Arc<Vec<String>> =
            Arc::new((0..GET_POOL_SIZE).map(|i| format!("get-pool-{i}")).collect());

        for &concurrency in CONCURRENCY_LEVELS {
            for &op in &["put", "get"] {
                print!("  alive={alive}  c={concurrency:2}  {op} …");
                let _ = std::io::stdout().flush();
                let key_pool = if op == "get" {
                    Arc::clone(&get_pool)
                } else {
                    Arc::new(vec![])
                };
                let outcomes = drive_load(
                    base_url.clone(),
                    op,
                    concurrency,
                    key_pool,
                    WARMUP,
                    MEASURE,
                )
                .await;
                let total = outcomes.len();
                let ok = outcomes.iter().filter(|&&s| s).count();
                let pct = 100.0 * ok as f64 / total.max(1) as f64;
                println!("  {pct:.1}%  ({ok}/{total})");

                for success in outcomes {
                    all_samples.push(Sample {
                        alive,
                        dead_count,
                        concurrency,
                        op,
                        success,
                    });
                }
            }
        }

        cluster.shutdown_all().await;
    }

    let samples_path = "eval_failure_samples.csv";
    {
        let mut f = File::create(samples_path).expect("create samples CSV");
        writeln!(f, "alive,dead_count,concurrency,op,success").unwrap();
        for s in &all_samples {
            writeln!(
                f,
                "{},{},{},{},{}",
                s.alive,
                s.dead_count,
                s.concurrency,
                s.op,
                s.success as u8
            )
            .unwrap();
        }
    }
    println!("\nSamples → {samples_path}  ({} rows)", all_samples.len());

    let summary_path = "eval_failure_summary.csv";
    let mut sf = File::create(summary_path).expect("create summary CSV");
    writeln!(sf, "alive,concurrency,put_success_pct,get_success_pct").unwrap();

    println!("\n{:>5}  {:>12}  {:>16}  {:>16}", "alive", "concurrency", "put_success%", "get_success%");
    println!("{}", "─".repeat(55));

    for &alive in ALIVE_COUNTS {
        for &concurrency in CONCURRENCY_LEVELS {
            let put_total = all_samples
                .iter()
                .filter(|s| s.alive == alive && s.concurrency == concurrency && s.op == "put")
                .count();
            let put_ok = all_samples
                .iter()
                .filter(|s| {
                    s.alive == alive && s.concurrency == concurrency && s.op == "put" && s.success
                })
                .count();
            let get_total = all_samples
                .iter()
                .filter(|s| s.alive == alive && s.concurrency == concurrency && s.op == "get")
                .count();
            let get_ok = all_samples
                .iter()
                .filter(|s| {
                    s.alive == alive && s.concurrency == concurrency && s.op == "get" && s.success
                })
                .count();

            let put_pct = 100.0 * put_ok as f64 / put_total.max(1) as f64;
            let get_pct = 100.0 * get_ok as f64 / get_total.max(1) as f64;

            println!(
                "{alive:>5}  {concurrency:>12}  {put_pct:>15.1}%  {get_pct:>15.1}%"
            );
            writeln!(sf, "{alive},{concurrency},{put_pct:.2},{get_pct:.2}").unwrap();
        }
        println!();
    }
    println!("Summary → {summary_path}");
}
