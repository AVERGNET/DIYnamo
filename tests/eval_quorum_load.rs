//! Experiment 2b — Quorum strictness under load (healthy cluster).
//!
//! Compares loose (W=2, R=2) vs strict (W=3, R=3) on a healthy 5-node cluster
//! as client concurrency increases.  Strict quorum should drop success before
//! loose quorum when slow replicas cannot keep up.
//!
//! Run with:
//!   cargo test --features test-utils --test eval_quorum_load -- --nocapture
//!
//! Outputs:
//!   eval_quorum_load_samples.csv
//!   eval_quorum_load_summary.csv
//!
//! Plot:
//!   python eval/plot_quorum_load.py eval_quorum_load_summary.csv

use std::fs::File;
use std::io::Write as IoWrite;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serial_test::serial;

use diynamo::client::KvClient;
use diynamo::test_support::TestCluster;

const N: usize = 3;

/// Loose vs strict quorum on the same healthy cluster.
const RW_CONFIGS: &[(usize, usize)] = &[
    (2, 2), // loose — tolerates one slow preferred replica
    (3, 3), // strict — all N preferred replicas required
];

const CONCURRENCY_LEVELS: &[usize] = &[1, 2, 4, 8, 16, 32, 48, 64, 96, 128];

const WARMUP: Duration = Duration::from_secs(5);
const MEASURE: Duration = Duration::from_secs(10);

const VALUE: &str = "diynamo-eval-padding-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
const GET_POOL_SIZE: usize = 1_000;
const SEED_BATCH_SIZE: usize = 32;
const SEED_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

struct Sample {
    w: usize,
    r: usize,
    concurrency: usize,
    op: &'static str,
    latency_us: u64,
    success: bool,
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0 * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[idx]
}

async fn drive_load(
    base_url: String,
    op: &'static str,
    concurrency: usize,
    key_pool: Arc<Vec<String>>,
    warmup: Duration,
    measure: Duration,
) -> Vec<Sample> {
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
            let mut samples: Vec<Sample> = Vec::new();
            let mut counter: u64 = 0;
            let mut err_count: u32 = 0;
            const MAX_ERRORS_PRINTED: u32 = 5;

            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }

                let key = if op == "put" {
                    format!("eval-p{worker_idx}-{counter}")
                } else {
                    let idx = (counter as usize) % key_pool.len().max(1);
                    key_pool[idx].clone()
                };
                counter += 1;

                let t0 = Instant::now();
                let result = if op == "put" {
                    client.put(&key, VALUE).await.map(|_| ())
                } else {
                    client.get(&key).await.map(|_| ())
                };
                let latency_us = t0.elapsed().as_micros() as u64;

                if let Err(ref e) = result {
                    if err_count < MAX_ERRORS_PRINTED {
                        eprintln!("[{op} c={concurrency} w={worker_idx}] {e:#}");
                        err_count += 1;
                        if err_count == MAX_ERRORS_PRINTED {
                            eprintln!("[{op} c={concurrency}] (suppressing further errors)");
                        }
                    }
                }

                if recording.load(Ordering::Relaxed) {
                    samples.push(Sample {
                        w: 0,
                        r: 0,
                        concurrency,
                        op,
                        latency_us,
                        success: result.is_ok(),
                    });
                }
            }

            samples
        });
    }

    tokio::time::sleep(warmup).await;
    recording.store(true, Ordering::Relaxed);
    tokio::time::sleep(measure).await;
    stop.store(true, Ordering::Relaxed);

    let mut all = Vec::new();
    while let Some(Ok(mut worker_samples)) = tasks.join_next().await {
        all.append(&mut worker_samples);
    }
    all
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
                if let Err(e) = client.put(&format!("get-pool-{i}"), VALUE).await {
                    eprintln!("[seed key={i}] {e:#}");
                }
            });
        }
        while seed.join_next().await.is_some() {}
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 128)]
#[serial]
async fn experiment_2b_quorum_strictness_under_load() {
    let node_ids = ["n1", "n2", "n3", "n4", "n5"];

    println!("\n┌─ Experiment 2b: Quorum Strictness Under Load ──────────────────────────┐");
    println!("│  Cluster : 5 nodes, N={N}, vnodes=3, healthy                          │");
    println!("│  Compare : W=2,R=2 (loose) vs W=3,R=3 (strict)                        │");
    println!(
        "│  Concurrency: {:?}",
        CONCURRENCY_LEVELS
    );
    println!("└────────────────────────────────────────────────────────────────────────┘");

    let mut all_samples: Vec<Sample> = Vec::new();

    for &(w, r) in RW_CONFIGS {
        println!("\n── W={w}  R={r} ──────────────────────────────────────────────────────");

        let cluster = TestCluster::spawn_with_vnodes(&node_ids, N, w, r, 3)
            .await
            .unwrap_or_else(|e| panic!("spawn cluster W={w} R={r}: {e:#}"));

        let base_url = cluster.nodes[0].http_url.clone();

        print!("  seeding {GET_POOL_SIZE} keys …");
        let _ = std::io::stdout().flush();
        seed_get_pool(&base_url).await;
        println!(" done.");

        let get_pool: Arc<Vec<String>> =
            Arc::new((0..GET_POOL_SIZE).map(|i| format!("get-pool-{i}")).collect());

        for &concurrency in CONCURRENCY_LEVELS {
            for &op in &["put", "get"] {
                print!("  W={w} R={r}  c={concurrency:3}  {op} …");
                let _ = std::io::stdout().flush();
                let key_pool = if op == "get" {
                    Arc::clone(&get_pool)
                } else {
                    Arc::new(vec![])
                };
                let mut results = drive_load(
                    base_url.clone(),
                    op,
                    concurrency,
                    key_pool,
                    WARMUP,
                    MEASURE,
                )
                .await;
                let total = results.len();
                let ok = results.iter().filter(|s| s.success).count();
                println!(
                    "  done  ops={total}  success={:.1}%",
                    100.0 * ok as f64 / total.max(1) as f64
                );
                for s in &mut results {
                    s.w = w;
                    s.r = r;
                }
                all_samples.append(&mut results);
            }
        }

        cluster.shutdown_all().await;
    }

    let samples_path = "eval_quorum_load_samples.csv";
    {
        let mut f = File::create(samples_path).expect("create samples CSV");
        writeln!(f, "w,r,concurrency,op,latency_us,success").unwrap();
        for s in &all_samples {
            writeln!(
                f,
                "{},{},{},{},{},{}",
                s.w, s.r, s.concurrency, s.op, s.latency_us, s.success as u8
            )
            .unwrap();
        }
    }
    println!("\nSamples → {samples_path}  ({} rows)", all_samples.len());

    let summary_path = "eval_quorum_load_summary.csv";
    let mut sf = File::create(summary_path).expect("create summary CSV");
    writeln!(
        sf,
        "w,r,concurrency,op,ops_per_sec,p50_ms,p95_ms,p99_ms,success_pct"
    )
    .unwrap();

    println!(
        "\n{:>3} {:>3} {:>12} {:>3} {:>9} {:>7} {:>7} {:>7} {:>9}",
        "W", "R", "concurrency", "op", "ops/sec", "p50", "p95", "p99", "success%"
    );
    println!("{}", "─".repeat(72));

    for &(w, r) in RW_CONFIGS {
        for &op in &["put", "get"] {
            for &concurrency in CONCURRENCY_LEVELS {
                let total: usize = all_samples
                    .iter()
                    .filter(|s| s.w == w && s.r == r && s.op == op && s.concurrency == concurrency)
                    .count();
                let mut latencies: Vec<u64> = all_samples
                    .iter()
                    .filter(|s| {
                        s.w == w
                            && s.r == r
                            && s.op == op
                            && s.concurrency == concurrency
                            && s.success
                    })
                    .map(|s| s.latency_us)
                    .collect();

                if total == 0 {
                    continue;
                }
                latencies.sort_unstable();

                let successes = latencies.len();
                let tput = total as f64 / MEASURE.as_secs_f64();
                let p50 = percentile(&latencies, 50.0) as f64 / 1_000.0;
                let p95 = percentile(&latencies, 95.0) as f64 / 1_000.0;
                let p99 = percentile(&latencies, 99.0) as f64 / 1_000.0;
                let spct = 100.0 * successes as f64 / total as f64;

                println!(
                    "{w:>3} {r:>3} {concurrency:>12} {op:>3} {tput:>9.1} {p50:>7.2} {p95:>7.2} {p99:>7.2} {spct:>8.1}%"
                );
                writeln!(
                    sf,
                    "{w},{r},{concurrency},{op},{tput:.2},{p50:.3},{p95:.3},{p99:.3},{spct:.2}"
                )
                .unwrap();
            }
            println!();
        }
    }
    println!("Summary → {summary_path}");
}
