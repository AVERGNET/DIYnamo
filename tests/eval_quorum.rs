//! Experiment 4 — Latency and write success rate while varying W/R (healthy cluster).
//!
//! Run with:
//!   cargo test --features test-utils --test eval_quorum -- --nocapture
//!
//! Sweeps meaningful (W, R) configs where W + R > N on a 9-node cluster with N=5.
//! Healthy cluster only; concurrency=48 to stress quorum waits.
//!
//! Outputs written to the workspace root:
//!   eval_quorum_samples.csv   — one row per recorded operation
//!   eval_quorum_summary.csv   — aggregated stats per (w, r, op)
//!
//! To generate graphs after running:
//!   pip install pandas matplotlib numpy
//!   python eval/plot_quorum.py eval_quorum_samples.csv

use std::fs::File;
use std::io::Write as IoWrite;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serial_test::serial;

use diynamo::client::KvClient;
use diynamo::test_support::TestCluster;

// ---------------------------------------------------------------------------
// Experiment parameters
// ---------------------------------------------------------------------------

const NODE_COUNT: usize = 9;
const N: usize = 5;

/// Meaningful (W, R) pairs where W + R > N — corners and balanced midpoint.
const RW_CONFIGS: &[(usize, usize)] = &[
    (1, 5), // min write quorum, max read quorum
    (3, 3), // balanced (Dynamo-style for N=5)
    (5, 1), // max write quorum, min read quorum
    (5, 5), // strictest: wait for all N replicas on both paths
    (2, 4), // asymmetric: light writes, heavy reads
];

const CONCURRENCY: usize = 48;

const WARMUP: Duration = Duration::from_secs(5);
const MEASURE: Duration = Duration::from_secs(10);

const VALUE: &str = "diynamo-eval-padding-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
const GET_POOL_SIZE: usize = 1_000;
const SEED_BATCH_SIZE: usize = 32;
const SEED_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------

struct Sample {
    w: usize,
    r: usize,
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

struct OpSample {
    op: &'static str,
    latency_us: u64,
    success: bool,
}

async fn drive_load(
    base_url: String,
    op: &'static str,
    concurrency: usize,
    key_pool: Arc<Vec<String>>,
    warmup: Duration,
    measure: Duration,
) -> Vec<OpSample> {
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
            let mut samples: Vec<OpSample> = Vec::new();
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
                            eprintln!("[{op}] (suppressing further errors)");
                        }
                    }
                }

                if recording.load(Ordering::Relaxed) {
                    samples.push(OpSample {
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
    while let Some(Ok(worker_samples)) = tasks.join_next().await {
        all.extend(worker_samples);
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

// ---------------------------------------------------------------------------
// Experiment 4
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 48)]
#[serial]
async fn experiment_4_quorum_latency_rw_sweep() {
    let node_ids: [&str; NODE_COUNT] =
        ["n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"];

    println!("\n┌─ Experiment 4: Latency vs W/R (healthy cluster) ──────────────────────┐");
    println!(
        "│  Cluster : {NODE_COUNT} nodes, N={N}, vnodes=3, concurrency={CONCURRENCY:<20}│"
    );
    println!(
        "│  Sweep   : {} (W,R) configs where W+R>N                               │",
        RW_CONFIGS.len()
    );
    println!(
        "│  Phases  : warmup={}s  measure={}s  per config                         │",
        WARMUP.as_secs(),
        MEASURE.as_secs()
    );
    println!("└────────────────────────────────────────────────────────────────────────┘");

    let mut all_samples: Vec<Sample> = Vec::new();

    for &(w, r) in RW_CONFIGS {
        println!("\n── W={w}  R={r}  (W+R={}) ──────────────────────────────────────────", w + r);

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

        for &op in &["put", "get"] {
            print!("  {op}  warming up …");
            let _ = std::io::stdout().flush();
            let key_pool = if op == "get" {
                Arc::clone(&get_pool)
            } else {
                Arc::new(vec![])
            };
            let results = drive_load(
                base_url.clone(),
                op,
                CONCURRENCY,
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
            for s in results {
                all_samples.push(Sample {
                    w,
                    r,
                    op: s.op,
                    latency_us: s.latency_us,
                    success: s.success,
                });
            }
        }

        cluster.shutdown_all().await;
    }

    // -----------------------------------------------------------------------
    // Write per-sample CSV.
    // -----------------------------------------------------------------------
    let samples_path = "eval_quorum_samples.csv";
    {
        let mut f = File::create(samples_path).expect("create samples CSV");
        writeln!(f, "w,r,op,latency_us,success").unwrap();
        for s in &all_samples {
            writeln!(
                f,
                "{},{},{},{},{}",
                s.w, s.r, s.op, s.latency_us, s.success as u8
            )
            .unwrap();
        }
    }
    println!("\nSamples → {samples_path}  ({} rows)", all_samples.len());

    // -----------------------------------------------------------------------
    // Aggregate stats and write summary CSV.
    // -----------------------------------------------------------------------
    let summary_path = "eval_quorum_summary.csv";
    let mut sf = File::create(summary_path).expect("create summary CSV");
    writeln!(sf, "w,r,op,ops_per_sec,p50_ms,p95_ms,p99_ms,success_pct").unwrap();

    println!("\n{:>3}  {:>3}  {:>3}  {:>9}  {:>7}  {:>7}  {:>7}  {:>9}",
             "W", "R", "op", "ops/sec", "p50 ms", "p95 ms", "p99 ms", "success%");
    println!("{}", "─".repeat(58));

    for &(w, r) in RW_CONFIGS {
        for &op in &["put", "get"] {
            let total: usize = all_samples
                .iter()
                .filter(|s| s.w == w && s.r == r && s.op == op)
                .count();
            let mut latencies: Vec<u64> = all_samples
                .iter()
                .filter(|s| s.w == w && s.r == r && s.op == op && s.success)
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
                "{w:>3}  {r:>3}  {op:>3}  {tput:>9.1}  {p50:>7.2}  {p95:>7.2}  {p99:>7.2}  {spct:>8.1}%"
            );
            writeln!(
                sf,
                "{w},{r},{op},{tput:.2},{p50:.3},{p95:.3},{p99:.3},{spct:.2}"
            )
            .unwrap();
        }
    }
    println!("\nSummary → {summary_path}");
}
