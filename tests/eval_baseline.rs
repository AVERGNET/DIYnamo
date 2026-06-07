//! Experiment 1 — Baseline throughput and latency in a healthy 5-node cluster.
//!
//! Run with:
//!   cargo test --features test-utils --test eval_baseline -- --nocapture
//!
//! Expected wall time: ~7 minutes
//!   (6 concurrency levels × 2 ops × (5 s warmup + 30 s measure) each)
//!
//! Outputs written to the workspace root:
//!   eval_baseline_samples.csv   — one row per recorded operation
//!   eval_baseline_summary.csv   — aggregated stats per (concurrency, op)
//!
//! To generate graphs after running:
//!   pip install pandas matplotlib
//!   python eval/plot_baseline.py eval_baseline_samples.csv

use std::fs::File;
use std::io::Write as IoWrite;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use diynamo::client::KvClient;
use diynamo::test_support::TestCluster;

// ---------------------------------------------------------------------------
// Experiment parameters
// ---------------------------------------------------------------------------

// const CONCURRENCY_LEVELS: &[usize] = &[1];
const CONCURRENCY_LEVELS: &[usize] = &[1, 2, 4, 8, 16, 32, 48];

/// Discard ops during this window to warm up gossip, connection pools, and JIT.
const WARMUP: Duration = Duration::from_secs(5);

/// Record ops during this window.
const MEASURE: Duration = Duration::from_secs(10);

/// Fixed payload written on every PUT (~64 bytes).
const VALUE: &str = "diynamo-eval-padding-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

/// Keys pre-seeded for the GET pool so every GET hits a real value.
const GET_POOL_SIZE: usize = 1_000;

/// Seed at most this many keys concurrently.
const SEED_BATCH_SIZE: usize = 32;

/// Per-request timeout while seeding (quorum writes can be slow under load).
const SEED_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------

struct Sample {
    concurrency: usize,
    op: &'static str,
    latency_us: u64,
    success: bool,
}

/// Nearest-rank percentile over a *sorted* slice of microsecond latencies.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((p / 100.0 * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Spawn `concurrency` workers, each looping PUT or GET requests.
///
/// Workers run for `warmup + measure`; samples are recorded only during the
/// measurement window.  Returns all recorded samples.
async fn drive_load(
    base_url: String,
    op: &'static str,
    concurrency: usize,
    // For GET: pool of pre-seeded keys to read; ignored for PUT.
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

                // PUT: unique keys per worker to distribute load across the ring.
                // GET: round-robin over the pre-seeded pool.
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
                    // 404 (key absent despite seeding) is treated as failure.
                    client.get(&key).await.map(|_| ())
                };
                let latency_us = t0.elapsed().as_micros() as u64;

                if let Err(ref e) = result {
                    if err_count < MAX_ERRORS_PRINTED {
                        eprintln!("[{op} c={concurrency} w={worker_idx}] {e:#}");
                        err_count += 1;
                        if err_count == MAX_ERRORS_PRINTED {
                            eprintln!("[{op} c={concurrency} w={worker_idx}] (suppressing further errors)");
                        }
                    }
                }

                if recording.load(Ordering::Relaxed) {
                    samples.push(Sample {
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

    // Warmup window — workers run but samples are discarded.
    tokio::time::sleep(warmup).await;
    recording.store(true, Ordering::Relaxed);

    // Measurement window.
    tokio::time::sleep(measure).await;
    stop.store(true, Ordering::Relaxed);

    // Drain workers.  Each may still be in an in-flight HTTP call (≤ 1 s timeout),
    // so this blocks for at most ~1 s after setting stop.
    let mut all = Vec::new();
    while let Some(Ok(worker_samples)) = tasks.join_next().await {
        all.extend(worker_samples);
    }
    all
}

// ---------------------------------------------------------------------------
// Experiment 1
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 48)]
async fn experiment_1_baseline_throughput_latency() {
    // -----------------------------------------------------------------------
    // 1. Cluster: 5 nodes, N=3, W=2, R=2, 3 virtual nodes per physical node.
    // -----------------------------------------------------------------------
    let node_ids = ["n1", "n2", "n3", "n4", "n5"];
    let cluster = TestCluster::spawn_with_vnodes(&node_ids, 3, 2, 2, 3)
        .await
        .expect("spawn 5-node cluster");

    let base_url = cluster.nodes[0].http_url.clone();

    println!("\n┌─ Experiment 1: Baseline Throughput & Latency ─────────────────────────┐");
    println!("│  Cluster : 5 nodes, N=3, W=2, R=2, vnodes=3                          │");
    println!("│  Base URL: {base_url:<56}  │");
    println!(
        "│  Phases  : warmup={}s  measure={}s  per concurrency level            │",
        WARMUP.as_secs(),
        MEASURE.as_secs()
    );
    println!("└────────────────────────────────────────────────────────────────────────┘");

    let mut all_samples: Vec<Sample> = Vec::new();

    // -----------------------------------------------------------------------
    // 2. PUT phase.
    // -----------------------------------------------------------------------
    println!("\n── PUT phase ───────────────────────────────────────────────────────────");
    for &concurrency in CONCURRENCY_LEVELS {
        print!("  concurrency={concurrency:2}  warming up …");
        let _ = std::io::stdout().flush();
        let samples = drive_load(
            base_url.clone(),
            "put",
            concurrency,
            Arc::new(vec![]),
            WARMUP,
            MEASURE,
        )
        .await;
        let total = samples.len();
        let ok = samples.iter().filter(|s| s.success).count();
        println!(
            "  done  ops={}  success={:.1}%",
            total,
            100.0 * ok as f64 / total.max(1) as f64
        );
        all_samples.extend(samples);
    }

    // -----------------------------------------------------------------------
    // 3. Seed GET key pool: 1 000 keys.
    // -----------------------------------------------------------------------
    println!("\n── Seeding {} keys for GET pool ─────────────────────────────────────", GET_POOL_SIZE);
    {
        let seed_client = KvClient::new(&base_url)
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
    }
    // Allow quorum propagation and any background read-repair to settle.
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("  done.");

    let get_pool: Arc<Vec<String>> =
        Arc::new((0..GET_POOL_SIZE).map(|i| format!("get-pool-{i}")).collect());

    // -----------------------------------------------------------------------
    // 4. GET phase.
    // -----------------------------------------------------------------------
    println!("\n── GET phase ───────────────────────────────────────────────────────────");
    for &concurrency in CONCURRENCY_LEVELS {
        print!("  concurrency={concurrency:2}  warming up …");
        let _ = std::io::stdout().flush();
        let samples = drive_load(
            base_url.clone(),
            "get",
            concurrency,
            Arc::clone(&get_pool),
            WARMUP,
            MEASURE,
        )
        .await;
        let total = samples.len();
        let ok = samples.iter().filter(|s| s.success).count();
        println!(
            "  done  ops={}  success={:.1}%",
            total,
            100.0 * ok as f64 / total.max(1) as f64
        );
        all_samples.extend(samples);
    }

    // -----------------------------------------------------------------------
    // 5. Write per-sample CSV.
    // -----------------------------------------------------------------------
    let samples_path = "eval_baseline_samples.csv";
    {
        let mut f = File::create(samples_path).expect("create samples CSV");
        writeln!(f, "concurrency,op,latency_us,success").unwrap();
        for s in &all_samples {
            writeln!(
                f,
                "{},{},{},{}",
                s.concurrency,
                s.op,
                s.latency_us,
                s.success as u8
            )
            .unwrap();
        }
    }
    println!("\nSamples → {samples_path}  ({} rows)", all_samples.len());

    // -----------------------------------------------------------------------
    // 6. Aggregate stats and write summary CSV.
    // -----------------------------------------------------------------------
    let summary_path = "eval_baseline_summary.csv";
    let mut sf = File::create(summary_path).expect("create summary CSV");
    writeln!(sf, "concurrency,op,ops_per_sec,p50_ms,p95_ms,p99_ms,success_pct").unwrap();

    println!("\n{:>12}  {:>3}  {:>9}  {:>7}  {:>7}  {:>7}  {:>9}",
             "concurrency", "op", "ops/sec", "p50 ms", "p95 ms", "p99 ms", "success%");
    println!("{}", "─".repeat(62));

    for &op in &["put", "get"] {
        for &concurrency in CONCURRENCY_LEVELS {
            let total: usize = all_samples
                .iter()
                .filter(|s| s.op == op && s.concurrency == concurrency)
                .count();
            let mut latencies: Vec<u64> = all_samples
                .iter()
                .filter(|s| s.op == op && s.concurrency == concurrency && s.success)
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
                "{concurrency:>12}  {op:>3}  {tput:>9.1}  {p50:>7.2}  {p95:>7.2}  {p99:>7.2}  {spct:>8.1}%"
            );
            writeln!(
                sf,
                "{concurrency},{op},{tput:.2},{p50:.3},{p95:.3},{p99:.3},{spct:.2}"
            )
            .unwrap();
        }
        println!();
    }
    println!("Summary → {summary_path}");

    cluster.shutdown_all().await;
}
