//! Experiment 1 — Baseline throughput and latency.
//!
//! Spawns 5 real server processes (each with its own tokio runtime and RocksDB
//! instance), then drives PUT/GET load at varying concurrency levels.
//!
//! Usage:
//!   cargo build                          # build server + eval_baseline first
//!   cargo run --bin eval_baseline        # run the eval
//!
//! For accurate numbers use a release build:
//!   cargo build --release
//!   cargo run --release --bin eval_baseline
//!
//! Outputs written to the current directory:
//!   eval_baseline_samples.csv   — one row per recorded operation
//!   eval_baseline_summary.csv   — aggregated stats per (concurrency, op)

use std::fs::File;
use std::io::Write as IoWrite;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::process::{Child, Command};

use diynamo::client::KvClient;

// ---------------------------------------------------------------------------
// Cluster parameters
// ---------------------------------------------------------------------------

const N_NODES: usize = 5;
const N: usize = 3;
const W: usize = 2;
const R: usize = 2;
const VNODES: usize = 3;

/// Base HTTP port; nodes use HTTP_BASE_PORT + i.
const HTTP_BASE_PORT: u16 = 18_081;
/// Base gossip port; nodes use GOSSIP_BASE_PORT + i.
const GOSSIP_BASE_PORT: u16 = 17_946;

// ---------------------------------------------------------------------------
// Eval parameters
// ---------------------------------------------------------------------------

const CONCURRENCY_LEVELS: &[usize] = &[1, 2, 4, 8, 16, 32];
const WARMUP: Duration = Duration::from_secs(5);
const MEASURE: Duration = Duration::from_secs(30);
const VALUE: &str = "diynamo-eval-padding-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
const GET_POOL_SIZE: usize = 1_000;

// ---------------------------------------------------------------------------
// Process management
// ---------------------------------------------------------------------------

struct ServerProcess {
    child: Child,
    #[allow(dead_code)]
    node_id: String,
}

impl ServerProcess {
    async fn kill(&mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        // Best-effort signal; the async kill() call in main is more reliable.
        let _ = self.child.start_kill();
    }
}

// ---------------------------------------------------------------------------
// Config generation
// ---------------------------------------------------------------------------

fn http_port(i: usize) -> u16 {
    HTTP_BASE_PORT + i as u16
}

fn gossip_port(i: usize) -> u16 {
    GOSSIP_BASE_PORT + i as u16
}

fn node_id(i: usize) -> String {
    format!("n{}", i + 1)
}

fn data_dir(i: usize) -> PathBuf {
    std::env::temp_dir().join(format!("diynamo-eval-n{}", i + 1))
}

fn generate_toml(node_idx: usize) -> String {
    let id = node_id(node_idx);
    let hp = http_port(node_idx);
    let gp = gossip_port(node_idx);
    let dd = data_dir(node_idx);

    let join_list = if node_idx == 0 {
        "[]".to_string()
    } else {
        format!("[\"127.0.0.1:{}\"]", gossip_port(0))
    };

    let mut members = String::new();
    for i in 0..N_NODES {
        members.push_str(&format!(
            "\n[[cluster.members]]\nid = \"{}\"\ngossip_addr = \"127.0.0.1:{}\"\nforward_port = {}\n",
            node_id(i),
            gossip_port(i),
            http_port(i)
        ));
    }

    format!(
        "[node]\nid = \"{id}\"\nhttp_port = {hp}\ngossip_bind = \"127.0.0.1:{gp}\"\ndata_dir = \"{dd}\"\n\n\
         [cluster]\njoin = {join_list}\nn = {N}\nw = {W}\nr = {R}\nvnodes = {VNODES}\n{members}",
        dd = dd.display()
    )
}

// ---------------------------------------------------------------------------
// Cluster lifecycle
// ---------------------------------------------------------------------------

async fn spawn_cluster(server_bin: &Path) -> Result<Vec<ServerProcess>> {
    let cfg_dir = std::env::temp_dir().join("diynamo-eval-configs");
    std::fs::create_dir_all(&cfg_dir).context("create eval config dir")?;

    let mut processes = Vec::new();
    for i in 0..N_NODES {
        let cfg_path = cfg_dir.join(format!("node{}.toml", i + 1));
        std::fs::write(&cfg_path, generate_toml(i))
            .with_context(|| format!("write config for node {}", i + 1))?;

        let child = Command::new(server_bin)
            .arg("-c").arg(&cfg_path)
            .arg("--wipe-data")
            .kill_on_drop(true)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .with_context(|| format!("spawn server node {}", i + 1))?;

        processes.push(ServerProcess { child, node_id: node_id(i) });

        // Let the seed node start before the others try to join.
        if i == 0 {
            tokio::time::sleep(Duration::from_millis(600)).await;
        }
    }
    Ok(processes)
}

/// Poll every node's HTTP port until it responds (any HTTP status = up).
async fn wait_for_ready(timeout: Duration) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(300))
        .build()?;

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut all_up = true;
        for i in 0..N_NODES {
            let url = format!("http://127.0.0.1:{}/kv/__probe__", http_port(i));
            if client.get(&url).send().await.is_err() {
                all_up = false;
                break;
            }
        }
        if all_up {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("nodes did not become ready within {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ---------------------------------------------------------------------------
// Load driver
// ---------------------------------------------------------------------------

struct Sample {
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
    url: String,
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
        let url = url.clone();
        let key_pool = Arc::clone(&key_pool);
        let recording = Arc::clone(&recording);
        let stop = Arc::clone(&stop);

        tasks.spawn(async move {
            let client = KvClient::new(&url).expect("build KvClient");
            let mut samples: Vec<Sample> = Vec::new();
            let mut counter: u64 = 0;
            let mut err_count: u32 = 0;
            const MAX_ERRORS: u32 = 5;

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
                    if err_count < MAX_ERRORS {
                        eprintln!("[{op} c={concurrency} w={worker_idx}] {e:#}");
                        err_count += 1;
                        if err_count == MAX_ERRORS {
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Locate sibling server binary (same target dir as this binary).
    let exe = std::env::current_exe().context("can't resolve current exe path")?;
    let server_bin = exe
        .parent()
        .context("can't find parent dir of eval_baseline binary")?
        .join("server");

    if !server_bin.exists() {
        anyhow::bail!(
            "server binary not found at {}.\nRun `cargo build` (or `cargo build --release`) first.",
            server_bin.display()
        );
    }

    println!("\n┌─ Experiment 1: Baseline Throughput & Latency ─────────────────────────┐");
    println!("│  Cluster : {N_NODES} nodes  N={N}  W={W}  R={R}  vnodes={VNODES}                         │");
    println!("│  Binary  : {:<57}│", server_bin.display());
    println!(
        "│  Phases  : warmup={}s  measure={}s  per concurrency level             │",
        WARMUP.as_secs(),
        MEASURE.as_secs()
    );
    println!("│  HTTP    : ports {HTTP_BASE_PORT}–{}                                          │", HTTP_BASE_PORT + N_NODES as u16 - 1);
    println!("└────────────────────────────────────────────────────────────────────────┘");

    // -----------------------------------------------------------------------
    // Spawn cluster
    // -----------------------------------------------------------------------
    println!("\nSpawning {N_NODES} server processes …");
    let mut processes = spawn_cluster(&server_bin).await?;

    print!("Waiting for nodes to come up … ");
    let _ = std::io::stdout().flush();
    wait_for_ready(Duration::from_secs(20))
        .await
        .context("cluster failed to start")?;

    // Extra pause for gossip convergence across all 5 nodes.
    tokio::time::sleep(Duration::from_secs(4)).await;
    println!("ready.");

    let coord_url = format!("http://127.0.0.1:{}", http_port(0));
    let all_urls: Vec<String> = (0..N_NODES)
        .map(|i| format!("http://127.0.0.1:{}", http_port(i)))
        .collect();

    let mut all_samples: Vec<Sample> = Vec::new();

    // -----------------------------------------------------------------------
    // PUT phase
    // -----------------------------------------------------------------------
    println!("\n── PUT phase ───────────────────────────────────────────────────────────");
    for &concurrency in CONCURRENCY_LEVELS {
        print!("  concurrency={concurrency:2}  warming up …");
        let _ = std::io::stdout().flush();
        let samples = drive_load(
            coord_url.clone(),
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
            "  done  ops={total}  success={:.1}%",
            100.0 * ok as f64 / total.max(1) as f64
        );
        all_samples.extend(samples);
    }

    // -----------------------------------------------------------------------
    // Seed GET pool — spread across all nodes for even ring coverage.
    // -----------------------------------------------------------------------
    println!("\n── Seeding {GET_POOL_SIZE} keys for GET pool ──────────────────────────────────");
    {
        let mut seed = tokio::task::JoinSet::new();
        for i in 0..GET_POOL_SIZE {
            let url = all_urls[i % all_urls.len()].clone();
            seed.spawn(async move {
                let client = KvClient::new(&url).expect("seed client");
                if let Err(e) = client.put(&format!("get-pool-{i}"), VALUE).await {
                    eprintln!("[seed key={i}] {e:#}");
                }
            });
        }
        while seed.join_next().await.is_some() {}
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("  done.");

    let get_pool: Arc<Vec<String>> =
        Arc::new((0..GET_POOL_SIZE).map(|i| format!("get-pool-{i}")).collect());

    // -----------------------------------------------------------------------
    // GET phase
    // -----------------------------------------------------------------------
    println!("\n── GET phase ───────────────────────────────────────────────────────────");
    for &concurrency in CONCURRENCY_LEVELS {
        print!("  concurrency={concurrency:2}  warming up …");
        let _ = std::io::stdout().flush();
        let samples = drive_load(
            coord_url.clone(),
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
            "  done  ops={total}  success={:.1}%",
            100.0 * ok as f64 / total.max(1) as f64
        );
        all_samples.extend(samples);
    }

    // -----------------------------------------------------------------------
    // Write per-sample CSV
    // -----------------------------------------------------------------------
    let samples_path = "eval_baseline_samples.csv";
    {
        let mut f = File::create(samples_path).context("create samples CSV")?;
        writeln!(f, "concurrency,op,latency_us,success")?;
        for s in &all_samples {
            writeln!(f, "{},{},{},{}", s.concurrency, s.op, s.latency_us, s.success as u8)?;
        }
    }
    println!("\nSamples → {samples_path}  ({} rows)", all_samples.len());

    // -----------------------------------------------------------------------
    // Compute and print summary
    // -----------------------------------------------------------------------
    let summary_path = "eval_baseline_summary.csv";
    let mut sf = File::create(summary_path).context("create summary CSV")?;
    writeln!(sf, "concurrency,op,ops_per_sec,p50_ms,p95_ms,p99_ms,success_pct")?;

    println!(
        "\n{:>12}  {:>3}  {:>9}  {:>7}  {:>7}  {:>7}  {:>9}",
        "concurrency", "op", "ops/sec", "p50 ms", "p95 ms", "p99 ms", "success%"
    );
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
            )?;
        }
        println!();
    }
    println!("Summary → {summary_path}");

    // -----------------------------------------------------------------------
    // Shut down cluster
    // -----------------------------------------------------------------------
    println!("\nShutting down cluster …");
    for p in &mut processes {
        p.kill().await;
    }
    println!("done.");

    Ok(())
}
