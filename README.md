# DIYnamo

A Dynamo-inspired distributed key-value store built for CSE 223b.  
Implements sloppy quorum writes, quorum reads with read repair, hinted handoff, and UUID-based node reconciliation on top of RocksDB and a consistent hash ring with virtual nodes.

## Prerequisites

- Rust toolchain (stable, 2024 edition) — install via [rustup](https://rustup.rs)
- No other runtime dependencies; RocksDB is compiled from source by the `rocksdb` crate

## Build

```bash
cargo build --release
```

Binaries land in `target/release/`: `server`, `cli`, `gossip`.

## Running a Local 3-Node Cluster

Three config files are provided under `config/`. Each node must be started in its own terminal.

**Terminal 1 — seed node:**
```bash
cargo run --bin server -- -c config/node1.toml
```

**Terminal 2:**
```bash
cargo run --bin server -- -c config/node2.toml
```

**Terminal 3:**
```bash
cargo run --bin server -- -c config/node3.toml
```

Nodes gossip on `127.0.0.1:7946-7948` and serve HTTP on ports `8081-8083`.  
Wait for all three to print their gossip live-set before issuing requests.

### Wiping data between runs

Pass `--wipe-data` to delete the RocksDB directory on startup:

```bash
cargo run --bin server -- -c config/node1.toml --wipe-data
```

## CLI Client

Connect to any node and issue `put`/`get` commands:

```bash
cargo run --bin cli -- --url http://127.0.0.1:8081
```

```
diynamo> put mykey hello world
put ok
diynamo> get mykey
hello world
diynamo> exit
```

## HTTP API

All nodes expose the same public API:

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/kv/<key>` | Write a value (body: plain text) |
| `GET` | `/kv/<key>` | Read a value |

```bash
curl -X PUT http://127.0.0.1:8081/kv/foo -d "bar"
curl http://127.0.0.1:8081/kv/foo
```

## Node Configuration

Each node is configured with a TOML file. The full structure is:

```toml
[node]
id           = "n1"              # unique node identifier (string, used in the hash ring and hint store)
http_port    = 8081              # port the HTTP API listens on
gossip_bind  = "127.0.0.1:7946" # address:port the gossip protocol binds to
data_dir     = "./data/n1"       # directory for RocksDB data and hint stores (created automatically)

[cluster]
join  = []                       # gossip addresses to contact on startup for cluster join
                                 # leave empty [] on the seed node; non-seeds list the seed's gossip_bind
seeds = ["http://127.0.0.1:8081"] # HTTP addresses of seed nodes (used for client request routing)
n     = 3                        # replication factor — how many nodes each key is replicated to
w     = 2                        # write quorum — minimum acknowledgements for a write to succeed
r     = 2                        # read quorum — minimum responses required for a read to succeed
vnodes = 3                       # virtual ring positions per physical node (higher = more even distribution)

# Every physical node in the cluster must be listed here in EVERY config file.
# This is the static membership roster used to build the consistent hash ring.
[[cluster.members]]
id           = "n1"
gossip_addr  = "127.0.0.1:7946"  # must match that node's gossip_bind
forward_port = 8081               # must match that node's http_port

[[cluster.members]]
id           = "n2"
gossip_addr  = "127.0.0.1:7947"
forward_port = 8082
```

### Adding a new node

To add a 4th node to the cluster:

1. **Create `config/node4.toml`** — pick a unique `id`, unused ports for `http_port` and `gossip_bind`, and a new `data_dir`. Set `join` to the seed node's `gossip_bind`. Copy the full `[[cluster.members]]` list from an existing config and append the new node's entry.

2. **Update every other config file** — add the new node as a `[[cluster.members]]` entry in `node1.toml`, `node2.toml`, and `node3.toml`. The ring is built from this static list at startup, so all nodes must agree on the full roster.

Example entry to add to each existing config:
```toml
[[cluster.members]]
id           = "n4"
gossip_addr  = "127.0.0.1:7949"
forward_port = 8084
```

Example `config/node4.toml`:
```toml
[node]
id          = "n4"
http_port   = 8084
gossip_bind = "127.0.0.1:7949"
data_dir    = "./data/n4"

[cluster]
join  = ["127.0.0.1:7946"]
seeds = ["http://127.0.0.1:8081"]
n     = 3
w     = 2
r     = 2

[[cluster.members]]
id           = "n1"
gossip_addr  = "127.0.0.1:7946"
forward_port = 8081

[[cluster.members]]
id           = "n2"
gossip_addr  = "127.0.0.1:7947"
forward_port = 8082

[[cluster.members]]
id           = "n3"
gossip_addr  = "127.0.0.1:7948"
forward_port = 8083

[[cluster.members]]
id           = "n4"
gossip_addr  = "127.0.0.1:7949"
forward_port = 8084
```

The `--vnodes` CLI flag overrides the config value at startup:

```bash
cargo run --bin server -- -c config/node1.toml --vnodes 5
```

## Running Tests

Integration tests spin up in-process clusters and require no external dependencies:

```bash
# All integration tests (read repair, hinted handoff, migration)
cargo test --features test-utils

# A single suite
cargo test --features test-utils --test read_repair
cargo test --features test-utils --test hint_handoff
cargo test --features test-utils --test migration
```

> Tests are serialised (`serial_test`) because they bind real ports — run them sequentially, not with `-j`.

## Evaluation Experiments

Each eval test spawns a full in-process cluster, runs a workload, and writes results as CSV files to the **workspace root**. Run them with `--nocapture` to stream progress. Wall time ranges from a few minutes (experiments 1 and 4) to ~15 minutes (experiments 3a/3b with large key counts).

> **Important:** run eval tests sequentially, not in parallel. They bind real ports and will conflict if run concurrently.

### What each experiment measures

| # | Test file | What it measures |
|---|-----------|------------------|
| 1 | `eval_baseline` | Throughput and p50/p99 latency on a healthy 5-node cluster (`N=3, W=2, R=2`) across concurrency levels |
| 2 | `eval_failure` | PUT and GET success rate as nodes are killed (4 → 3 → 2 alive) at varying concurrency |
| 3a | `eval_recovery` | Time for hinted handoff to complete after a node recovers with data intact, vs. number of parked hints |
| 3b | `eval_recovery` | Time for key-range reconciliation to complete after a node restarts with data loss, vs. key count |
| 4 | `eval_quorum` | PUT/GET latency across all meaningful `(W, R)` pairs where `W + R > N`, on a 9-node cluster |

### Running the experiments

```bash
# Run all eval experiments (sequentially)
cargo test --features test-utils --test eval_baseline -- --nocapture
cargo test --features test-utils --test eval_failure -- --nocapture
cargo test --features test-utils --test eval_recovery -- --nocapture
cargo test --features test-utils --test eval_quorum -- --nocapture
```

CSV files are written to the workspace root after each run:

| Experiment | Output files |
|------------|-------------|
| 1 | `eval_baseline_samples.csv`, `eval_baseline_summary.csv` |
| 2 | `eval_failure_samples.csv`, `eval_failure_summary.csv` |
| 3a / 3b | `eval_recovery_3a.csv`, `eval_recovery_3b.csv` |
| 4 | `eval_quorum_samples.csv`, `eval_quorum_summary.csv` |


## Project Layout

```
src/
  bin/
    server.rs       HTTP + gossip server entry point
    cli.rs          Interactive CLI client
  cluster/
    ring.rs         Consistent hash ring (wraps hashring_coordinator)
    membership.rs   Gossip-based live-set tracking
  coordinator/
    mod.rs          ReplicatedStore — quorum put/get, read repair
    handoff.rs      Background hinted handoff + UUID reconciliation
  config/
    server.rs       Config file + CLI arg parsing
  store/
    rocksdb_store.rs  Local RocksDB wrapper
    hints.rs          Hint store (pending handoff entries)
  test_support/     In-process multi-node harness for integration tests
config/
  node1.toml        Seed node config
  node2.toml
  node3.toml
tests/
  eval_baseline.rs   Experiment 1
  eval_failure.rs    Experiment 2
  eval_recovery.rs   Experiments 3a / 3b
  eval_quorum.rs     Experiment 4
  read_repair.rs
  hint_handoff.rs
  migration.rs
eval/
  plot_baseline.py
  plot_failure.py
  plot_recovery.py
  plot_quorum.py
```
