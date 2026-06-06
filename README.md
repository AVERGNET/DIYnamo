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

## Configuration Reference

Config files are TOML. The `[cluster]` section controls replication and ring behaviour:

```toml
[cluster]
n      = 3   # replication factor (replicas per key)
w      = 2   # write quorum
r      = 2   # read quorum
vnodes = 3   # virtual ring positions per physical node (default: 1)
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
  read_repair.rs
  hint_handoff.rs
  migration.rs
```
