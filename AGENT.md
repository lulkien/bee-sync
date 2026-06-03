# bee-sync - Agent Guide

## Project Overview

Fast parallel file transfer over TCP/TLS with chunked concurrency, BLAKE3 integrity, resume support.

**Stack**: Rust 2024 edition, tokio async, clap derive, anyhow errors, rustls TLS, blake3 hashing, indicatif progress.

## Architecture

```
Control Channel (port 19999, TLS optional) — handshake + final status
Data Channels (ports 45000-46000, plain TCP) — parallel chunk transfer
```

1. Client connects control → sends handshake (filename, size, chunk_size, num_chunks, BLAKE3 hash)
2. Server responds with allocated data port list
3. Workers send chunks in parallel over persistent TCP data connections
4. Server writes `.part` files, verifies per-chunk BLAKE3, sends ACKs
5. Server assembles parts → verifies full-file BLAKE3 → sends final status

## Module Tree

```
src/
├── main.rs              Entry, tokio::main, subcommand dispatch
├── cli.rs               clap derive structs (Cli, ServerArgs, ClientArgs)
├── protocol.rs          Frame transport (length-prefixed), handshake/chunk constants
├── file_ops.rs          BLAKE3 calc_hash, file_hash
├── utils.rs             parse_address, parse_chunk_size, download_file
├── client/
│   ├── mod.rs           run_client, handshake, worker orchestration
│   ├── tls.rs           Stream trait, connect_to_server, NoCertVerifier
│   └── worker.rs        WorkerConfig, send_chunk, query_received, worker loop
└── server/
    ├── mod.rs           run_server, accept loop, ACTIVE_RECEIVERS, PORT_POOL
    ├── tls.rs           load_tls_context
    ├── file_receiver.rs FileReceiver (part tracking, assembly)
    └── handler/
        ├── mod.rs       re-exports
        ├── control.rs   handle_control_connection, handshake parsing, orchestration
        └── data.rs      handle_data_connection, chunk processing, ACK
```

## Key Patterns

### CLI
- `clap` derive: `#[derive(Parser)]` structs, `#[derive(Subcommand)]` enum
- All args use docstring comments (these are clap help text)
- `Option<String>` for optional args, validated in code (not `required = true`)
- `conflicts_with` / `requires` for mutual exclusion / conditional args
- Server `--temp-dir` is `Option<String>` (falls back to output_dir); Client `--temp-dir` is `String` with default `/tmp`

### Async
- `#[tokio::main]` entrypoint
- `tokio::spawn` for concurrent handlers
- `Arc<Mutex<T>>` for shared mutable state across tasks
- `AtomicBool` for shutdown signals
- `tokio::net::TcpStream` / `TcpListener` for all I/O

### Errors
- `anyhow::Result<T>` everywhere
- `anyhow::bail!()` for early returns, `anyhow::anyhow!()` for ad-hoc errors
- Top-level handlers match on Result and return exit codes (0/1)
- `log::error!()` before returning errors

### Protocol
- Length-prefixed frames: 4-byte BE u32 + payload
- Hand-wired `to_be_bytes()` / `from_be_bytes()` (no serde - intentionally)
- Constants in `protocol.rs` (MAGIC, ACK_OK, QUERY_MAGIC, etc.)

### Shared State
- `LazyLock<Mutex<HashMap<u16, Arc<Mutex<FileReceiver>>>>>` for global port→receiver registry
- `Semaphore` for port pool limits
- `Arc<Mutex<indicatif::ProgressBar>>` for progress

### Function Style
- ~50 line functions, extract helpers
- Private `async fn` / `fn` within modules
- Decomposition pattern: orchestrator → setup → execute → cleanup

## Rules (from ~/rules/rust.md)

1. Big module → split into submodules
2. Prefer existing crates, don't reinvent
3. **Never edit Cargo.toml directly** — use `cargo add` with latest version
4. Build with `cargo build --release`
5. Avoid `unsafe`
6. Always run `cargo clippy` after build
7. Always run `cargo fmt` after fix clippy

## Build & Run

```bash
cargo build --release
cargo clippy
cargo fmt
# Server
cargo run --release -- server [--address 0.0.0.0:19999] [--output-dir ./received/] [--cert cert.pem --key key.pem]
# Client
cargo run --release -- client --file <path> [--address localhost:19999] [--tls --tls-no-verify]
cargo run --release -- client --url <url> [--temp-dir /tmp] [--address ...]
```

## Observability
- `--verbose` enables `LevelFilter::Debug`
- Log format: `[LEVEL] message`
- Progress bar via indicatif (hidden in debug/verbose mode)
