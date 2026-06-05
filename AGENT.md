# bee-sync - Agent instruction

## Project Overview

Fast parallel file transfer over TCP/TLS with chunked concurrency, BLAKE3 integrity,
verified resume support, and a native GUI server dashboard.

**Workspace**: 3 crates under `bee-sync/`
- `bee-sync-core` — library: protocol, server, client, CLI types, utilities
- `bee-sync-cli`  — binary: command-line client and server (clap)
- `bee-sync-gui`  — binary: Slint native GUI server dashboard

**Stack**: Rust 2024 edition, tokio async, clap derive, anyhow errors, rustls TLS,
blake3 hashing, indicatif progress, Slint GUI.

**Defaults**: 2 MiB chunks, 25 parallel workers, 3 retries per chunk.

## Technology Stack

| Concern | Crate | Notes |
| ------- | ----- | ----- |
| Error handling | `anyhow` | Use `anyhow::Result<T>` everywhere. Use `anyhow::bail!()` for early returns, `anyhow::anyhow!()` for ad-hoc errors. |
| Async runtime | `tokio` (full features) | All I/O is async. Prefer `tokio::spawn` for concurrent tasks. Use `tokio::net::TcpStream` / `TcpListener`. |
| CLI parsing | `clap` (derive) | In `bee-sync-core/src/cli.rs`. `requires` for dependent flags, `conflicts_with` for mutual exclusion. |
| Logging | `fern` + `log` | Use `log::info!()`, `log::error!()`, `log::debug!()`. Logger initialized in each binary's main. `--verbose` enables debug level. |
| Progress bars | `indicatif` | Use `ProgressBar` for transfer progress. Wrap in `Arc<Mutex<ProgressBar>>` for shared access across workers. |
| TLS | `rustls` 0.23 + `tokio-rustls` + `webpki-roots` | Server uses `TlsAcceptor`, client uses `TlsConnector`. Custom `NoCertVerifier` for `--tls-no-verify`. |
| Hashing | `blake3` | BLAKE3 for chunk and full-file integrity verification. Wrappers in `bee-sync-core/src/file_ops.rs`. |
| Signal handling | `ctrlc` | Graceful shutdown for both client and server via `AtomicBool` flag. |
| HTTP download | `reqwest` + `futures-util` | Client `--url` flag downloads via `reqwest::get` with streaming body. |
| GUI | `slint` 1.x | Native rendering (no webview). `.slint` files in `bee-sync-gui/ui/`, compiled at build time via `slint-build`. |

## Architecture

```
          Control Channel (port 19999, TLS optional)
Client  ────────────────────────────────────────────────>  Server
        <────────────────────────────────────────────────
        Handshake: filename, size, chunk_size, num_chunks, BLAKE3 hash
        Response:  status + allocated data port list

          Data Channels (ports 45000-46000, plain TCP)
Client  ════════════════════════════════════════════════>  Server
        ════════════════════════════════════════════════>
        ════════════════════════════════════════════════>
        Parallel chunk transfer with per-chunk BLAKE3 ACKs
```

- **Control connection**: single TCP/TLS, used for handshake + final status
- **Data connections**: plain TCP, one per worker, persistent (multiple chunks per connection)
- **Resume**: client queries server for already-received chunks before sending. Server validates `.bee-meta` metadata — re-verifies every `.part` hash, detects chunk-size changes and corruption

## Project Structure

```
bee-sync/
├── Cargo.toml              # workspace root ([workspace] only)
├── bee-sync-core/           # lib: protocol, server, client, cli types, utils
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs           # re-exports: client, server, protocol, cli, utils
│       ├── cli.rs           # Clap command definitions (Cli, ServerArgs, ClientArgs)
│       ├── protocol.rs      # Frame transport (length-prefixed), handshake/chunk constants
│       ├── file_ops.rs      # BLAKE3 calculation (calc_hash, file_hash)
│       ├── utils.rs         # parse_address, parse_chunk_size, download_file
│       ├── client/
│       │   ├── mod.rs       # run_client, perform_handshake, worker orchestration
│       │   ├── tls.rs       # NoCertVerifier, connect_to_server, Stream trait
│       │   └── worker.rs    # WorkerConfig, send_chunk, query_received, worker task loop
│       └── server/
│           ├── mod.rs       # run_server, accept loop, ACTIVE_RECEIVERS, PORT_POOL
│           ├── tls.rs       # load_tls_context
│           ├── metadata.rs  # TransferMetadata — binary .bee-meta format, safe resume
│           ├── file_receiver.rs # FileReceiver state machine (part files, assembly, metadata)
│           └── handler/
│               ├── mod.rs   # Re-exports control + data handlers
│               ├── control.rs # handle_control_connection, handshake parsing, orchestration
│               └── data.rs  # handle_data_connection, chunk processing, ACK
├── bee-sync-cli/            # bin: CLI (clap)
│   ├── Cargo.toml           # depends on bee-sync-core
│   └── src/main.rs          # tokio::main, subcommand dispatch, logging init
└── bee-sync-gui/            # bin: Slint native GUI
    ├── Cargo.toml           # depends on bee-sync-core + slint
    ├── build.rs             # compiles ui/main.slint → Rust
    ├── src/main.rs          # tokio::main, AppWindow, start/stop server callback
    └── ui/main.slint        # 3-tab layout: Dashboard, Log, Config
```

### Protocol Frame Format

Every message on the wire is length-prefixed: 4 bytes big-endian length + payload.

- **Handshake**: `MAGIC(4) + filename_len(2,BE) + filename + file_size(8,BE) + chunk_size(4,BE) + num_chunks(4,BE) + full_hash(32)`
- **Handshake response**: `status(1) + num_ports(1) + ports(num_ports * 2,BE)`
- **Chunk message**: `chunk_index(4,BE) + chunk_offset(8,BE) + chunk_size(4,BE) + chunk_data + chunk_hash(32)`
- **ACK**: single byte (`ACK_OK = 0x00`, `ACK_HASH_MISMATCH = 0x01`)
- **Query (resume)**: single byte `QUERY_MAGIC = 0x01`
- **Query response**: `count(4,BE) + indices(count * 4,BE)`

### Response codes

| Code | Name | Meaning |
|------|------|---------|
| `0` | `RESP_OK` | Transfer accepted, data ports follow |
| `1` | `RESP_ERR` | Server error |
| `2` | `RESP_EXISTS` | File already exists with matching hash |
| `3` | `RESP_COMPLETE` | All chunks already valid, no ports needed |

### Metadata format (`.bee-meta`)

Persisted alongside `.part` files. Written atomically (tmp + rename). Temp file cleaned up on rename failure.

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | Magic `"BEMT"` |
| 4 | 1 | Version (1) |
| 5 | 4 | chunk_size (u32 BE) |
| 9 | 4 | num_chunks (u32 BE) |
| 13 | 8 | file_size (u64 BE) |
| 21 | 32 | full_hash (BLAKE3) |
| 53 | 4 | entry count (u32 BE) |
| 57 | N×36 | entries: chunk_index(u32 BE) + chunk_hash(32 bytes) |

## Coding Conventions

### Rust Idiomatic
- Use `impl Trait` in function signatures over `Box<dyn Trait>` unless type erasure is necessary.
- Prefer `&[u8]` over `&Vec<u8>` for function parameters.
- Use `const` for all protocol constants (already in `bee-sync-core/src/protocol.rs`).
- Derive `Clone`, `Debug` on config structs.
- All shared logic lives in `bee-sync-core`. CLI and GUI are thin entry points.

### Module Splitting
- Each logical concern gets its own module or subdirectory.
- Server and client are separate submodules of `bee-sync-core`.
- TLS logic lives in per-side `tls.rs` modules, not in a shared crate.
- Protocol constants and framing functions are in `protocol.rs`, shared by both sides.

### Function Granularity
- Functions should do one thing. If a function exceeds ~50 lines, extract helpers.
- Use private helper functions (`async fn` or plain `fn`) within the same module.
- Decomposition pattern: orchestrator → setup → execute → cleanup.

### Error Handling
- Return `anyhow::Result<T>` from fallible functions. Never call `process::exit()` — propagate errors.
- In `async fn`, use `?` for propagation.
- Log errors with `log::error!()` before returning them.
- Don't panic. Use `anyhow::bail!()` for unrecoverable states.

### Async Patterns
- Use `tokio::spawn` for concurrent connection handlers.
- `Arc<Mutex<T>>` for shared mutable state across spawned tasks.
- `AtomicBool` for shutdown signals (both client and server).
- `tokio::net::TcpStream` for all network I/O.
- The main function uses `#[tokio::main]`.

### Shared State
- `LazyLock<Mutex<HashMap<u16, Arc<Mutex<FileReceiver>>>>>` for global port→receiver registry.
- `tokio::sync::Semaphore` for port pool and connection limits.
- `Arc<Mutex<indicatif::ProgressBar>>` for progress tracking.
- `Arc<AtomicBool>` for graceful shutdown signals.

### Slint GUI
- UI defined in `bee-sync-gui/ui/main.slint` (declarative DSL).
- Properties accessed from Rust must be declared `in-out property<Type>`.
- Build script (`build.rs`) calls `slint_build::compile()` at build time.
- Server runs in `tokio::spawn`, UI updates via property bindings.
- `ComboBox` uses `current-value` (string) with `<=>` two-way binding.

## Build & Test

```bash
# Build CLI
cargo build -p bee-sync-cli --release

# Build GUI
cargo build -p bee-sync-gui --release

# Build everything
cargo build --release

# Lint and format (always run fmt before commit)
cargo fmt
cargo clippy --workspace -- -D warnings

# Run tests
cargo test -p bee-sync-core
```

## Rules

1. Big module → split into submodules
2. Prefer existing crates, don't reinvent
3. **Never edit Cargo.toml directly** — use `cargo add` with latest version
4. Always build with `cargo build --release`
5. Avoid `unsafe`
6. Always run `cargo fmt` before commit
7. Always run `cargo clippy` after build
8. AGENT.md + README.md kept in sync with module tree and crate structure

## Observability

- `--verbose` enables `LevelFilter::Debug`
- Log format: `[LEVEL] message`
- Server logs: client connect, handshake params, transfer summary (complete/fail with re-run hint)
- Client logs: total elapsed time ("completed in" vs "interrupted after", `Xm Ys` format)
- Progress bar via indicatif (hidden in verbose mode or non-TTY)
- GUI has a dedicated Log tab showing server output

## Don't Reinvent the Wheel

- **Never** implement your own BLAKE3, async runtime, TLS, CLI parser, logger, or GUI framework. Use the crates listed in the Technology Stack table.
- When a standard library API exists (`std::fs`, `std::path`, `std::io`), prefer it over external crates.
- For protocol serialization, use manual `to_be_bytes()` / `from_be_bytes()` — the protocol is simple enough that `serde` would be overkill.
