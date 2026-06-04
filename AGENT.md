# bee-sync - Agent instruction

## Project Overview

Fast parallel file transfer over TCP/TLS with chunked concurrency, BLAKE3 integrity, resume support.

**Stack**: Rust 2024 edition, tokio async, clap derive, anyhow errors, rustls TLS, blake3 hashing, indicatif progress.

## Technology Stack

| Concern | Crate | Notes |
| ------- | ----- | ----- |
| Error handling | `anyhow` | Use `anyhow::Result<T>` everywhere. Use `anyhow::bail!()` for early returns, `anyhow::anyhow!()` for ad-hoc errors. |
| Async runtime | `tokio` (full features) | All I/O is async. Prefer `tokio::spawn` for concurrent tasks. Use `tokio::net::TcpStream` / `TcpListener`. |
| CLI parsing | `clap` (derive) | `#[derive(Parser)]` / `#[derive(Subcommand)]`. See `src/cli.rs` for the pattern. |
| Logging | `fern` + `log` | Use `log::info!()`, `log::error!()`, `log::debug!()`. The logger is initialized in `src/logger.rs`. `--verbose` enables debug level. |
| Progress bars | `indicatif` | Use `ProgressBar` for transfer progress. Wrap in `Arc<Mutex<ProgressBar>>` for shared access across workers. |
| TLS | `rustls` 0.23 + `tokio-rustls` + `webpki-roots` | Server uses `TlsAcceptor`, client uses `TlsConnector`. Custom `NoCertVerifier` for `--tls-no-verify`. |
| Hashing | `blake3` | BLAKE3 for chunk and full-file integrity verification. Wrappers in `src/file_ops.rs`. |
| Signal handling | `ctrlc` | Graceful shutdown via `AtomicBool` flag. |

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

- **Control connection**: single TCP/TLS, used for handshake only
- **Data connections**: plain TCP, one per worker, persistent (multiple chunks per connection)
- **Resume**: client queries server for already-received chunks before sending

### Module Tree

```
src/
├── main.rs          Entry point, tokio::main, subcommand dispatch
├── cli.rs           Clap command definitions (Cli, ServerArgs, ClientArgs)
├── logger.rs        Fern-based logging initialization
├── protocol.rs      Frame format, MAGIC, constants, send_frame / recv_frame
├── file_ops.rs      BLAKE3 calculation (calc_hash, file_hash)
├── utils.rs         Chunk size parser (parse_chunk_size)
├── client/
│   ├── mod.rs       run_client, perform_handshake, worker orchestration
│   ├── tls.rs       NoCertVerifier, connect_to_server, Stream trait
│   └── worker.rs    send_chunk, query_received, worker task loop
└── server/
    ├── mod.rs       run_server, accept loop, ACTIVE_RECEIVERS registry
    ├── tls.rs       load_tls_context
    ├── file_receiver.rs  FileReceiver state machine (part files, assembly)
    └── handler/
        ├── mod.rs       Re-exports control + data handlers
        ├── control.rs   handle_control_connection, handshake parsing, orchestration
        └── data.rs      handle_data_connection, chunk processing, ACK
```

### Protocol Frame Format

Every message on the wire is length-prefixed: 4 bytes big-endian length + payload.

- **Handshake**: `MAGIC(4) + filename_len(2,BE) + filename + file_size(8,BE) + chunk_size(4,BE) + num_chunks(4,BE) + full_hash(32)`
- **Handshake response**: `status(1) + num_ports(1) + ports(num_ports * 2,BE)`
- **Chunk message**: `chunk_index(4,BE) + chunk_offset(8,BE) + chunk_size(4,BE) + chunk_data + chunk_hash(32)`
- **ACK**: single byte (`ACK_OK = 0x00`, `ACK_HASH_MISMATCH = 0x01`)
- **Query (resume)**: single byte `QUERY_MAGIC = 0x01`
- **Query response**: `count(4,BE) + indices(count * 4,BE)`

## Coding Conventions

### Rust Idiomatic
- Use `impl Trait` in function signatures over `Box<dyn Trait>` unless type erasure is necessary (e.g., the client's `connect_to_server` return type).
- Prefer `&[u8]` over `&Vec<u8>` for function parameters.
- Use `const` for all protocol constants (already in `src/protocol.rs`).
- Derive `Clone`, `Debug` on config structs.

### Module Splitting
- Each logical concern gets its own module or subdirectory.
- Server and client are separate top-level modules (`src/client/`, `src/server/`).
- TLS logic lives in per-side `tls.rs` modules, not in a shared crate.
- Protocol constants and framing functions are in `src/protocol.rs`, shared by both sides.

### Function Granularity
- Functions should do one thing. If a function exceeds ~50 lines, extract helpers.
- Use private helper functions (`async fn` or plain `fn`) within the same module.
- Example decomposition pattern from the codebase:
  - `run_client()` orchestrates: `setup_transfer()` → `perform_handshake()` → `setup_workers()` → `run_workers()`
  - `handle_control_connection()` orchestrates: `parse_handshake()` → `allocate_sockets()` → `send_handshake_response()` → `spawn_data_servers()` → `wait_for_completion()` → `assemble_file()` → `cleanup()`

### Error Handling
- Return `anyhow::Result<T>` from fallible functions.
- In `async fn`, use `?` for propagation.
- At the top-level entry points (`main.rs`), match on `Result` and return exit codes (0 for success, 1 for error).
- Log errors with `log::error!()` before returning them.
- Don't panic. Use `anyhow::bail!()` for unrecoverable states.

### Async Patterns
- Use `tokio::spawn` for concurrent connection handlers.
- `Arc<Mutex<T>>` for shared mutable state across spawned tasks.
- `AtomicBool` for shutdown signals.
- `tokio::net::TcpStream` for all network I/O.
- The main function uses `#[tokio::main]`.

### Thread Safety
- `LazyLock<Mutex<HashMap<...>>>` for global registries (see `ACTIVE_RECEIVERS`).
- `Arc<Mutex<FileReceiver>>` for per-transfer shared state.
- Progress bars: `Arc<Mutex<ProgressBar>>`.

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

## Build & Run

```bash
# Always build with --release
cargo build --release

# Run server (plain TCP)
cargo run --release -- server

# Run server (TLS)
cargo run --release -- server --cert cert.pem --key key.pem

# Run client (plain TCP)
cargo run --release -- client --file testfile.bin

# Run client (TLS, self-signed cert)
cargo run --release -- client --tls --tls-no-verify --file testfile.bin

# Debug logging
cargo run --release -- server --verbose
```

## Static Analysis

```bash
# Before committing, run these checks:
cargo build --release              # Must compile with zero warnings
cargo fmt --check                  # Formatting (if rustfmt is configured)
cargo clippy -- -D warnings        # Lints (if clippy is installed)
```

## Rules (from ~/rules/rust.md)

1. Big module → split into submodules
2. Prefer existing crates, don't reinvent
3. **Never edit Cargo.toml directly** — use `cargo add` with latest version
4. Always build with `cargo build --release`
5. Avoid `unsafe`
6. Always run `cargo clippy` after build
7. Always run `cargo fmt` after fix clippy

## Observability

- `--verbose` enables `LevelFilter::Debug`
- Log format: `[LEVEL] message`
- Progress bar via indicatif (hidden in debug/verbose mode)

## Don't Reinvent the Wheel

- **Never** implement your own BLAKE3, async runtime, TLS, CLI parser, or logger. Use the crates listed in the Technology Stack table.
- When a standard library API exists (`std::fs`, `std::path`, `std::io`), prefer it over external crates.
- For protocol serialization, use manual `to_be_bytes()` / `from_be_bytes()` — the protocol is simple enough that `serde` would be overkill.

## Module Tree (Expanded)

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

## Build Commands

```bash
# Build
cargo build --release

# Check
cargo clippy -- -D warnings
cargo fmt --check
```

## Run Examples

### Server
```bash
# Plain TCP
cargo run --release -- server

# TLS
cargo run --release -- server --cert cert.pem --key key.pem

# With temp dir
cargo run --release -- server --output-dir ./received/

# Debug
cargo run --release -- server --verbose
```

### Client
```bash
# Plain TCP
cargo run --release -- client --file <path> --address localhost:19999

# TLS with verification disabled
cargo run --release -- client --tls --tls-no-verify --file <path>

# Download from URL
cargo run --release -- client --url <url> --temp-dir /tmp
```
