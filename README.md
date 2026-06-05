# bee-sync

A fast, parallel file transfer tool written in Rust. Transfers files over TCP/TLS with
concurrent chunked connections, BLAKE3 integrity verification, and verified resume support.

## Crates

| Crate | Type | Description |
|-------|------|-------------|
| `bee-sync-core` | lib | Protocol, server, client, CLI types, utilities |
| `bee-sync-cli` | bin | Command-line client and server |
| `bee-sync-gui` | bin | Slint-based server dashboard (Windows/Linux/macOS) |

## Features

- **Parallel transfer** — 25 concurrent TCP connections transfer chunks in parallel for high
  throughput even on low-bandwidth-per-connection links
- **Verified resume** — per-chunk BLAKE3 hashes persisted to `.bee-meta` files. On resume,
  transfer parameters are validated and every `.part` is re-hashed to detect corruption
  or chunk-size mismatches
- **TLS encryption** — control channel encrypted with `rustls`; `--tls-no-verify` for
  self-signed certs (requires `--tls`)
- **BLAKE3 hashing** — fast, cryptographically secure integrity checks on every chunk and
  the assembled file (~3–5 GB/s per core)
- **Graceful shutdown** — Ctrl+C on client finishes in-flight chunks and exits cleanly;
  on server stops accepting new connections and lets active transfers complete
- **Dynamic timeouts** — per-operation timeouts scale with chunk size (150 KB/s floor),
  with automatic backoff on retry
- **DoS hardening** — frame size limits, connection caps, port-pool semaphore, and input
  validation throughout
- **Progress bar** — live transfer progress with speed and ETA
- **URL download** — client can download from a URL before transferring to the server
- **GUI dashboard** — standalone server monitor with config, log, and transfer status

## Quick Start

```bash
# Build everything
cargo build --release

# Build CLI only
cargo build -p bee-sync-cli --release

# Build GUI only
cargo build -p bee-sync-gui --release
```

```bash
# Start server (plain TCP)
./target/release/bee-sync-cli server

# Or use the GUI
./target/release/bee-sync-gui

# Send a file
./target/release/bee-sync-cli client --file myfile.bin
```

## Usage

### Server

```
bee-sync-cli server [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-a`, `--address` | `0.0.0.0:19999` | Bind address (host:port) |
| `-o`, `--output-dir` | `./received/` | Directory for final assembled files |
| `-t`, `--temp-dir` | *(same as output)* | Directory for in-progress `.part` files |
| `-c`, `--cert` | — | TLS certificate (PEM) |
| `-k`, `--key` | — | TLS private key (PEM) |
| `-m`, `--max-parallel` | `100` | Maximum parallel data connections per transfer |
| `-v`, `--verbose` | — | Enable debug logging |

### Server GUI

```
bee-sync-gui
```

Launches a native Slint window with three tabs:
- **Dashboard** — active transfer count, server status, bind info
- **Log** — scrollable log output
- **Config** — bind address, port, output dir, TLS cert/key, max parallel

### Client

```
bee-sync-cli client --file PATH [OPTIONS]
bee-sync-cli client --url URL [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-f`, `--file` | *(required)* | File to send (mutually exclusive with `--url`) |
| `-u`, `--url` | — | Download from URL then send |
| `-a`, `--address` | `localhost:19999` | Server address (host:port) |
| `-s`, `--chunk-size` | `2M` | Chunk size, e.g. `1M`, `10M` (mutually exclusive with `--chunk-count`) |
| `-n`, `--chunk-count` | — | Split into N chunks instead (mutually exclusive with `--chunk-size`) |
| `-p`, `--parallel` | `25` | Max parallel data connections |
| `-r`, `--retries` | `3` | Retries per chunk |
| `-t`, `--temp-dir` | `/tmp` | Temp directory for URL downloads |
| `--tls` | — | Enable TLS |
| `--tls-no-verify` | — | Skip certificate verification (requires `--tls`) |
| `-v`, `--verbose` | — | Enable debug logging |

### Examples

```bash
# Basic transfer
bee-sync-cli server
bee-sync-cli client --file ubuntu.iso

# TLS with self-signed cert
bee-sync-cli server --cert cert.pem --key key.pem
bee-sync-cli client --tls --tls-no-verify --file ubuntu.iso

# Custom chunking: split into exactly 8 chunks
bee-sync-cli client --chunk-count 8 --file largefile.bin

# Custom server port and output directory
bee-sync-cli server --address 0.0.0.0:8080 --output /srv/staging
bee-sync-cli client --address myserver.local:8080 --file data.bin

# Download from URL then transfer
bee-sync-cli client --url https://example.com/file.bin
```

## Architecture

```
          Control Channel (port 19999, TLS optional)
Client  ─────────────────────────────────────────────────>  Server
        <────────────────────────────────────────────────
        Handshake: filename, size, chunk_size, num_chunks, BLAKE3 hash
        Response:  status + allocated data port list

          Data Channels (ports 45000–46000, plain TCP)
Client  ═════════════════════════════════════════════════>  Server
        ═════════════════════════════════════════════════>
        ═════════════════════════════════════════════════>
        Parallel chunk transfer with per-chunk BLAKE3 ACKs
```

- **Control connection** (port 19999): single TCP/TLS connection for handshake and
  final confirmation — client sends file metadata, server responds with allocated
  data ports
- **Data connections** (ports 45000–46000): plain TCP, one per worker, persistent
  across multiple chunks. Client queries for already-received chunks before sending
  (resume)
- **Metadata** (`.bee-meta`): binary file stored alongside `.part` files with
  transfer parameters and per-chunk BLAKE3 hashes. Enables safe resume with
  corruption detection and chunk-size validation
- After all chunks arrive, the server assembles the `.part` files, verifies the
  full-file BLAKE3 hash, and sends a final status to the client

## Protocol

Every wire message is a length-prefixed frame: 4-byte big-endian length + payload.

| Message | Format |
| ------- | ------ |
| Handshake | `"BESN"(4) + filename_len(2,BE) + filename + file_size(8,BE) + chunk_size(4,BE) + num_chunks(4,BE) + full_hash(32)` |
| Handshake response | `status(1) + num_ports(1) + ports(N×2,BE)` |
| Chunk data | `chunk_index(4,BE) + chunk_offset(8,BE) + chunk_size(4,BE) + data + chunk_hash(32)` |
| Chunk ACK | `ACK_OK(0x00)` or `ACK_HASH_MISMATCH(0x01)` |
| Resume query | `QUERY_MAGIC(0x01)` |
| Resume response | `count(4,BE) + indices(N×4,BE)` |

### Response codes

| Code | Name | Meaning |
|------|------|---------|
| `0` | `RESP_OK` | Transfer accepted, data ports follow |
| `1` | `RESP_ERR` | Server error |
| `2` | `RESP_EXISTS` | File already exists with matching hash |
| `3` | `RESP_COMPLETE` | All chunks already valid, no ports needed |

### Metadata format (`.bee-meta`)

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

## Build from Source

Requires Rust 1.94+ (edition 2024).

```bash
git clone https://github.com/lulkien/bee-sync.git
cd bee-sync

# CLI only
cargo build -p bee-sync-cli --release

# GUI (requires Slint's system dependencies)
cargo build -p bee-sync-gui --release

# Everything
cargo build --release
```

## Project Structure

```
bee-sync/
├── Cargo.toml              # workspace root
├── bee-sync-core/           # lib: protocol, server, client, utils
│   └── src/
├── bee-sync-cli/            # bin: CLI (clap)
│   └── src/main.rs
└── bee-sync-gui/            # bin: Slint server dashboard
    ├── src/main.rs
    └── ui/main.slint
```

## Security

- ✅ Frame size limits (16 MiB) prevent memory exhaustion
- ✅ Chunk count capped at 1,000,000
- ✅ Bounds checks on all network-provided indices
- ✅ Per-operation timeouts with throughput-aware duration
- ✅ Connection and port-pool limits prevent resource exhaustion
- ✅ BLAKE3 for cryptographic integrity
- ✅ Per-chunk hash verification on resume detects disk corruption
- ✅ Chunk-size validation on resume prevents mismatched reassembly
- ✅ Chunk size overflow guard rejects values > `usize::MAX`

## License

[UNLICENSE](UNLICENSE)
