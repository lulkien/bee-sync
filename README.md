# bee-sync

A fast, parallel file transfer tool written in Rust. Transfers files over TCP/TLS with
concurrent chunked connections, BLAKE3 integrity verification, and resume support.

## Features

- **Parallel transfer** — multiple TCP connections transfer chunks concurrently for high
  throughput even on low-bandwidth-per-connection links
- **Resume support** — interrupted transfers pick up where they left off without re-sending
  already-received chunks
- **TLS encryption** — control channel is encrypted with `rustls`; optional certificate
  verification skip for self-signed certs
- **BLAKE3 hashing** — fast, cryptographically secure integrity checks on every chunk and
  the assembled file (~3–5 GB/s per core)
- **Dynamic timeouts** — per-operation timeouts scale with chunk size (150 KB/s floor),
  with automatic backoff on retry
- **DoS hardening** — frame size limits, connection caps, port-pool semaphore, and input
  validation throughout
- **Progress bar** — live transfer progress with speed and ETA

## Quick Start

```bash
# Build
cargo build --release

# Start server (plain TCP)
./target/release/bee-sync server

# Send a file
./target/release/bee-sync client --file myfile.bin
```

## Usage

### Server

```
bee-sync server [OPTIONS]
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

### Client

```
bee-sync client --file PATH [OPTIONS]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-f`, `--file` | *(required)* | File to send |
| `-a`, `--address` | `localhost:19999` | Server address (host:port) |
| `-s`, `--chunk-size` | `5M` | Chunk size (e.g. `1M`, `10M`, `1G`) |
| `-n`, `--chunk-count` | — | Split into N chunks |
| `-p`, `--parallel` | `10` | Max parallel data connections |
| `-r`, `--retries` | `3` | Retries per chunk |
| `--tls` | — | Enable TLS |
| `--tls-no-verify` | — | Skip certificate verification |
| `-v`, `--verbose` | — | Enable debug logging |

### Examples

```bash
# Basic transfer
bee-sync server
bee-sync client --file ubuntu.iso

# TLS with self-signed cert
bee-sync server --cert cert.pem --key key.pem
bee-sync client --tls --tls-no-verify --file ubuntu.iso

# Custom chunking: split into exactly 8 chunks
bee-sync client --chunk-count 8 --file largefile.bin

# Custom server port and output directory
bee-sync server --address 0.0.0.0:8080 --output /srv/staging
bee-sync client --address myserver.local:8080 --file data.bin
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

## Build from Source

Requires Rust 1.82+ (edition 2024).

```bash
git clone https://github.com/your-org/bee-sync.git
cd bee-sync
cargo build --release
```

## Security

A [full security audit](SECURITY.md) is available. All critical and high-severity findings
have been addressed:

- ✅ Frame size limits (16 MiB) prevent memory exhaustion
- ✅ Chunk count capped at 1,000,000
- ✅ Bounds checks on all network-provided indices
- ✅ Per-operation timeouts with throughput-aware duration
- ✅ Connection and port-pool limits prevent resource exhaustion
- ✅ BLAKE3 replaces MD5 for cryptographic integrity

## License

[UNLICENSE](UNLICENSE)
