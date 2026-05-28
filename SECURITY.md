# bee-sync Security Audit

> Generated 2026-05-28 — covers commit at time of review

## Scope

Full source review of the bee-sync file transfer tool. Both client and server, all protocol layers, TLS configuration, and filesystem interactions.

---

## Findings

### CRITICAL — Remote DoS / Crash

#### 1. Unbounded `num_chunks` → OOM / panic

**File:** `src/server/handler/control.rs:283`
**Also:** `src/server/file_receiver.rs:32`

`num_chunks` comes from the network as `u32`. `FileReceiver::new()` creates `vec![false; num_chunks]`. A malicious handshake with `num_chunks = u32::MAX` (~4 billion) tries to allocate ~4 GB of booleans, OOM-killing the process.

**Exploit:** Send a single handshake frame. No authentication required.

**Status:** ✅ Fixed — `parse_handshake` in `src/server/handler/control.rs` now rejects `num_chunks > 1_000_000`, `num_chunks == 0`, and `chunk_size == 0` before `FileReceiver` allocation.

---

#### 2. `recv_frame` allocates up to 4 GB from network → OOM

**File:** `src/protocol.rs:66-75`

`recv_frame` reads a 4-byte length prefix and allocates `vec![0u8; payload_len]`. A value of `0xFFFFFFFF` causes a ~4 GB allocation, OOM-killing either client or server.

**Exploit:** Send 4 bytes of `0xFF` as the frame header on any connection. No authentication required.

**Status:** ✅ Fixed — `frame::recv` in `src/protocol.rs` now rejects payloads > 16 MiB (`frame::MAX_PAYLOAD`) before allocation.

---

#### 3. `chunk_index` out-of-bounds → panic

**File:** `src/server/handler/data.rs:220`

`recv.received_chunks[chunk_index] = true` has no bounds check. A chunk message with `chunk_index > num_chunks` panics the server with an index-out-of-bounds error.

**Exploit:** Send a single chunk frame with a crafted index on any data connection.

**Status:** ✅ Fixed — `process_chunk` in `src/server/handler/data.rs` now validates `chunk_index < num_chunks` before passing to `verify_and_write_chunk`.

---

#### 4. Integer overflow in chunk offset calculation

**File:** `src/client/worker.rs:141`

`idx as u64 * config.chunk_size as u64` — both values originate from the server's handshake response (derived from client input or computed server-side). The product of two `u32` values can overflow `u64` in edge cases.

**Status:** ✅ Fixed — `worker.rs` uses `checked_mul` + bounds check; invalid offsets log and skip the chunk.

---

#### 5. `actual_size` underflow panic

**File:** `src/client/worker.rs:142`

`config.file_size as usize - offset as usize` panics on underflow if `offset > file_size`. A rogue server could set `file_size` in the handshake response smaller than what the chunk offsets imply.

**Status:** ✅ Fixed — `worker.rs` validates `offset <= file_size` before computing `remaining`, avoiding underflow.

---

### HIGH — Resource Exhaustion / Hanging

#### 6. No connection timeouts

**Files:** `src/protocol.rs`, `src/server/handler/control.rs`, `src/server/handler/data.rs`, `src/client/worker.rs`

Neither `TcpStream` nor TLS connections have read/write timeouts set. An attacker who opens a connection and sends nothing causes:
- `frame::recv` to block forever
- `wait_for_completion` to poll forever (data connection never finishes)
- Worker tasks to hang indefinitely

**Exploit:** Connect to any server port and idle.

**Status:** ✅ Fixed — all `frame::send`/`recv`/`send_parts` replaced with `*_timeout` variants (30s per operation). Persistent connections reset the clock per chunk. Server `Semaphore` caps concurrent connections at 128.

---

#### 7. Unlimited concurrent connections

**File:** `src/server/mod.rs:147`

Every accepted control connection spawns an unbounded `tokio::spawn`. No connection limit, no backpressure.

**Exploit:** Open thousands of TCP connections to port 19999. Each spawns a task; combined with #6, each task lives indefinitely.

**Status:** ✅ Fixed — `Semaphore::new(MAX_CONCURRENT_CONNECTIONS=128)` in `run_server`. Excess connections are rejected with a log message.

---

#### 8. Data port exhaustion

**File:** `src/server/handler/control.rs:170-192`

A handshake with `num_chunks >= max_parallel` (default 100) binds up to 100 ports in range 45000-46000 (1000 ports total). A few concurrent transfers can saturate the range.

**Exploit:** 10 concurrent handshakes with `num_chunks = 100` exhaust all 1000 data ports. New transfers fail with "Failed to allocate data port".

**Status:** ✅ Fixed — global `PORT_POOL` semaphore (1001 permits). `allocate_sockets` reserves before binding; `cleanup` releases on completion. Transfers are rejected when the pool is exhausted.

---

### MEDIUM — Data Integrity / Logic

#### 9. `chunk_size = 0` not rejected by server

**File:** `src/server/handler/control.rs:279`

The server does not validate `chunk_size > 0`. A zero value causes `div_ceil(0)` panics downstream. The legitimate client enforces this, but a modified client can send 0.

**Fix:** Reject handshake if `chunk_size == 0`.

---

#### 10. Chunk data slice length mismatch → panic

**File:** `src/server/handler/data.rs:164-183`

The code checks `data.len() < HEADER_SIZE + MD5_SIZE` but not `data.len() >= HEADER_SIZE + chunk_size + MD5_SIZE`. If `chunk_size` in the chunk header exceeds the actual remaining data, the slice at line 177 panics.

**Fix:** Check `data.len() >= chunk::HEADER_SIZE + chunk_size + chunk::MD5_SIZE`.

---

#### 11. MD5 for file integrity

**Files:** `src/file_ops.rs`, `src/server/file_receiver.rs:80`

MD5 is cryptographically broken — collision attacks are practical. A MITM attacker who knows the expected MD5 could craft a different file with the same hash. For a file transfer tool, this is moderate risk depending on threat model.

**Fix:** Consider SHA-256 if cryptographic integrity is needed.

---

#### 12. Filename up to 64 KB

**Files:** `src/server/handler/control.rs:247`, `src/client/mod.rs:135`

`filename_len` is `u16` → up to 65,535 bytes. While `Path::file_name()` strips directory components, a 64 KB base filename could cause issues with filesystem limits and memory usage.

**Fix:** Cap filename length (e.g. 255 bytes, the common filesystem limit).

---

### MEDIUM — Data Channel Security

#### 13. Data channels unauthenticated, plain TCP

**Files:** `src/server/handler/data.rs:58`, `src/client/worker.rs`

Data connections on ports 45000-46000 are plain TCP with no authentication. Anyone on the network can:
- **Query received chunks** — info disclosure about active transfers
- **Inject bogus chunks** — caught by MD5 but wastes server I/O
- **Connect and idle** — blocks `wait_for_completion`
- **Port-scan the range** — enumerate active transfers

**Fix:** Consider TLS for data channels. At minimum, validate that chunks reference a valid transfer ID.

---

#### 14. No validation of chunk data length against expected chunk_size

**File:** `src/server/handler/data.rs:177`

The server doesn't verify that `chunk_size` in the chunk header matches the handshake's `chunk_size` for that index. An attacker could send arbitrary-sized chunk payloads (within the frame limit). The MD5 check provides some protection.

**Fix:** Validate chunk_size against the expected size for the chunk index (last chunk may differ).

---

### LOW — Edge Cases

#### 15. `--tls-no-verify` disables all certificate validation

**File:** `src/client/tls.rs:26-66`

The `NoCertVerifier` accepts any certificate, enabling trivial MITM when `--tls-no-verify` is used. This is by design and the flag is explicit.

**Risk:** Accepted risk — documented behavior.

---

#### 16. TOCTOU on file assembly

**File:** `src/server/file_receiver.rs:60-86`

`assemble()` checks `.part` file existence, then opens and reads each part. Between the check and the read, another process could modify part files. Low risk since this is a local-only race.

**Fix:** Open files unconditionally and handle the error.

---

#### 17. `parse_response` accepts up to 255 ports from server

**File:** `src/client/mod.rs:183`

A rogue server could send `num_ports = 255`, causing the client to attempt 255 data connections. The legitimate server sends at most `max_parallel` (100).

**Fix:** Cap at a reasonable value or match against `max_parallel`.

---

### Summary

| # | Severity | Issue | Exploitable by |
| - | -------- | ----- | -------------- |
| 1 | Critical | `num_chunks` unbounded → OOM | Any remote client |
| 2 | Critical | `recv_frame` 4 GB allocation → OOM | Any remote peer |
| 3 | Critical | `chunk_index` OOB → panic | Any remote client |
| 4 | Critical | Integer overflow in offset calc | Rogue server |
| 5 | Critical | `actual_size` underflow panic | Rogue server |
| 6 | High | No connection timeouts | Any remote peer |
| 7 | High | Unlimited concurrent connections | Any remote peer |
| 8 | High | Data port exhaustion | Any remote client |
| 9 | Medium | `chunk_size = 0` not rejected | Modified client |
| 10 | Medium | Chunk data slice mismatch → panic | Any remote client |
| 11 | Medium | MD5 for integrity | MITM (with crypto effort) |
| 12 | Medium | Filename up to 64 KB | Any remote client |
| 13 | Medium | Unauthenticated data channels | Network attacker |
| 14 | Medium | No chunk_size verification | Any remote client |
| 15 | Low | `--tls-no-verify` MITM | By design |
| 16 | Low | TOCTOU on assembly | Local process only |
| 17 | Low | `parse_response` max 255 ports | Rogue server |

### Recommended Fix Priority

1. **Add frame size limit** in `recv_frame` (#2) — single-line check, highest impact
2. **Cap `num_chunks`** against `file_size` (#1) — stops OOM from handshake
3. **Bounds-check `chunk_index`** (#3) — stops panic from data channel
4. **Add connection timeout** (#6) — stops indefinite hangs
5. **Validate chunk data length** in `process_chunk` (#10) — stops panic
6. **Cap concurrent connections** (#7) — stops connection flood DoS
