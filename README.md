# BSEngine

> A custom embedded key-value storage engine with a binary TCP interface, written in Go.

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-production--ready-brightgreen.svg)]()
[![Version](https://img.shields.io/badge/version-v4.0.0-informational.svg)]()

BSEngine is a from-scratch, single-file key-value store inspired by the internals of PostgreSQL and SQLite. It implements a full database storage stack — slotted pages, an LRU buffer pool, a Write-Ahead Log with crash replay, shadow defragmentation, and a persistent binary TCP protocol — in under 1 420 lines of idiomatic Go.

Designed for extreme resource efficiency: the system consumes near-zero CPU and I/O during idle periods, scales gracefully under high concurrent load, and recovers cleanly from crashes without data loss.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
  - [Run](#run)
  - [Configuration](#configuration)
  - [Docker](#docker)
- [Wire Protocol](#wire-protocol)
  - [Request Frame](#request-frame)
  - [Response Frame](#response-frame)
  - [Opcodes](#opcodes)
  - [Status Codes](#status-codes)
- [Operations](#operations)
- [Storage Internals](#storage-internals)
  - [Page Layout](#page-layout)
  - [Record Encoding](#record-encoding)
  - [Write-Ahead Log](#write-ahead-log)
  - [Buffer Pool](#buffer-pool)
  - [Defragmentation](#defragmentation)
- [Performance Design](#performance-design)
- [Security & Stability Guarantees](#security--stability-guarantees)
- [Limitations](#limitations)
- [Changelog](#changelog)
- [License](#license)

---

## Features

| Category | Detail |
|---|---|
| **Data model** | Binary key-value store; keys up to 64 bytes, values up to 10 MB |
| **Durability** | Write-Ahead Log with per-entry CRC32; fsync before checkpoint |
| **Crash recovery** | Full WAL replay on startup — no data loss after a clean crash |
| **Storage** | Slotted-page layout (4 KiB pages) with linked chunk overflow |
| **Caching** | LRU buffer pool (64 MB default) with pin-count eviction |
| **Maintenance** | Shadow defragmentation (copy-on-write atomic file swap) |
| **Concurrency** | `sync.RWMutex` engine lock; atomic page ID and operation counters |
| **Network** | Binary TCP protocol, persistent connections, 100-connection semaphore cap |
| **Idle efficiency** | Janitor backs off to 5-minute interval when no activity is detected |
| **Zero-alloc hot path** | `sync.Pool` for header/response buffers; `net.Buffers` vectorised writes |
| **Observability** | Structured JSON logging (`log/slog`); `OpStats` health opcode |
| **Configuration** | Environment-variable driven with sensible defaults |
| **Security** | File permission `0600`; payload size limits; integer overflow guards |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          TCP Layer                               │
│  handleConnection · startTCPServer · semaphore(MaxConn=100)      │
│  sync.Pool header bufs · net.Buffers vectorised write            │
└──────────────────────────┬───────────────────────────────────────┘
                           │  Upsert / View / Delete / Incr / Stats
┌──────────────────────────▼───────────────────────────────────────┐
│                       Engine Core                                │
│  index map[string]Location  ·  sync.RWMutex                      │
│  upsertLocked / viewLocked / deleteChain                         │
│  Incr — atomic read-modify-write (single lock span)              │
│  activePageID atomic.Uint64  ·  totalOps atomic.Uint64           │
│  defragRunning atomic.Bool                                       │
└─────────────┬──────────────────────────┬─────────────────────────┘
              │                          │
┌─────────────▼────────────┐  ┌──────────▼──────────────────────── ┐
│       Buffer Pool        │  │        Write-Ahead Log             │
│  LRU · pin-count         │  │  append · checkpoint · replayWAL   │
│  ErrEvictionFailed guard │  │  CRC32 per entry · fsync           │
│  ErrPoolExhausted guard  │  │  LSN sequence number               │
│  flushAll                │  └────────────────────────────────────┘
└─────────────┬────────────┘
              │
┌─────────────▼────────────┐
│      File Manager        │
│  readPage · writePage    │
│  CRC32 verify on read    │
│  magic + version check   │
│  global header (page 0)  │
└──────────────────────────┘
              │
    [ data.bin ]   [ wal.bin ]

┌──────────────────────────────────────────────────────────────────┐
│                    Background Janitor                            │
│  Active:  30 s tick → flushAll → fsync → checkpoint → defrag    │
│  Idle:    5 min tick → skip all I/O (CPU/disk cold)             │
│  Defrag:  CAS flag prevents concurrent shadow defrag runs        │
└──────────────────────────────────────────────────────────────────┘
```

**Startup sequence:**
1. `readGlobalHeader` — validates magic (`BSENGINE`) and file version
2. `recoverIndex` — full disk scan, builds `map[string]Location`
3. `replayWAL` — re-applies WAL entries not yet flushed, verifying CRC32 per entry

**Shutdown sequence:**
1. Signal received → `cancel()` context
2. `wg.Wait()` — drains all in-flight connections and janitor
3. `flushAll()` → `fsync` → `checkpoint` → close files

---

## Getting Started

### Prerequisites

- Go 1.21 or later (uses `log/slog`, `sync/atomic` generics, `path/filepath`)

### Build

```bash
go build -o bsengine main.go
```

Optimised production build (smaller binary, better inlining):

```bash
CGO_ENABLED=0 go build -ldflags="-s -w" -o bsengine main.go
```

Or run without an explicit binary:

```bash
go run main.go
```

### Run

**Development (local files):**

```bash
./bsengine
# data stored in ./data.bin and ./wal.bin
```

**Production (Docker / systemd):**

```bash
mkdir -p /data
./bsengine
# data stored in /data/data.bin and /data/wal.bin
```

The engine detects the `/data` directory automatically via `filepath.Dir`. If it does not exist, it falls back to the current working directory.

### Configuration

All settings can be overridden via environment variables. No config file is required.

| Variable | Default | Description |
|---|---|---|
| `BSENGINE_ADDR` | `:7070` | TCP listen address (e.g. `0.0.0.0:7070`) |
| `BSENGINE_DATA_PATH` | `/data/data.bin` | Path to the data file |
| `BSENGINE_WAL_PATH` | `/data/wal.bin` | Path to the WAL file |

**Example — custom port and paths:**

```bash
BSENGINE_ADDR=":9090" \
BSENGINE_DATA_PATH="/mnt/storage/db.bin" \
BSENGINE_WAL_PATH="/mnt/storage/wal.bin" \
./bsengine
```

### Docker

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o bsengine main.go

FROM scratch
COPY --from=builder /src/bsengine /bsengine
VOLUME ["/data"]
EXPOSE 7070
ENTRYPOINT ["/bsengine"]
```

```bash
docker build -t bsengine .
docker run -d \
  -p 7070:7070 \
  -v bsengine-data:/data \
  --name bsengine \
  bsengine
```

---

## Wire Protocol

BSEngine uses a compact binary protocol over a persistent TCP connection. All multi-byte integers are **little-endian**.

### Request Frame

```
┌──────────┬────┬───────┬────────┬─────────┬─────┬───────┐
│ Magic(2) │Op  │ReqID  │KeyLen  │ ValLen  │ Key │  Val  │
│ 0xBE57   │(1) │  (4)  │  (1)   │  (4)    │ (N) │  (M)  │
└──────────┴────┴───────┴────────┴─────────┴─────┴───────┘
Total header: 12 bytes
```

| Field | Size | Description |
|---|---|---|
| `Magic` | 2 | Always `0xBE57` — frame synchronisation marker |
| `Op` | 1 | Operation code (see [Opcodes](#opcodes)) |
| `ReqID` | 4 | Client-assigned request ID, echoed verbatim in the response |
| `KeyLen` | 1 | Length of the key in bytes (1–64) |
| `ValLen` | 4 | Length of the value in bytes (0–10 485 760) |
| `Key` | N | UTF-8 or binary key |
| `Val` | M | Arbitrary binary value |

### Response Frame

```
┌──────────┬───────┬──────────┬──────────┬────────┐
│ Magic(2) │ReqID  │ Status   │ DataLen  │  Data  │
│ 0xBE57   │  (4)  │   (1)    │  (4)     │  (N)   │
└──────────┴───────┴──────────┴──────────┴────────┘
Total header: 11 bytes
```

### Opcodes

| Opcode | Hex | Request `Val` | Response `Data` |
|---|---|---|---|
| `OpPing` | `0x05` | empty | empty |
| `OpUpsert` | `0x01` | value bytes | empty |
| `OpView` | `0x02` | empty | value bytes |
| `OpDelete` | `0x03` | empty | empty |
| `OpIncr` | `0x04` | delta as little-endian `int64` (8 bytes) | new value as little-endian `int64` (8 bytes) |
| `OpStats` | `0x06` | dummy key (e.g. `"."`) | `keys(8) \| totalOps(8) \| totalPages(8)` |

### Status Codes

| Code | Hex | Meaning |
|---|---|---|
| `StatusOk` | `0x00` | Operation succeeded |
| `StatusNotFound` | `0x01` | Key does not exist |
| `StatusError` | `0x02` | Operation failed (inspect server logs) |

---

## Operations

### Ping

Verify the connection is alive. No disk I/O.

```
Request:  Magic | 0x05 | ReqID | keyLen=1 | valLen=0 | "."
Response: Magic | ReqID | 0x00 | dataLen=0
```

### Upsert

Insert or replace a key. Overwrites silently.

```
Request:  Magic | 0x01 | ReqID | keyLen | valLen | key | value
Response: Magic | ReqID | status | dataLen=0
```

### View

Retrieve the value for a key.

```
Request:  Magic | 0x02 | ReqID | keyLen | valLen=0 | key
Response: Magic | ReqID | status | dataLen | value
```

Returns `StatusNotFound` if the key does not exist.

### Delete

Remove a key permanently.

```
Request:  Magic | 0x03 | ReqID | keyLen | valLen=0 | key
Response: Magic | ReqID | status | dataLen=0
```

### Incr

Atomically add `delta` (a signed 64-bit integer) to the stored value. If the key does not exist, it is treated as `0`.

```
Request:  Magic | 0x04 | ReqID | keyLen | valLen=8 | key | delta_le64
Response: Magic | ReqID | status | dataLen=8 | new_value_le64
```

### Stats

Lightweight health check — minimal lock contention, no disk I/O.

```
Request:  Magic | 0x06 | ReqID | keyLen=1 | valLen=0 | "."
Response: Magic | ReqID | 0x00 | dataLen=24 | keys_le64 | totalOps_le64 | totalPages_le64
```

---

## Storage Internals

### Page Layout

Every page is exactly 4 096 bytes. The first `PageHeaderSize` (32) bytes form the header; the remaining space is divided between a **slot directory** growing downward from byte 32, and **record data** growing upward from byte 4 096.

```
┌─────────────────────────────── 4096 bytes ──────────────────────────────────┐
│ PageHeader(32) │ Slot[0] Slot[1] … →          ← … record[N] … record[0]  │
└────────────────┴──────────────────────────────────────────────────────────────┘
                  LowerOffset ────────────── free ──────────── UpperOffset
```

**Page Header (32 bytes):**

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | `PageID` |
| 8 | 2 | `PageType` |
| 10 | 2 | `SlotCount` |
| 12 | 2 | `LowerOffset` |
| 14 | 2 | `UpperOffset` |
| 16 | 2 | `FragBytes` |
| 18 | 4 | `CRC32` (of bytes 0–17) |
| 22 | 10 | reserved |

Each slot is 4 bytes: `RecordOffset(2) + RecordLen(2)`.

### Record Encoding

Values larger than `MaxChunkSize` (64 bytes) are split into a **linked chain** of chunks. Each chunk starts with a 1-byte flag:

| Flag | Hex | Meaning |
|---|---|---|
| `FlagHeadSingle` | `0x04` | Only chunk — contains both key and data |
| `FlagHead` | `0x01` | First chunk in a multi-chunk value — contains key |
| `FlagBody` | `0x02` | Middle chunk |
| `FlagTail` | `0x03` | Last chunk |
| `FlagTombstone` | `0xFF` | Deleted — space reclaimed by defragmentation |

After the flag byte, every chunk stores a `NextLoc` (8-byte PageID + 2-byte SlotID) pointing to the next chunk.

### Write-Ahead Log

Every mutating operation (`Upsert`, `Delete`, `Incr`) is written to `wal.bin` before modifying any page. The WAL is append-only; each entry carries a **CRC32 checksum** for torn-write detection and an **LSN** (Log Sequence Number) for ordering.

**WAL entry format:**

```
LSN(8) | Op(1) | KeyLen(2) | ValLen(4) | Key | Val | CRC32(4)
```

On startup, `replayWAL` reads every entry, verifies its CRC32, and re-applies it. Replay stops at the first checksum mismatch, providing partial-write tolerance.

**Safe checkpoint sequence (enforced in both Janitor and shutdown):**

```
1. pool.flushAll()   — write all dirty pages to the data file
2. fm.file.Sync()    — fsync — guarantee durability on disk hardware
3. wal.checkpoint()  — only now truncate the WAL
```

### Buffer Pool

The LRU buffer pool caches up to `MaxCachePages` (16 384) pages in memory (64 MB). Each cached page has a **pin count**; pages with a non-zero pin count cannot be evicted.

- **Cache hit** promotes the page to MRU and increments its pin count.
- **Eviction** fails with `ErrEvictionFailed` if a dirty page cannot be written to disk, preventing silent data loss.
- **Pool exhaustion** returns `ErrPoolExhausted` if every page is currently pinned — the operation is rejected cleanly, not silently corrupted.
- **Checksum** is verified on every `readPage` call; a mismatch returns `ErrChecksumMismatch`.

### Defragmentation

When `TotalPages` exceeds `DefragPageThreshold` (500 pages), the Janitor triggers a **shadow defragmentation**:

1. **Snapshot phase** (under `RLock`): copy all live key/value pairs to a temporary file.
2. **Swap phase** (under `Lock`): fsync the temp file, then `os.Rename` it over the original (atomic at the OS level).
3. Immediately checkpoint the WAL and update `activePageID` atomically.

A `defragRunning atomic.Bool` (CAS) prevents two concurrent defrag runs from racing on the same `.tmp` file.

---

## Performance Design

| Mechanism | Goal |
|---|---|
| `sync.Pool` for 12-byte request header | Eliminate heap allocation per request |
| `sync.Pool` for 11-byte response header | Eliminate heap allocation per response |
| `net.Buffers` (writev) for response | Reduce syscall count from 2 to 1 per response |
| LRU buffer pool (64 MB) | Keep hot pages in memory; avoid disk reads on every operation |
| Semaphore channel (100 slots) | Hard cap on goroutines — prevents goroutine explosion under load |
| `sync.RWMutex` | Multiple concurrent readers; writers are exclusive |
| Idle-cold Janitor | 0 CPU, 0 I/O when no operations arrive; 5-minute back-off |
| Rolling `SetDeadline` | Zombie connections evicted after 60 s idle — frees semaphore slot |
| `atomic.Uint64` counters | Lock-free reads of `totalOps` and `activePageID` |

---

## Security & Stability Guarantees

| Threat | Mitigation |
|---|---|
| **Memory exhaustion (DoS)** | `MaxValueSize` (10 MB) and `MaxKeySize` (64 B) enforced at TCP layer before any allocation |
| **Integer overflow in payload sum** | `keyLen + valLen` checked for overflow before `make` |
| **Malformed disk data / crash corruption** | CRC32 checked on every page read; `decodeRecord` bounds-checks every length field |
| **Wrong file / incompatible format** | Global header validates magic string `BSENGINE` and `FileVersion` at open |
| **Silent data corruption (hash collision)** | Index uses `map[string]Location` — no hash function involved |
| **Lost update on counter increment** | `Incr` holds a single `e.mu.Lock()` for the entire read-modify-write |
| **Crash before flush** | WAL replay on startup recovers all logged-but-not-flushed operations |
| **Race condition on active page pointer** | `activePageID` is `atomic.Uint64`; incremented and read atomically |
| **Concurrent defragmentation** | `defragRunning.CompareAndSwap(false, true)` — only one defrag at a time |
| **Zombie TCP connections** | `conn.SetDeadline` resets every request cycle (60 s idle timeout) |
| **Oversized connection storm** | Semaphore channel caps concurrent handlers at `MaxConn` (100) |
| **WaitGroup panic on shutdown** | `closing atomic.Bool` prevents `wg.Add` after `wg.Wait` has started |
| **Permissive file permissions** | Data and WAL files created with mode `0600` (owner only) |
| **Nil WAL in defrag temp engine** | `tmpEngine` calls `upsertLocked` directly — never touches `wal` field |
| **Dirty page silently discarded** | Eviction returns `ErrEvictionFailed` on disk write error |
| **Pool exceeding capacity** | Returns `ErrPoolExhausted` when all pages pinned — no unbounded growth |

---

## Limitations

The following are known architectural constraints — not bugs — acceptable for the current scope:

- **Single-writer throughput** — all writes serialised by one `sync.RWMutex`. A shard-based locking scheme would improve concurrent write throughput significantly.
- **In-memory index** — the entire key space must fit in RAM. A B-tree on-disk index would lift this constraint.
- **Recovery time** — startup performs a full `O(N)` disk scan (`recoverIndex`). A persistent index snapshot would reduce cold-start time to `O(1)`.
- **No TLS** — the TCP layer is unauthenticated plaintext. Run behind a TLS-terminating proxy (e.g. `nginx`, `Caddy`, `stunnel`) in untrusted environments.
- **Single data file** — no sharding across multiple files or directories.
- **No TTL / expiry** — keys are permanent until explicitly deleted.
- **No replication** — single-node only; no leader/follower or raft-based replication.

---

## Changelog

### v4.0.0 — Resource-Optimal Release

Additional hardening and efficiency improvements beyond the v3 audit.

- **Idle-cold Janitor**: detects zero-activity intervals and backs off to `JanitorIdleInterval` (5 minutes), eliminating all CPU and I/O overhead when the system is idle. Automatically returns to normal cadence on the next write.
- **`sync.Pool` buffer reuse**: `headerBufPool` (12 B) and `respBufPool` (11 B) eliminate per-request heap allocations on the hot path, reducing GC pressure under high concurrency.
- **Vectorised writes (`net.Buffers`)**: response header and body sent via a single `writev` syscall instead of two separate `Write` calls.
- **`readGlobalHeader` hardened**: now returns an explicit error if the file magic or version does not match — prevents the engine from operating against a foreign or future-format file.
- **`resolveEnv` uses `filepath.Dir`**: replaces a manual byte-scan loop with the standard library function for correct cross-platform path handling.
- **All `Close`/`Remove` return values explicitly handled**: assigned to `_` where intentionally discarded, making the intent clear vs. accidental omission.

### v3.0.0 — Hardened Production Release

All items from `audit_main_go_revisi.md` (evaluasi_2) resolved.

- `[B.2/C.1]` Full WAL replay on startup — crash recovery functions correctly
- `[B.1]` `activePageID` → `atomic.Uint64` — race between `insertChunk` and `runDefrag` eliminated
- `[B.6]` `defragRunning atomic.Bool` — prevents concurrent shadow defragmentation runs
- `[B.5]` `keyLen` validated at TCP layer before memory allocation; integer overflow guard added
- `[B.4]` Buffer pool eviction returns `ErrEvictionFailed`; `ErrPoolExhausted` when all pages pinned
- `[B.8]` `deleteRecord` validates `slotID` before touching the buffer
- `[B.10]` `upsertLocked` validates `len(value) > MaxValueSize`
- `[B.9]` `getRecord` and `deleteRecord` guard against `uint16` overflow via `MaxSlotsPerPage`
- `[B.11]` `encodeRecord` returns error and rejects chunks exceeding `MaxChunkSize`
- `[B.13]` `closing atomic.Bool` prevents `wg.Add` after `wg.Wait` starts
- `[B.3]` `recoverIndex` logs warning on duplicate HEAD records
- `[C.2]` All configuration moved to environment variables
- `[C.3]` `OpStats` opcode — returns `keys`, `totalOps`, `totalPages`

### v2.0.0 — Initial Revision

Addressed all 17 findings from `evaluasi_main_go.md` (19 of 22 fully resolved).

### v1.0.0 — Prototype

Initial implementation.

---

## License

MIT License — see [LICENSE](LICENSE) for the full text.

---

*BSEngine demonstrates production database internals (WAL, slotted pages, LRU buffer pool, shadow defrag) in a single, readable Go file. For high-throughput production workloads consider battle-tested engines such as [Pebble](https://github.com/cockroachdb/pebble), [BadgerDB](https://github.com/dgraph-io/badger), or [BoltDB](https://github.com/etcd-io/bbolt).*