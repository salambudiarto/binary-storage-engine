# BSEngine

> A custom embedded key-value storage engine with a binary TCP interface, written in Go.

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-production--ready-brightgreen.svg)]()

BSEngine is a from-scratch, single-file key-value store inspired by the internals of PostgreSQL and SQLite. It implements a full database storage stack — slotted pages, an LRU buffer pool, a Write-Ahead Log with crash replay, shadow defragmentation, and a persistent binary TCP protocol — in under 1 400 lines of idiomatic Go.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
  - [Run](#run)
  - [Configuration](#configuration)
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
| **Network** | Binary TCP protocol, persistent connections, 100-connection cap |
| **Observability** | Structured JSON logging (`log/slog`); `OpStats` health opcode |
| **Configuration** | Environment-variable driven with sensible defaults |

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                       TCP Layer                          │
│   handleConnection  ·  startTCPServer  ·  OpStats        │
└───────────────────────────┬──────────────────────────────┘
                            │  Upsert / View / Delete / Incr
┌───────────────────────────▼──────────────────────────────┐
│                     Engine Core                          │
│   index map[string]Location  ·  sync.RWMutex             │
│   upsertLocked / viewLocked / deleteChain                │
│   Incr (atomic read-modify-write)                        │
└────────────┬──────────────────────────┬──────────────────┘
             │                          │
┌────────────▼──────────┐  ┌────────────▼──────────────────┐
│      Buffer Pool      │  │       Write-Ahead Log         │
│  LRU · pin-count      │  │  append · checkpoint · replay │
│  eviction · flushAll  │  │  CRC32 per entry · fsync      │
└────────────┬──────────┘  └───────────────────────────────┘
             │
┌────────────▼──────────┐
│     File Manager      │
│  readPage · writePage │
│  CRC32 page verify    │
│  global header (p.0)  │
└───────────────────────┘
             │
        [ data.bin ]   [ wal.bin ]
```

**Startup sequence:**
1. `recoverIndex` — full disk scan, builds `map[string]Location`
2. `replayWAL` — re-applies any WAL entries not yet flushed to the data file

**Background Janitor** (every 30 s):
- `flushAll` dirty pages → `fsync` → truncate WAL (safe checkpoint order)
- Launches `runDefrag` via `defragRunning.CompareAndSwap` to avoid double-runs

---

## Getting Started

### Prerequisites

- Go 1.21 or later

### Build

```bash
go build -o bsengine main.go
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

The engine detects the `/data` directory automatically. If it does not exist, it falls back to the current working directory.

### Configuration

All settings can be overridden via environment variables. No config file is required.

| Variable | Default | Description |
|---|---|---|
| `BSENGINE_ADDR` | `:7070` | TCP listen address |
| `BSENGINE_DATA_PATH` | `/data/data.bin` | Path to the data file |
| `BSENGINE_WAL_PATH` | `/data/wal.bin` | Path to the WAL file |

**Example — custom port and paths:**

```bash
BSENGINE_ADDR=":9090" \
BSENGINE_DATA_PATH="/mnt/storage/db.bin" \
BSENGINE_WAL_PATH="/mnt/storage/wal.bin" \
./bsengine
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
| `ReqID` | 4 | Client-assigned request ID, echoed in the response |
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
| `OpStats` | `0x06` | empty | `keys(8) \| totalOps(8) \| totalPages(8)` |

### Status Codes

| Code | Hex | Meaning |
|---|---|---|
| `StatusOk` | `0x00` | Operation succeeded |
| `StatusNotFound` | `0x01` | Key does not exist |
| `StatusError` | `0x02` | Operation failed (see server logs) |

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

Atomically add `delta` (a signed 64-bit integer) to the stored value.
If the key does not exist, it is treated as `0`.

```
Request:  Magic | 0x04 | ReqID | keyLen | valLen=8 | key | delta_le64
Response: Magic | ReqID | status | dataLen=8 | new_value_le64
```

### Stats

Lightweight health check — no lock, no disk I/O.

```
Request:  Magic | 0x06 | ReqID | keyLen=1 | valLen=0 | "."
Response: Magic | ReqID | 0x00 | dataLen=24 | keys_le64 | totalOps_le64 | totalPages_le64
```

---

## Storage Internals

### Page Layout

Every page is exactly 4 096 bytes. The first `PageHeaderSize` (32) bytes form the header; the remaining space is divided between a **slot directory** growing downward from byte 32, and **record data** growing upward from byte 4 096.

```
┌─────────────────────────────────── 4096 bytes ──────────────────────────────────┐
│ PageHeader(32) │ Slot[0] Slot[1] … →      ← … record data … record data │ free │
└────────────────┴─────────────────────────────────────────────────────────────────┘
                  LowerOffset ──────────────────────────────────── UpperOffset
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
| 18 | 4 | `CRC32` checksum of bytes 0–17 |

Each slot is 4 bytes: `RecordOffset(2) + RecordLen(2)`.

### Record Encoding

Values larger than `MaxChunkSize` (64 bytes) are split into a **linked chain** of chunks. Each chunk starts with a 1-byte flag:

| Flag | Meaning |
|---|---|
| `0x04` (`HeadSingle`) | Only chunk — contains both key and data |
| `0x01` (`Head`) | First chunk in a multi-chunk value — contains key |
| `0x02` (`Body`) | Middle chunk |
| `0x03` (`Tail`) | Last chunk |
| `0xFF` (`Tombstone`) | Deleted — space is reclaimed by defragmentation |

After the flag byte, every chunk stores a `NextLoc` (8-byte PageID + 2-byte SlotID) pointing to the next chunk in the chain.

### Write-Ahead Log

Every mutating operation (`Upsert`, `Delete`, `Incr`) is written to `wal.bin` before modifying any page. The WAL is append-only and each entry carries a **CRC32 checksum** for torn-write detection.

**WAL entry format:**

```
LSN(8) | Op(1) | KeyLen(2) | ValLen(4) | Key | Val | CRC32(4)
```

On startup, `replayWAL` reads every entry, verifies its checksum, and re-applies it. Replay stops at the first checksum mismatch, providing partial-write tolerance.

**Safe checkpoint sequence (enforced in both Janitor and shutdown):**

```
1. pool.flushAll()   — write all dirty pages to the data file
2. fm.file.Sync()    — fsync — guarantee durability on disk
3. wal.checkpoint()  — only now truncate the WAL
```

### Buffer Pool

The LRU buffer pool caches up to `MaxCachePages` (16 384) pages in memory. Each cached page has a **pin count**; pages with a non-zero pin count cannot be evicted.

- **Eviction** fails with `ErrEvictionFailed` if a dirty page cannot be written to disk, preventing silent data loss.
- **Pool exhaustion** returns `ErrPoolExhausted` if every page is currently pinned.
- **Checksum** is verified on every `readPage` call; a mismatch returns `ErrChecksumMismatch`.

### Defragmentation

When `TotalPages` exceeds `DefragPageThreshold` (500 pages), the Janitor triggers a **shadow defragmentation**:

1. **Snapshot phase** (under `RLock`): copy all live key/value pairs to a temporary file.
2. **Swap phase** (under `Lock`): fsync the temp file, then `os.Rename` it over the original — an atomic OS-level operation.
3. Immediately checkpoint the WAL and update `activePageID`.

A `defragRunning` atomic flag prevents two concurrent defrag runs from racing on the same `.tmp` file.

---

## Security & Stability Guarantees

| Threat | Mitigation |
|---|---|
| **Memory exhaustion (DoS)** | `MaxValueSize` (10 MB) and `MaxKeySize` (64 B) enforced at the TCP layer before any allocation |
| **Integer overflow in payload sum** | `keyLen + valLen` is checked for overflow before `make` |
| **Malformed disk data / crash corruption** | CRC32 checked on every page read; `decodeRecord` bounds-checks every length field |
| **Silent data corruption (hash collision)** | Index uses `map[string]Location` — no hash function involved |
| **Lost update on counter increment** | `Incr` holds a single `e.mu.Lock()` for the entire read-modify-write |
| **Crash before flush** | WAL replay on startup recovers all logged-but-not-flushed operations |
| **Race condition on active page pointer** | `activePageID` is `atomic.Uint64`; incremented and read atomically |
| **Concurrent defragmentation** | `defragRunning.CompareAndSwap(false, true)` — only one defrag at a time |
| **Zombie TCP connections** | `conn.SetDeadline` resets every request cycle (60 s idle timeout) |
| **Oversized connection storm** | Semaphore channel caps concurrent handlers at `MaxConn` (100) |
| **WaitGroup panic on shutdown** | `closing atomic.Bool` prevents `wg.Add` after `wg.Wait` has started |
| **Permissive file permissions** | Data and WAL files created with mode `0600` (owner only) |
| **Panic on nil WAL in defrag** | `tmpEngine` in `runDefrag` calls `upsertLocked` directly — never touches `wal` field |

---

## Limitations

The following are known architectural constraints — not bugs — that are acceptable for the current scope and would be addressed in a future major version:

- **Single-writer throughput** — all writes are serialised by one `sync.RWMutex`. A shard-based locking scheme would improve concurrent write throughput.
- **In-memory index** — the entire key space must fit in RAM. A B-tree on-disk index would lift this constraint.
- **Recovery time** — startup performs a full `O(N)` disk scan (`recoverIndex`). A persistent index snapshot would reduce cold-start time.
- **No TLS** — the TCP layer is unauthenticated plaintext. Run behind a TLS-terminating proxy in untrusted environments.
- **Single data file** — there is no sharding across multiple files or directories.
- **No TTL / expiry** — keys are permanent until explicitly deleted.

---

## Changelog

### v3.0.0 — Hardened Production Release

All items from `audit_main_go_revisi.md` resolved.

**Critical fixes (🔴)**
- `[B.2/C.1]` Implemented full **WAL replay** on startup — crash recovery now functions correctly
- `[B.1]` `activePageID` changed to `atomic.Uint64` — eliminates race between `insertChunk` and `runDefrag`
- `[B.6]` Added `defragRunning atomic.Bool` — prevents concurrent shadow defragmentation runs

**Security / robustness (🟠)**
- `[B.5]` `keyLen` validated at the TCP layer before memory allocation; integer overflow guard added
- `[B.4]` Buffer pool eviction now returns `ErrEvictionFailed` on disk error instead of silently discarding dirty data; returns `ErrPoolExhausted` when all pages are pinned
- `[B.8]` `deleteRecord` validates `slotID` before touching the buffer
- `[B.10]` `upsertLocked` validates `len(value) > MaxValueSize`
- `[B.7]` `tmpEngine` in `runDefrag` documented as WAL-less by design; `upsertLocked` called directly

**Hardening (🟡)**
- `[B.9]` `getRecord` and `deleteRecord` guard against `uint16` overflow via `MaxSlotsPerPage` constant
- `[B.11]` `encodeRecord` now returns `([]byte, error)` and rejects chunks exceeding `MaxChunkSize`
- `[B.13]` `closing atomic.Bool` in `startTCPServer` prevents `wg.Add` after `wg.Wait` starts
- `[B.3]` `recoverIndex` logs a warning when duplicate HEAD records are found for the same key
- `[C.2]` All configuration (address, paths) moved to environment variables with documented defaults
- `[C.3]` `OpStats` opcode added — returns live `keys`, `totalOps`, and `totalPages` metrics

### v2.0.0 — Initial Revision

Addressed all 17 findings from `evaluasi_main_go.md` (19 of 22 fully resolved). See that document for details.

### v1.0.0 — Prototype

Initial implementation.

---

## License

MIT License — see [LICENSE](LICENSE) for the full text.

---

*BSEngine is a learning-grade storage engine that demonstrates production database internals in a single, readable Go file. It is suitable for embedded or low-traffic use cases. For high-throughput production workloads consider battle-tested engines such as [Pebble](https://github.com/cockroachdb/pebble), [BadgerDB](https://github.com/dgraph-io/badger), or [BoltDB](https://github.com/etcd-io/bbolt).*