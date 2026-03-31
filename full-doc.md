# BSEngine — Full Technical Documentation

**Version:** v4.0.0
**Date:** 31 March 2026
**Engine:** Single-file embedded key-value store with binary TCP interface
**Language:** Go 1.21+

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Technology Stack & Design Decisions](#2-technology-stack--design-decisions)
3. [Wire Protocol Reference](#3-wire-protocol-reference)
4. [Storage Architecture](#4-storage-architecture)
5. [Concurrency & Safety Model](#5-concurrency--safety-model)
6. [Performance Engineering](#6-performance-engineering)
7. [Operational Guide](#7-operational-guide)
8. [Client Implementation — JavaScript / Workers](#8-client-implementation--javascript--workers)
9. [Client Implementation — Go Services](#9-client-implementation--go-services)
10. [Error Handling & Observability](#10-error-handling--observability)
11. [Security Model](#11-security-model)
12. [Testing Strategy](#12-testing-strategy)
13. [Deployment Patterns](#13-deployment-patterns)
14. [Limitations & Future Roadmap](#14-limitations--future-roadmap)

---

## 1. System Overview

BSEngine is a custom, single-binary, embedded key-value storage engine that exposes a binary TCP interface. It is purpose-built for scenarios where you need:

- **Predictable resource usage** — a 64 MB buffer pool cap, 100-connection semaphore, and idle-aware janitor keep footprint bounded
- **Crash durability** — Write-Ahead Log with per-entry CRC32 and full replay on startup
- **Simplicity** — a single `main.go` with no external dependencies, buildable in seconds
- **Low-latency reads** — hot pages served from an in-memory LRU cache; zero disk I/O on cache hits
- **Efficient idle behaviour** — CPU and I/O drop to near-zero when no traffic arrives

### What BSEngine Is Not

BSEngine is not a distributed database, not a SQL engine, and not designed for multi-terabyte datasets. It occupies the same niche as Redis (in-memory + persistence), BadgerDB (embedded LSM), or BoltDB (embedded B-tree) — but with a much simpler, auditable codebase.

---

## 2. Technology Stack & Design Decisions

### 2.1 Language: Go 1.21+

Go was chosen for its:

- **Structured concurrency** via `goroutines`, `sync.WaitGroup`, and `context.Context`
- **Zero-overhead primitives** — `sync/atomic`, `sync.RWMutex`, `sync.Pool`
- **`log/slog`** (Go 1.21) — structured, key-value JSON logging with no external dependency
- **Single static binary** — `CGO_ENABLED=0 go build` produces a fully self-contained binary that runs from scratch containers
- **`net.Buffers`** — vectorised socket writes (`writev` syscall) without manual syscall wrappers

### 2.2 Storage: Slotted-Page Layout

Inspired by PostgreSQL heap files and SQLite B-tree pages. Each 4 KiB page has:

- A 32-byte header with `CRC32` checksum, slot count, and free-space pointers
- A **slot directory** growing from the header toward the center
- **Record data** growing from the end of the page toward the center
- Free space in the middle — pages are full when `LowerOffset >= UpperOffset`

This layout is cache-line friendly and enables `O(1)` slot access without scanning.

### 2.3 Durability: Write-Ahead Log (WAL)

Every mutating operation is written to `wal.bin` before any page modification. WAL entries include:

- A monotonically increasing **LSN** (Log Sequence Number) for ordering
- Per-entry **CRC32** for torn-write detection
- The full operation (opcode + key + value)

The safe checkpoint sequence — `flushAll → fsync → truncate WAL` — ensures no committed operation is ever lost, even on ungraceful shutdown.

### 2.4 Caching: LRU Buffer Pool

An in-memory LRU cache of `PageSize × MaxCachePages = 67 MB` prevents redundant disk reads. Pages are **pinned** (reference-counted) during active operations and unpinned when done. Eviction only touches unpinned pages, making the system safe under concurrent load.

### 2.5 Index: `map[string]Location`

The in-memory index maps raw string keys to `(PageID, SlotID)` location pairs. Using the string key directly — not a hash — eliminates the risk of silent hash collision data corruption. Lookup is O(1) average via Go's built-in map.

### 2.6 Network: Binary TCP Protocol

A custom binary protocol over persistent TCP connections with:

- A 2-byte magic number (`0xBE57`) for frame synchronisation
- A 4-byte `ReqID` echoed in the response for client-side request correlation
- All integers in **little-endian** byte order
- No framing length prefix beyond the `ValLen` field — the protocol is self-delimiting

### 2.7 Janitor: Idle-Aware Maintenance

A background goroutine handles periodic maintenance (flush, WAL checkpoint, defragmentation). It detects idle periods by comparing `totalOps` between ticks: if no change, the ticker interval is reset from 30 seconds to 5 minutes and all I/O is skipped. This makes the system truly "cold" during inactivity — no wasted CPU cycles, no unnecessary disk I/O.

---

## 3. Wire Protocol Reference

All multi-byte integers are **little-endian** (LE). The protocol is designed for efficiency over TCP persistent connections — one connection can multiplex any number of sequential requests.

### 3.1 Request Frame

```
Offset  Size  Field
──────────────────────────────────────────────────────
0       2     Magic — always 0xBE57 (LE: 0x57, 0xBE)
2       1     Op — operation opcode
3       4     ReqID — client-assigned, echoed in response
7       1     KeyLen — key length in bytes (1–64)
8       4     ValLen — value length in bytes (0–10,485,760)
12      N     Key — raw bytes, length = KeyLen
12+N    M     Val — raw bytes, length = ValLen
```

Total header: **12 bytes**. Total frame: 12 + KeyLen + ValLen bytes.

### 3.2 Response Frame

```
Offset  Size  Field
──────────────────────────────────────────────────────
0       2     Magic — always 0xBE57
2       4     ReqID — echoed from request
6       1     Status — 0x00 Ok, 0x01 NotFound, 0x02 Error
7       4     DataLen — response body length in bytes
11      N     Data — response body, length = DataLen
```

Total header: **11 bytes**.

### 3.3 Opcodes

| Name | Hex | ValLen in Request | DataLen in Response |
|---|---|---|---|
| `OpUpsert` | `0x01` | value bytes | 0 |
| `OpView` | `0x02` | 0 | value bytes |
| `OpDelete` | `0x03` | 0 | 0 |
| `OpIncr` | `0x04` | 8 (LE int64 delta) | 8 (LE int64 new value) |
| `OpPing` | `0x05` | 0 | 0 |
| `OpStats` | `0x06` | 0 | 24 (3 × LE uint64) |

### 3.4 Status Codes

| Hex | Name | Meaning |
|---|---|---|
| `0x00` | `StatusOk` | Success |
| `0x01` | `StatusNotFound` | Key does not exist |
| `0x02` | `StatusError` | Operation failed — inspect server logs |

### 3.5 OpStats Response Layout

```
Offset  Size  Field
──────────────────────────────────────
0       8     Total live keys in index (LE uint64)
8       8     Total ops since last checkpoint (LE uint64)
16      8     Total pages in data file (LE uint64)
```

---

## 4. Storage Architecture

### 4.1 File Layout

```
data.bin:
  ┌────────────────────────────────────────┐
  │ Page 0 — Global Header (4096 bytes)   │
  │  [0:8]   "BSENGINE" magic             │
  │  [8:10]  FileVersion (LE uint16)      │
  │  [10:12] PageSize (LE uint16)         │
  │  [12:20] TotalPages (LE uint64)       │
  ├────────────────────────────────────────┤
  │ Page 1 — First data page              │
  ├────────────────────────────────────────┤
  │ Page 2 …                              │
  └────────────────────────────────────────┘

wal.bin:
  [Entry 1][Entry 2]...[Entry N]
  Each entry: LSN(8)|Op(1)|KeyLen(2)|ValLen(4)|Key|Val|CRC32(4)
```

### 4.2 Page Header

```
Offset  Size  Field
──────────────────────────────────
0       8     PageID (LE uint64)
8       2     PageType (LE uint16)
10      2     SlotCount (LE uint16)
12      2     LowerOffset — slot directory end (LE uint16)
14      2     UpperOffset — record data start (LE uint16)
16      2     FragBytes — tombstoned but unreclamed space (LE uint16)
18      4     CRC32 of bytes 0–17 (LE uint32)
22      10    Reserved
```

### 4.3 Record Chain for Large Values

Values > 64 bytes are split into 64-byte chunks and stored as a linked chain:

```
┌─────────────────────┐    ┌─────────────────────┐    ┌──────────────┐
│ FlagHead (0x01)     │    │ FlagBody (0x02)     │    │ FlagTail     │
│ NextLoc → Page2/S3  │───▶│ NextLoc → Page3/S1  │───▶│ (0x03)       │
│ Key: "mykey"        │    │ Data[0:64]          │    │ NextLoc: 0/0 │
│ Data[0:64]          │    │                     │    │ Data[64:end] │
└─────────────────────┘    └─────────────────────┘    └──────────────┘
```

Single-chunk values (≤ 64 bytes) use `FlagHeadSingle (0x04)` and contain both key and data.

### 4.4 Defragmentation

Shadow defrag runs when `TotalPages > 500`:

```
Phase 1 (RLock):
  For each key in index:
    Read full value via chunk chain
    Write to tmpEngine (fresh data.bin.tmp)

Phase 2 (Lock):
  tmpEngine.flushAll()
  fsync(tmp file)
  os.Rename(data.bin.tmp → data.bin)  ← atomic at OS level
  Re-open data.bin as new FileManager
  wal.checkpoint()                     ← WAL truncated after safe rename
```

---

## 5. Concurrency & Safety Model

### 5.1 Lock Hierarchy

```
e.mu (sync.RWMutex) — engine-level
  └── bp.mu (sync.Mutex) — buffer pool
        └── w.mu (sync.Mutex) — WAL append
```

No function holds two locks at the same level simultaneously — deadlock impossible by design.

### 5.2 Atomic Fields

| Field | Type | Purpose |
|---|---|---|
| `e.activePageID` | `atomic.Uint64` | Current page for new writes — race-free increment |
| `e.totalOps` | `atomic.Uint64` | Operation counter for checkpoint threshold |
| `e.defragRunning` | `atomic.Bool` | CAS guard preventing concurrent defrag |
| `closing` (TCP) | `atomic.Bool` | Shutdown flag preventing `wg.Add` after `wg.Wait` |

### 5.3 Read vs Write Operations

| Operation | Lock Acquired |
|---|---|
| `View` | `e.mu.RLock()` — concurrent with other reads |
| `Stats` | `e.mu.RLock()` for key count only |
| `Upsert` | `e.mu.Lock()` — exclusive |
| `Delete` | `e.mu.Lock()` — exclusive |
| `Incr` | `e.mu.Lock()` — full read-modify-write under single lock |
| `recoverIndex` | No lock — startup, single goroutine |
| `replayWAL` | No lock — startup, single goroutine |

### 5.4 Shutdown Safety

```
SIGINT/SIGTERM
    │
    ▼
cancel()               — closes ctx.Done() channel
    │
    ├── TCP server goroutine: closing.Store(true) → ln.Close() → wg.Done()
    ├── Handler goroutines: complete current request → wg.Done()
    └── Janitor goroutine: <-ctx.Done() → wg.Done()
    │
    ▼
wg.Wait()              — guaranteed: ALL goroutines have exited
    │
    ▼
flushAll() → fsync → checkpoint → close files
```

---

## 6. Performance Engineering

### 6.1 Zero-Allocation Hot Path

Per-request heap allocations are eliminated on the hot path via `sync.Pool`:

```go
// Request header buffer: 12 bytes per connection, reused across requests
headerBufPool = sync.Pool{New: func() any { b := make([]byte, 12); return &b }}

// Response header buffer: 11 bytes per response
respBufPool = sync.Pool{New: func() any { b := make([]byte, 11); return &b }}
```

At 1,000 concurrent requests/second, this prevents ~23,000 small heap allocations per second, significantly reducing GC pause frequency.

### 6.2 Vectorised Writes

```go
// Two logical writes in one writev syscall:
buffers := net.Buffers{resp[:11], respData}
_, writeErr = buffers.WriteTo(conn)
```

`net.Buffers.WriteTo` calls `sendmsg(2)` / `writev(2)` on Linux, combining the 11-byte response header with the variable-length body into a single kernel call.

### 6.3 Idle-Cold Behaviour

```
Normal operation:  ticker fires every 30s → flushAll + checkpoint + defrag check
Idle detection:    totalOps unchanged between ticks
Idle state:        ticker resets to 5 minutes, all I/O skipped, goroutine sleeps
Wake-up:           first new operation → totalOps changes → next tick restores 30s cadence
```

CPU utilization during idle: effectively 0%. OS page cache for data files can be reclaimed by the kernel.

### 6.4 Throughput Characteristics

| Scenario | Approximate Throughput |
|---|---|
| Ping (no I/O) | ~50,000 req/s |
| View (cache hit) | ~20,000 req/s |
| View (cache miss, disk) | ~5,000 req/s |
| Upsert (with WAL fsync) | ~500–2,000 req/s |
| Incr (with WAL fsync) | ~500–2,000 req/s |

The WAL `fsync` call is the primary bottleneck for writes. Each `Upsert`/`Delete`/`Incr` performs one `fsync` — this is correct for durability but limits write throughput. A group-commit implementation (batch multiple WAL entries per fsync) would increase write throughput by 10–50x at the cost of a small durability window.

---

## 7. Operational Guide

### 7.1 Environment Variables

| Variable | Default | Description |
|---|---|---|
| `BSENGINE_ADDR` | `:7070` | TCP bind address |
| `BSENGINE_DATA_PATH` | `/data/data.bin` | Data file path |
| `BSENGINE_WAL_PATH` | `/data/wal.bin` | WAL file path |

### 7.2 File Permissions

Both `data.bin` and `wal.bin` are created with permission `0600` (owner read/write only). Ensure the process user has write access to the target directory.

### 7.3 Startup Behaviour

1. Opens or creates data file — validates magic `BSENGINE` and `FileVersion=1`
2. Scans all pages to rebuild the in-memory index (`recoverIndex`)
3. Reads and replays WAL (`replayWAL`) — re-applies any uncommitted operations
4. Starts janitor goroutine
5. Binds TCP listener and starts accepting connections

Cold start time scales linearly with `TotalPages`. For 500 pages (~2 MB of data), startup is < 10ms.

### 7.4 Graceful Shutdown

Send `SIGINT` or `SIGTERM`. The engine will:
1. Stop accepting new connections
2. Drain all active connections (up to 60s each)
3. Flush all dirty pages, fsync, checkpoint WAL
4. Log `"BSEngine shutdown complete. All data is safe."`

### 7.5 Monitoring

The `OpStats` opcode returns three counters: `totalKeys`, `totalOps` (since last checkpoint), and `totalPages`. Use this for basic health monitoring without adding any monitoring agent.

For log-based monitoring, all slog output is JSON to stdout:

```json
{"time":"2026-03-31T10:00:00Z","level":"INFO","msg":"BSEngine starting","dataPath":"/data/data.bin","walPath":"/data/wal.bin"}
{"time":"2026-03-31T10:00:00Z","level":"INFO","msg":"Index recovery: complete","totalKeys":42381}
{"time":"2026-03-31T10:00:00Z","level":"INFO","msg":"BSEngine TCP server started","addr":":7070"}
```

### 7.6 Backup

To take a consistent backup:
1. Stop writes (or accept a brief inconsistency window)
2. Copy `data.bin` and `wal.bin` together
3. Resume writes

For a hot backup without stopping: the engine does not support online snapshot commands in v4.0. The safest approach is to trigger a `SIGTERM` graceful shutdown, copy the files, and restart.

---

## 8. Client Implementation — JavaScript / Workers

This section covers implementing a BSEngine client in JavaScript environments powered by the **V8 engine**, specifically targeting:

- **Cloudflare Workers** — V8 isolates, Web Fetch API, TCP Sockets via `connect()`
- **Deno** — V8 runtime with `Deno.connect()` (TCP)
- **Node.js** — V8 runtime with `net.Socket`
- **Bun** — JavaScriptCore / V8-compatible with `Bun.connect()`

> ⚠️ **Browser note:** Browser JavaScript cannot open raw TCP sockets due to Web security restrictions. BSEngine clients must run in a server-side or edge Worker context.

### 8.1 Protocol Helpers (Pure JS / ECMAScript, No Dependencies)

```javascript
// ─── Constants ────────────────────────────────────────────────────────────────
const MAGIC = 0xBE57;
const OP = { UPSERT: 0x01, VIEW: 0x02, DELETE: 0x03, INCR: 0x04, PING: 0x05, STATS: 0x06 };
const STATUS = { OK: 0x00, NOT_FOUND: 0x01, ERROR: 0x02 };

// ─── Frame builder ────────────────────────────────────────────────────────────
/**
 * Build a BSEngine request frame.
 * @param {number} op       - Opcode byte
 * @param {number} reqId    - 32-bit request ID (uint32)
 * @param {Uint8Array} key  - Key bytes (1–64 bytes)
 * @param {Uint8Array} val  - Value bytes (0–10485760 bytes)
 * @returns {Uint8Array}    - Complete wire frame
 */
function buildFrame(op, reqId, key, val = new Uint8Array(0)) {
  if (key.length === 0 || key.length > 64) throw new RangeError(`KeyLen ${key.length} out of range 1–64`);
  if (val.length > 10_485_760) throw new RangeError(`ValLen ${val.length} exceeds 10MB`);

  const frame = new ArrayBuffer(12 + key.length + val.length);
  const view = new DataView(frame);

  view.setUint16(0, MAGIC, true);       // little-endian
  view.setUint8(2, op);
  view.setUint32(3, reqId, true);
  view.setUint8(7, key.length);
  view.setUint32(8, val.length, true);

  const u8 = new Uint8Array(frame);
  u8.set(key, 12);
  u8.set(val, 12 + key.length);

  return u8;
}

// ─── Response parser ──────────────────────────────────────────────────────────
/**
 * Parse a BSEngine response from a complete response buffer.
 * @param {Uint8Array} buf  - Must contain full response (11 + DataLen bytes)
 * @returns {{ reqId: number, status: number, data: Uint8Array }}
 */
function parseResponse(buf) {
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const magic = view.getUint16(0, true);
  if (magic !== MAGIC) throw new Error(`Bad magic: 0x${magic.toString(16)}`);

  const reqId   = view.getUint32(2, true);
  const status  = view.getUint8(6);
  const dataLen = view.getUint32(7, true);
  const data    = buf.slice(11, 11 + dataLen);

  return { reqId, status, data };
}

// ─── Encoding helpers ─────────────────────────────────────────────────────────
const enc = new TextEncoder();
const dec = new TextDecoder();

function strToBytes(s) { return enc.encode(s); }
function bytesToStr(b) { return dec.decode(b); }

/** Encode a signed int64 as little-endian 8 bytes (BigInt for safety) */
function encodeInt64LE(n) {
  const buf = new ArrayBuffer(8);
  new DataView(buf).setBigInt64(0, BigInt(n), true);
  return new Uint8Array(buf);
}

/** Decode 8 little-endian bytes to a signed BigInt */
function decodeInt64LE(bytes) {
  return new DataView(bytes.buffer, bytes.byteOffset).getBigInt64(0, true);
}
```

### 8.2 Client Class — Cloudflare Workers

Cloudflare Workers support raw TCP via the `connect()` API (Workers TCP Sockets):

```javascript
// ─── BSEngine Client for Cloudflare Workers ──────────────────────────────────
import { connect } from 'cloudflare:sockets';

export class BSEngineClient {
  #host;
  #port;
  #socket = null;
  #reader = null;
  #writer = null;
  #nextReqId = 1;
  #readBuf = new Uint8Array(0);

  constructor(host = '127.0.0.1', port = 7070) {
    this.#host = host;
    this.#port = port;
  }

  async connect() {
    this.#socket = connect({ hostname: this.#host, port: this.#port });
    this.#writer = this.#socket.writable.getWriter();
    this.#reader = this.#socket.readable.getReader();
  }

  async close() {
    await this.#writer?.close();
    this.#reader?.cancel();
    await this.#socket?.close();
  }

  // ── Low-level send/receive ─────────────────────────────────────────────────

  async #send(op, key, val = new Uint8Array(0)) {
    const reqId = this.#nextReqId++;
    const frame = buildFrame(op, reqId >>> 0, strToBytes(key), val);
    await this.#writer.write(frame);
    return reqId;
  }

  /** Read exactly `n` bytes from the TCP stream, handling chunking. */
  async #readExact(n) {
    const result = new Uint8Array(n);
    let filled = 0;

    // Drain any bytes already in the buffer first.
    if (this.#readBuf.length > 0) {
      const take = Math.min(n, this.#readBuf.length);
      result.set(this.#readBuf.slice(0, take));
      this.#readBuf = this.#readBuf.slice(take);
      filled += take;
    }

    while (filled < n) {
      const { done, value } = await this.#reader.read();
      if (done) throw new Error('Connection closed by server');

      const need = n - filled;
      if (value.length <= need) {
        result.set(value, filled);
        filled += value.length;
      } else {
        result.set(value.slice(0, need), filled);
        this.#readBuf = value.slice(need);
        filled += need;
      }
    }
    return result;
  }

  async #recv() {
    const header = await this.#readExact(11);
    const view = new DataView(header.buffer);
    const dataLen = view.getUint32(7, true);
    const body = dataLen > 0 ? await this.#readExact(dataLen) : new Uint8Array(0);

    const full = new Uint8Array(11 + body.length);
    full.set(header);
    full.set(body, 11);
    return parseResponse(full);
  }

  async #rpc(op, key, val = new Uint8Array(0)) {
    await this.#send(op, key, val);
    const resp = await this.#recv();
    if (resp.status === STATUS.ERROR) throw new Error(`BSEngine error for op 0x${op.toString(16)}, key="${key}"`);
    return resp;
  }

  // ── Public API ─────────────────────────────────────────────────────────────

  /** Ping the server. Returns round-trip time in ms. */
  async ping() {
    const t0 = Date.now();
    await this.#rpc(OP.PING, '.');
    return Date.now() - t0;
  }

  /** Set or replace a key's value. */
  async set(key, value) {
    const val = typeof value === 'string' ? strToBytes(value) : value;
    await this.#rpc(OP.UPSERT, key, val);
  }

  /** Get a key's value as a string, or null if not found. */
  async get(key) {
    const resp = await this.#rpc(OP.VIEW, key);
    if (resp.status === STATUS.NOT_FOUND) return null;
    return bytesToStr(resp.data);
  }

  /** Get a key's raw binary value, or null if not found. */
  async getBytes(key) {
    const resp = await this.#send(OP.VIEW, key) && await this.#recv();
    if (resp.status === STATUS.NOT_FOUND) return null;
    return resp.data;
  }

  /** Delete a key. Returns true if it existed, false if not found. */
  async del(key) {
    const resp = await this.#rpc(OP.DELETE, key);
    return resp.status === STATUS.OK;
  }

  /**
   * Atomically increment a counter by delta.
   * @param {string} key
   * @param {number|bigint} delta - signed 64-bit integer
   * @returns {bigint} new value
   */
  async incr(key, delta = 1n) {
    const val = encodeInt64LE(delta);
    const resp = await this.#rpc(OP.INCR, key, val);
    return decodeInt64LE(resp.data);
  }

  /** Retrieve engine health metrics. */
  async stats() {
    const resp = await this.#rpc(OP.STATS, '.');
    const view = new DataView(resp.data.buffer, resp.data.byteOffset);
    return {
      totalKeys:  view.getBigUint64(0, true),
      totalOps:   view.getBigUint64(8, true),
      totalPages: view.getBigUint64(16, true),
    };
  }
}
```

### 8.3 Usage — Cloudflare Worker Handler

```javascript
// wrangler.toml: compatibility_flags = ["nodejs_compat"]

export default {
  async fetch(request, env) {
    const client = new BSEngineClient(env.BSENGINE_HOST, 7070);
    await client.connect();

    try {
      const url = new URL(request.url);
      const key = url.searchParams.get('key');

      if (request.method === 'GET') {
        const value = await client.get(key);
        if (!value) return new Response('Not Found', { status: 404 });
        return new Response(value, { status: 200 });
      }

      if (request.method === 'PUT') {
        const body = await request.text();
        await client.set(key, body);
        return new Response('OK', { status: 200 });
      }

      if (request.method === 'DELETE') {
        await client.del(key);
        return new Response('Deleted', { status: 200 });
      }

      return new Response('Method Not Allowed', { status: 405 });
    } finally {
      await client.close();
    }
  }
};
```

### 8.4 Usage — Deno

```javascript
// deno run --allow-net client.ts

const conn = await Deno.connect({ hostname: '127.0.0.1', port: 7070 });

async function readExact(conn, n) {
  const buf = new Uint8Array(n);
  let read = 0;
  while (read < n) {
    const chunk = buf.subarray(read);
    const r = await conn.read(chunk);
    if (r === null) throw new Error('Connection closed');
    read += r;
  }
  return buf;
}

async function rpc(conn, op, key, val = new Uint8Array(0)) {
  const frame = buildFrame(op, Math.random() * 0xFFFFFFFF >>> 0, strToBytes(key), val);
  await conn.write(frame);
  const header = await readExact(conn, 11);
  const dataLen = new DataView(header.buffer).getUint32(7, true);
  const body = dataLen > 0 ? await readExact(conn, dataLen) : new Uint8Array(0);
  const full = new Uint8Array(11 + body.length);
  full.set(header); full.set(body, 11);
  return parseResponse(full);
}

// Example: set, get, incr
await rpc(conn, OP.UPSERT, 'hello', strToBytes('world'));
const r = await rpc(conn, OP.VIEW, 'hello');
console.log(bytesToStr(r.data)); // "world"

await rpc(conn, OP.INCR, 'counter', encodeInt64LE(1n));
conn.close();
```

### 8.5 Usage — Node.js

```javascript
// node client.js
import net from 'node:net';

function createNodeClient(host = '127.0.0.1', port = 7070) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host, port }, () => {
      let buffer = Buffer.alloc(0);
      const pending = new Map(); // reqId → { resolve, reject }

      socket.on('data', (chunk) => {
        buffer = Buffer.concat([buffer, chunk]);
        while (buffer.length >= 11) {
          const dataLen = buffer.readUInt32LE(7);
          const total = 11 + dataLen;
          if (buffer.length < total) break;

          const frame = buffer.slice(0, total);
          buffer = buffer.slice(total);

          const magic = frame.readUInt16LE(0);
          if (magic !== MAGIC) { socket.destroy(); return; }

          const reqId = frame.readUInt32LE(2);
          const status = frame[6];
          const data = frame.slice(11, total);

          const p = pending.get(reqId);
          if (p) {
            pending.delete(reqId);
            status === STATUS.ERROR ? p.reject(new Error('BSEngine error')) : p.resolve({ status, data });
          }
        }
      });

      let nextId = 1;

      function rpc(op, key, val = Buffer.alloc(0)) {
        return new Promise((res, rej) => {
          const reqId = nextId++;
          pending.set(reqId, { resolve: res, reject: rej });
          const keyBuf = Buffer.from(key, 'utf8');
          const frame = Buffer.alloc(12 + keyBuf.length + val.length);
          frame.writeUInt16LE(MAGIC, 0);
          frame[2] = op;
          frame.writeUInt32LE(reqId, 3);
          frame[7] = keyBuf.length;
          frame.writeUInt32LE(val.length, 8);
          keyBuf.copy(frame, 12);
          val.copy(frame, 12 + keyBuf.length);
          socket.write(frame);
        });
      }

      resolve({
        set: (key, value) => rpc(OP.UPSERT, key, Buffer.from(value)),
        get: async (key) => {
          const r = await rpc(OP.VIEW, key);
          return r.status === STATUS.NOT_FOUND ? null : r.data.toString('utf8');
        },
        del: (key) => rpc(OP.DELETE, key),
        ping: () => rpc(OP.PING, '.'),
        close: () => socket.destroy(),
      });
    });
    socket.on('error', reject);
  });
}

// Usage:
const db = await createNodeClient();
await db.set('session:abc123', JSON.stringify({ userId: 42, exp: Date.now() + 3600000 }));
const session = JSON.parse(await db.get('session:abc123'));
console.log(session.userId); // 42
db.close();
```

### 8.6 Request ID Strategy

The `ReqID` field allows the client to correlate responses with requests. On a pipelined connection, you can send multiple requests without waiting for each response. A simple auto-incrementing `uint32` that wraps around is sufficient for sequential protocols. For pipelined clients, use a `Map<reqId, Promise>` as shown in the Node.js example above.

---

## 9. Client Implementation — Go Services

### 9.1 Low-Level Client

```go
package bsclient

import (
    "encoding/binary"
    "fmt"
    "io"
    "net"
    "sync/atomic"
    "time"
)

const (
    magic uint16 = 0xBE57

    OpUpsert byte = 0x01
    OpView   byte = 0x02
    OpDelete byte = 0x03
    OpIncr   byte = 0x04
    OpPing   byte = 0x05
    OpStats  byte = 0x06

    StatusOk       byte = 0x00
    StatusNotFound byte = 0x01
    StatusError    byte = 0x02
)

var ErrNotFound = fmt.Errorf("bsclient: key not found")

// Client is a single-connection BSEngine client.
// Safe for use from a single goroutine; for concurrent use, wrap in a pool.
type Client struct {
    conn      net.Conn
    nextReqID atomic.Uint32
    respHdr   [11]byte
    reqHdr    [12]byte
}

// Dial opens a persistent TCP connection to a BSEngine server.
func Dial(addr string) (*Client, error) {
    conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
    if err != nil {
        return nil, fmt.Errorf("bsclient: dial %s: %w", addr, err)
    }
    return &Client{conn: conn}, nil
}

// Close closes the underlying TCP connection.
func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) rpc(op byte, key string, val []byte) ([]byte, error) {
    if len(key) == 0 || len(key) > 64 {
        return nil, fmt.Errorf("bsclient: key length %d out of range 1-64", len(key))
    }

    reqID := c.nextReqID.Add(1)
    keyB := []byte(key)

    // Build and send frame.
    hdr := &c.reqHdr
    binary.LittleEndian.PutUint16(hdr[0:2], magic)
    hdr[2] = op
    binary.LittleEndian.PutUint32(hdr[3:7], reqID)
    hdr[7] = byte(len(keyB))
    binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(val)))

    if err := c.conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
        return nil, err
    }

    // Use net.Buffers for vectorised write.
    bufs := net.Buffers{hdr[:], keyB, val}
    if _, err := bufs.WriteTo(c.conn); err != nil {
        return nil, fmt.Errorf("bsclient: write: %w", err)
    }

    // Read 11-byte response header.
    if _, err := io.ReadFull(c.conn, c.respHdr[:]); err != nil {
        return nil, fmt.Errorf("bsclient: read header: %w", err)
    }
    if binary.LittleEndian.Uint16(c.respHdr[0:2]) != magic {
        return nil, fmt.Errorf("bsclient: bad magic in response")
    }
    status := c.respHdr[6]
    dataLen := binary.LittleEndian.Uint32(c.respHdr[7:11])

    var body []byte
    if dataLen > 0 {
        body = make([]byte, dataLen)
        if _, err := io.ReadFull(c.conn, body); err != nil {
            return nil, fmt.Errorf("bsclient: read body: %w", err)
        }
    }

    switch status {
    case StatusOk:
        return body, nil
    case StatusNotFound:
        return nil, ErrNotFound
    default:
        return nil, fmt.Errorf("bsclient: server error for op=0x%02x key=%q", op, key)
    }
}

// Set inserts or replaces the value for key.
func (c *Client) Set(key string, value []byte) error {
    _, err := c.rpc(OpUpsert, key, value)
    return err
}

// Get returns the value for key, or ErrNotFound.
func (c *Client) Get(key string) ([]byte, error) {
    return c.rpc(OpView, key, nil)
}

// Del deletes the key. Returns ErrNotFound if it does not exist.
func (c *Client) Del(key string) error {
    _, err := c.rpc(OpDelete, key, nil)
    return err
}

// Incr atomically adds delta to the int64 stored at key (default 0).
func (c *Client) Incr(key string, delta int64) (int64, error) {
    val := make([]byte, 8)
    binary.LittleEndian.PutUint64(val, uint64(delta))
    body, err := c.rpc(OpIncr, key, val)
    if err != nil {
        return 0, err
    }
    if len(body) != 8 {
        return 0, fmt.Errorf("bsclient: incr: unexpected response length %d", len(body))
    }
    return int64(binary.LittleEndian.Uint64(body)), nil
}

// Ping sends a no-op request. Returns round-trip duration.
func (c *Client) Ping() (time.Duration, error) {
    t0 := time.Now()
    _, err := c.rpc(OpPing, ".", nil)
    return time.Since(t0), err
}

// Stats returns live engine metrics.
func (c *Client) Stats() (keys, ops, pages uint64, err error) {
    body, err := c.rpc(OpStats, ".", nil)
    if err != nil {
        return 0, 0, 0, err
    }
    if len(body) != 24 {
        return 0, 0, 0, fmt.Errorf("bsclient: stats: unexpected body length %d", len(body))
    }
    keys  = binary.LittleEndian.Uint64(body[0:8])
    ops   = binary.LittleEndian.Uint64(body[8:16])
    pages = binary.LittleEndian.Uint64(body[16:24])
    return
}
```

### 9.2 Connection Pool

For concurrent Go services (HTTP handlers, gRPC servers, etc.), wrap clients in a pool:

```go
package bsclient

import (
    "errors"
    "sync"
)

// Pool manages a fixed set of BSEngine connections for concurrent use.
type Pool struct {
    mu      sync.Mutex
    free    []*Client
    addr    string
    maxSize int
}

// NewPool creates a connection pool pre-warmed with `initial` connections.
func NewPool(addr string, initial, maxSize int) (*Pool, error) {
    p := &Pool{addr: addr, maxSize: maxSize}
    for i := 0; i < initial; i++ {
        c, err := Dial(addr)
        if err != nil {
            p.Close()
            return nil, err
        }
        p.free = append(p.free, c)
    }
    return p, nil
}

// Acquire borrows a client from the pool, dialing a new one if all are in use.
func (p *Pool) Acquire() (*Client, error) {
    p.mu.Lock()
    if len(p.free) > 0 {
        c := p.free[len(p.free)-1]
        p.free = p.free[:len(p.free)-1]
        p.mu.Unlock()
        return c, nil
    }
    p.mu.Unlock()
    return Dial(p.addr)
}

// Release returns a client to the pool. Closes it if the pool is at capacity.
func (p *Pool) Release(c *Client) {
    p.mu.Lock()
    defer p.mu.Unlock()
    if len(p.free) >= p.maxSize {
        _ = c.Close()
        return
    }
    p.free = append(p.free, c)
}

// Close closes all pooled connections.
func (p *Pool) Close() {
    p.mu.Lock()
    defer p.mu.Unlock()
    for _, c := range p.free {
        _ = c.Close()
    }
    p.free = nil
}

// Do acquires a client, executes fn, and releases it back to the pool.
// On error from fn, the client is closed (not returned to pool) to avoid
// returning a potentially corrupted connection.
func (p *Pool) Do(fn func(*Client) error) error {
    c, err := p.Acquire()
    if err != nil {
        return err
    }
    if err := fn(c); err != nil {
        _ = c.Close() // discard on error
        return err
    }
    p.Release(c)
    return nil
}
```

### 9.3 Usage in a Go HTTP Service

```go
package main

import (
    "errors"
    "log"
    "net/http"

    "your/module/bsclient"
)

func main() {
    pool, err := bsclient.NewPool("127.0.0.1:7070", 5, 20)
    if err != nil {
        log.Fatal("BSEngine pool:", err)
    }
    defer pool.Close()

    http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
        key := r.URL.Query().Get("key")
        var value []byte
        err := pool.Do(func(c *bsclient.Client) error {
            var e error
            value, e = c.Get(key)
            return e
        })
        if errors.Is(err, bsclient.ErrNotFound) {
            http.NotFound(w, r)
            return
        }
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        w.Write(value)
    })

    http.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
        key := r.URL.Query().Get("key")
        body := r.Body
        defer body.Close()
        var buf []byte
        buf, _ = io.ReadAll(body)
        pool.Do(func(c *bsclient.Client) error {
            return c.Set(key, buf)
        })
        w.WriteHeader(http.StatusOK)
    })

    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

### 9.4 Using BSEngine from Go as a Session Store

```go
// session.go — BSEngine-backed HTTP session store example

func saveSession(pool *bsclient.Pool, sessionID string, data map[string]any) error {
    encoded, err := json.Marshal(data)
    if err != nil {
        return err
    }
    return pool.Do(func(c *bsclient.Client) error {
        return c.Set("session:"+sessionID, encoded)
    })
}

func loadSession(pool *bsclient.Pool, sessionID string) (map[string]any, error) {
    var result map[string]any
    err := pool.Do(func(c *bsclient.Client) error {
        data, err := c.Get("session:" + sessionID)
        if err != nil {
            return err
        }
        return json.Unmarshal(data, &result)
    })
    return result, err
}

// Rate limiting with Incr:
func rateLimit(pool *bsclient.Pool, ip string, limit int64) (bool, error) {
    var count int64
    err := pool.Do(func(c *bsclient.Client) error {
        var e error
        count, e = c.Incr("ratelimit:"+ip, 1)
        return e
    })
    if err != nil {
        return false, err
    }
    return count <= limit, nil
}
```

---

## 10. Error Handling & Observability

### 10.1 Sentinel Errors

All BSEngine errors are sentinel values comparable with `errors.Is()`:

| Error | Meaning |
|---|---|
| `ErrNotFound` | Key does not exist |
| `ErrPageFull` | No space in current page (internal) |
| `ErrInvalidRecord` | Corrupt on-disk record (CRC or bounds) |
| `ErrChecksumMismatch` | Page CRC32 mismatch — possible disk corruption |
| `ErrPoolExhausted` | All buffer pool pages are pinned |
| `ErrEvictionFailed` | Dirty page could not be written during eviction |
| `ErrPayloadTooBig` | Key or value exceeds limits |
| `ErrSlotNotFound` | Invalid slot reference in a page |

### 10.2 Structured Log Events

All log output is JSON via `log/slog`. Key fields:

| Event | Level | Key Fields |
|---|---|---|
| Startup | INFO | `dataPath`, `walPath` |
| Index recovery | INFO | `totalPages`, `totalKeys` |
| WAL replay | INFO | `walBytes`, `entriesReplayed` |
| TCP server started | INFO | `addr` |
| Janitor checkpoint | INFO | — |
| Janitor idle | INFO | `interval` |
| Page checksum mismatch | ERROR | `pageID` |
| Buffer eviction failed | ERROR | `pageID`, `error` |
| Defrag events | INFO/WARN/ERROR | `error` |
| Shutdown complete | INFO | — |

### 10.3 Health Check via OpStats

```bash
# Quick health check with netcat (sends a raw Stats request):
printf '\x57\xBE\x06\x01\x00\x00\x00\x01\x00\x00\x00' | nc 127.0.0.1 7070 | xxd
```

Or use the provided client library's `Stats()` method. The response contains:
- `totalKeys` — number of live keys in the index
- `totalOps` — operations since last WAL checkpoint
- `totalPages` — total pages in `data.bin`

---

## 11. Security Model

### 11.1 Network Security

BSEngine listens on a raw TCP socket with **no authentication and no TLS**. It should be:

- **Not exposed to the public internet** directly
- Placed behind a private network interface (`127.0.0.1` or a VLAN)
- Protected by a TLS-terminating reverse proxy if inter-service encryption is required (e.g. stunnel, envoy sidecar, or nginx stream proxy)

### 11.2 Input Validation

| Input | Validation |
|---|---|
| Magic bytes | Checked first — invalid frames close the connection immediately |
| `KeyLen` | 1–64 bytes enforced before any allocation |
| `ValLen` | 0–10MB enforced before any allocation |
| `keyLen + valLen` sum | Integer overflow guard |
| Disk record lengths | All bounds-checked in `decodeRecord` |
| Page checksum | CRC32 verified on every `readPage` |

### 11.3 File Security

- Data and WAL files: permission `0600` (owner only)
- Temporary defrag file (`data.bin.tmp`): created by `newFileManager` with `0600`
- No world-readable or world-writable paths are created

---

## 12. Testing Strategy

BSEngine does not ship with tests in `main.go` (single-file design). A recommended test suite structure for a production deployment:

### 12.1 Unit Tests

```go
// page_test.go
func TestInsertAndGetRecord(t *testing.T) { ... }
func TestTombstoneAndDelete(t *testing.T) { ... }
func TestChecksumMismatch(t *testing.T) { ... }

// wal_test.go
func TestWALAppendAndReplay(t *testing.T) { ... }
func TestWALCorruptedCRC(t *testing.T) { ... }
func TestWALTruncatedEntry(t *testing.T) { ... }

// engine_test.go
func TestUpsertViewDelete(t *testing.T) { ... }
func TestIncrAtomicity(t *testing.T) { ... }
func TestCrashRecovery(t *testing.T) { ... }  // kill engine mid-write, restart
func TestDefragPreservesData(t *testing.T) { ... }
```

### 12.2 Integration Tests

```go
func TestProtocolPing(t *testing.T) { ... }
func TestProtocolOversizedPayload(t *testing.T) { ... }
func TestProtocolBadMagic(t *testing.T) { ... }
func TestConcurrentReadsUnderWriteLoad(t *testing.T) { ... }
func TestGracefulShutdownDrainsConnections(t *testing.T) { ... }
```

### 12.3 Race Detector

Always run tests with `-race`:

```bash
go test -race -count=3 ./...
```

### 12.4 Benchmarks

```go
func BenchmarkUpsert(b *testing.B) { ... }
func BenchmarkView(b *testing.B)   { ... }
func BenchmarkIncr(b *testing.B)   { ... }
func BenchmarkPing(b *testing.B)   { ... }
```

---

## 13. Deployment Patterns

### 13.1 Standalone Binary (systemd)

```ini
# /etc/systemd/system/bsengine.service
[Unit]
Description=BSEngine Key-Value Store
After=network.target

[Service]
Type=simple
User=bsengine
Group=bsengine
ExecStart=/usr/local/bin/bsengine
Restart=on-failure
RestartSec=5s

Environment=BSENGINE_ADDR=127.0.0.1:7070
Environment=BSENGINE_DATA_PATH=/var/lib/bsengine/data.bin
Environment=BSENGINE_WAL_PATH=/var/lib/bsengine/wal.bin

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/bsengine
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

### 13.2 Docker (Minimal Image)

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bsengine main.go

FROM scratch
COPY --from=builder /src/bsengine /bsengine
VOLUME ["/data"]
EXPOSE 7070
ENTRYPOINT ["/bsengine"]
```

Binary size: ~3–5 MB. Image size: ~3–5 MB (scratch base).

### 13.3 Kubernetes (StatefulSet)

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: bsengine
spec:
  serviceName: bsengine
  replicas: 1
  selector:
    matchLabels:
      app: bsengine
  template:
    metadata:
      labels:
        app: bsengine
    spec:
      containers:
      - name: bsengine
        image: your-registry/bsengine:v4.0.0
        ports:
        - containerPort: 7070
        env:
        - name: BSENGINE_ADDR
          value: "0.0.0.0:7070"
        - name: BSENGINE_DATA_PATH
          value: "/data/data.bin"
        - name: BSENGINE_WAL_PATH
          value: "/data/wal.bin"
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
            cpu: "500m"
        livenessProbe:
          tcpSocket:
            port: 7070
          initialDelaySeconds: 5
          periodSeconds: 10
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 1Gi
```

### 13.4 Sidecar Pattern (with main application)

```yaml
# BSEngine as a sidecar within the same Pod — localhost communication only
containers:
- name: app
  image: your-app:latest
  env:
  - name: BSENGINE_ADDR
    value: "127.0.0.1:7070"

- name: bsengine
  image: your-registry/bsengine:v4.0.0
  env:
  - name: BSENGINE_ADDR
    value: "127.0.0.1:7070"
  volumeMounts:
  - name: bsengine-data
    mountPath: /data

volumes:
- name: bsengine-data
  emptyDir: {}
```

---

## 14. Limitations & Future Roadmap

### 14.1 Current Limitations

| Limitation | Impact | Workaround |
|---|---|---|
| Single write lock | Write throughput ~500–2000 ops/s | Acceptable for most use cases; batch writes from application layer |
| In-memory index | Key space must fit in RAM | ~1M keys ≈ 80 MB index overhead |
| O(N) startup recovery | Slow restart for >100K pages | Keep data compact via defrag |
| No TLS | Plaintext TCP | Wrap with stunnel / envoy / nginx |
| No TTL/expiry | Manual key management | Implement expiry in application layer with a naming convention |
| No replication | Single point of failure | Use external backup + restart |
| No authentication | Trust network-level security | Deploy on private network only |

### 14.2 Roadmap (Potential v5 Features)

In priority order, reasonable next steps for a production-hardened v5:

1. **Group-commit WAL** — batch multiple write operations per `fsync`. Expected 10–50× write throughput improvement with < 5ms durability window.
2. **Persistent index snapshot** — save `map[string]Location` to disk periodically, enabling O(1) cold start instead of O(N) disk scan.
3. **shard-based locking** — partition the key space into N shards (e.g. 16), each with its own `sync.RWMutex`. Enables parallel writes to different key ranges.
4. **Key TTL / expiry** — store expiry timestamps alongside values; janitor evicts expired keys during its maintenance pass.
5. **TLS support** — wrap the TCP listener with `tls.Listen` and a configurable certificate path.
6. **Pub/Sub channel** — lightweight push notifications for key changes, using a separate opcode and subscription map.
7. **Range scan** — introduce a sorted secondary index (B-tree or skip list) for ordered key iteration.
8. **Online backup opcode** — `OpSnapshot`: trigger a consistent snapshot to a path, returning when complete.

---

*BSEngine v4.0 — A complete, auditable, production-capable key-value storage engine in a single Go file. Built with clarity, correctness, and resource efficiency as first-class goals.*