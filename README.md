# Durable KV Store — System Design & Architecture

**Tech Stack:** Go 1.21+ · stdlib only · TCP or HTTP transport

**Correctness Target:**
- Zero data loss on clean shutdown
- Crash recovery via WAL replay from last snapshot
- Concurrency: `sync.RWMutex` (race-detector clean)

---

## 1. System Overview

A key-value storage engine with full disk durability.

- Writes are first appended to a Write-Ahead Log (WAL) before being applied to memory.
- On restart, the engine replays the WAL (from last snapshot) to rebuild state.
- Snapshot + compaction keep replay time bounded.

### Current Implementation Status

- In-memory `Set` / `Get` / `Delete` with `sync.RWMutex`
- WAL append-before-apply with CRC32 record validation
- Recovery on startup: load snapshot then replay WAL
- Snapshot persisted via `snapshot.tmp` + atomic rename
- WAL compaction (truncate/reset) after snapshot
- Sync policies: `SyncNone`, `SyncAlways`, `SyncPeriodic` (ticker loop)
- HTTP endpoints: `/keys/{key}`, `/health`, `/metrics`
- Graceful shutdown in server (`Shutdown` + engine close)

> Constraint: stdlib-only (no external DB, no ORM)  
> Goal: Understand durability, correctness, and crash recovery at the storage layer

## 1.1 Request Flow

1. Client sends `Set` / `Delete` over TCP or HTTP
2. Engine acquires write lock
3. WAL record appended (op, key, value, CRC32)
4. `fsync` called based on policy
5. Operation applied to in-memory map
6. Success response returned
7. `Get` uses read lock and reads memory only
8. Background snapshot when WAL exceeds threshold

## 1.2 Component Overview

| Layer | Component | Technology | Purpose |
|---|---|---|---|
| Storage | WAL | os + bufio + encoding/binary | Append-only durability log |
| Storage | Snapshot | os + encoding/gob or json | Full checkpoint |
| Index | In-memory map | map[string][]byte + RWMutex | Fast reads/writes |
| Integrity | Checksum | hash/crc32 | Detect corruption |
| Transport | HTTP/TCP | net/http or net | Client interface |
| Observability | Metrics | sync/atomic + JSON endpoint | Runtime counters + replay stats |

---

## 2. Storage Engine

## 2.1 WAL Record Format

| Offset | Size | Field | Description |
|---|---|---|---|
| 0 | 1 | Op | 0x01 Set, 0x02 Delete |
| 1 | 4 | KeyLen | uint32 big-endian |
| 5 | 4 | ValLen | uint32 |
| 9 | N | Key | Raw key bytes |
| ... | M | Value | Raw value bytes |
| ... | 4 | CRC32 | Checksum |

```go
func (w *WAL) Append(op Op, key, val []byte) error {
    rec := encodeRecord(op, key, val)
    if _, err := w.buf.Write(rec); err != nil {
        return err
    }
    if w.syncPolicy == SyncAlways {
        return w.file.Sync()
    }
    return nil
}
```

## 2.2 Crash Recovery on Startup

1. Load snapshot if present
2. Open WAL
3. Replay records sequentially
4. Validate CRC32
5. Apply valid ops
6. Stop on corruption / partial tail write

Required test:

```bash
write -> kill process -> restart -> verify data
```

## 2.3 Snapshot & WAL Compaction

1. Acquire write lock
2. Write `snapshot.tmp`
3. fsync temp file
4. Rename temp -> snapshot
5. Release lock
6. Truncate or rotate WAL

## 2.4 Durability Guarantees

| Policy | Durability | Throughput |
|---|---|---|
| SyncAlways | Highest | Lowest |
| SyncPeriodic | Medium | Medium/High |
| SyncNone | Lowest | Highest |

---

## 3. Concurrency Model

Single `sync.RWMutex`

| Operation | Lock | Notes |
|---|---|---|
| Get | RLock | Concurrent reads |
| Set | Lock | Serialized writes |
| Delete | Lock | Serialized writes |
| Snapshot | Lock | Brief pause |
| Replay | Lock | Startup only |

---

## 4. Transport Layer

Keep transport thin. Only parse requests and call engine methods.

### Minimal HTTP API

| Method | Path | Response |
|---|---|---|
| GET | /keys/{key} | 200 or 404 |
| PUT | /keys/{key} | 204 |
| DELETE | /keys/{key} | 204 |
| GET | /health | 200 |
| GET | /metrics | 200 JSON |

---

## 5. Testing Strategy

### Unit Tests

- WAL encode/decode
- CRC mismatch handling
- Set/Get/Delete correctness
- Snapshot round-trip

### Restart / Crash Tests

- Write then restart
- Corrupt WAL tail
- Snapshot then restart
- `go test ./...`

---

## 6. Project Structure

```text
durable-kv/
├── cmd/
│   ├── server/main.go
│   └── cli/main.go
├── internal/
│   ├── engine/
│   │   ├── engine.go
│   │   ├── wal.go
│   │   ├── snapshot.go
│   │   ├── metrics.go
│   │   ├── engine_test.go
│   │   ├── replay_test.go
│   │   ├── snapshot_test.go
│   │   └── engine_bench_test.go
│   └── transport/
│       └── http.go
├── docs/
│   └── architecture.md
└── README.md
```

---

## 7. Implementation Phases

| Phase | Goal |
|---|---|
| 1 | In-memory KV |
| 2 | WAL |
| 3 | Recovery |
| 4 | Snapshot |
| 5 | Polish |

---

## Critical Design Rule

> WAL append MUST happen before updating the in-memory map.

---

## Agent Guidance Rules

### Reject if:

- Map updated before WAL append
- No CRC validation during replay
- Snapshot written without temp + rename
- Replay ignores corruption
- Missing locks on writes

### Ensure:

- Configurable fsync policy
- Restart tests exist
- Race detector passes
- Snapshot compacts WAL

---

## Stretch Goals

- Group commit batching
- Advanced metrics export formats
- Compaction stats endpoint
- CLI client
