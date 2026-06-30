# Durable KV Store — System Design & Architecture

**Tech Stack:** Go 1.21+ · stdlib only · TCP or HTTP transport

**Module:** `github.com/carissaayo/go-durable-kv`

**Correctness Target:**
- Zero data loss on clean shutdown
- Crash recovery via WAL replay from last snapshot
- Concurrency: `sync.RWMutex` (race-detector clean)

This repo is both a **standalone durable KV server** and a **library** for distributed replicas (e.g. [go-kv-dist](https://github.com/carissaayo/go-kv-dist)): the KV state machine (`Engine`) and a separate consensus log (`RaftLog`) are intentionally split.

---

## 1. System Overview

A key-value storage engine with full disk durability.

- Writes are first appended to a Write-Ahead Log (WAL) before being applied to memory.
- On restart, the engine replays the WAL (from last snapshot) to rebuild state.
- Snapshot + compaction keep replay time bounded.

### Current Implementation Status

**KV engine (`internal/engine`)**
- In-memory `Set` / `Get` / `Delete` with `sync.RWMutex`
- WAL append-before-apply with CRC32 record validation
- Recovery on startup: load snapshot then replay WAL
- Snapshot persisted via `snapshot.tmp` + atomic rename
- WAL compaction (truncate/reset) after snapshot
- `SnapshotData()` / `RestoreSnapshot()` for raft snapshot install (used by importers)
- Sync policies: `SyncNone`, `SyncAlways`, `SyncPeriodic` (ticker loop)
- HTTP endpoints: `/keys/{key}`, `/health`, `/metrics`
- Graceful shutdown in server (`Shutdown` + engine close)

**Consensus log (`internal/raftlog`)**
- Append-only `raft.log` with CRC-framed opaque payloads (for serialized `raftpb.Entry`)
- Tail repair on open (truncates torn writes after crash)
- `Append`, `Scan`, `ReadAt`, `Sync`, `Close`
- `Truncate` / `DropPrefix` for log compaction after raft snapshots
- Tests: round-trip, torn tail repair, `SyncAlways` durability

**Public API (`pkg/`)** — for external modules that cannot import `internal/`:
- `pkg/engine` — KV state machine (`Open`, `Set`, `Get`, `Delete`, `SnapshotData`, `RestoreSnapshot`)
- `pkg/raftlog` — consensus byte log (`OpenRaftLog`, `Truncate`, …)

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
| Storage | WAL (`wal.log`) | os + bufio + encoding/binary | KV durability — Set/Delete before map update |
| Storage | RaftLog (`raft.log`) | os + bufio + crc32 | Consensus durability — opaque raft entries |
| Storage | Snapshot | os + encoding/gob | Full KV checkpoint (`snapshot.gob`) |
| Index | In-memory map | map[string][]byte + RWMutex | Fast reads/writes |
| Integrity | Checksum | hash/crc32 | Detect corruption (WAL + raft log) |
| Transport | HTTP/TCP | net/http or net | Client interface |
| Observability | Metrics | sync/atomic + JSON endpoint | Runtime counters + replay stats |

### 1.3 Dual persistence (library consumers)

When embedded in a Raft cluster, each node typically stores **two logs** under `--data`:

| File | Component | Contents |
|---|---|---|
| `raft.log` | `RaftLog` | CRC-framed serialized `raftpb.Entry` records (consensus) |
| `wal.log` | `Engine` | KV `Set` / `Delete` records (state machine; written only after commit + apply) |
| `snapshot.gob` | `Engine` | KV map checkpoint |

Raft metadata (`HardState`, `ConfState`, index maps) lives in the **consumer** (e.g. go-kv-dist `raft_meta`), not in this repo.

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

### Raft snapshot hooks

For distributed replicas, the engine exposes explicit snapshot helpers (also available via `pkg/engine`):

```go
// Export current KV map (clone) for encoding into raftpb.Snapshot.Data
data, err := e.SnapshotData()

// Install state from a peer snapshot: replace map, write snapshot.gob, truncate wal.log
err = e.RestoreSnapshot(data)
```

`RestoreSnapshot` does **not** append to the WAL — it replaces state wholesale, same as local compaction after a full checkpoint.

## 2.4 RaftLog record format

Each record in `raft.log` is length-prefixed and checksummed (opaque bytes — no KV keys):

| Part | Size | Description |
|---|---|---|
| Length | 4 | uint32 big-endian payload length |
| Payload | N | Opaque bytes (e.g. `proto.Marshal(raftpb.Entry)`) |
| CRC32 | 4 | IEEE CRC over length + payload |

On open, `repairTail` walks from byte 0 and truncates any partial tail record left by a crash.

```go
log, _ := raftlog.OpenRaftLog(path, raftlog.SyncAlways)
off, _ := log.Append(marshaledEntry)  // returns byte offset of record start
log.Scan(func(off int64, payload []byte) error { /* rebuild index */ return nil })
payload, next, _ := log.ReadAt(off)    // random access by offset
```

## 2.5 Durability Guarantees

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
- RaftLog append/scan/read, torn tail repair
- `SnapshotData` / `RestoreSnapshot`

### Restart / Crash Tests

- Write then restart
- Corrupt WAL tail
- Corrupt raft log tail
- Snapshot then restart
- `go test ./...`

---

## 6. Using as a library

Import the public packages (not `internal/`):

```go
import (
    "github.com/carissaayo/go-durable-kv/pkg/engine"
    "github.com/carissaayo/go-durable-kv/pkg/raftlog"
)

// KV state machine
cfg := engine.DefaultConfig("./data/node1")
e, err := engine.Open(cfg)
// e.Set / e.Get / e.Delete — only after raft commit in distributed mode

// Consensus log (separate file)
log, err := raftlog.OpenRaftLog("./data/node1/raft.log", raftlog.SyncAlways)
```

While developing both repos locally:

```go
// go.mod in go-kv-dist
require github.com/carissaayo/go-durable-kv v0.x.x
replace github.com/carissaayo/go-durable-kv => ../go-kv-store
```

---

## 7. Project Structure

```text
go-kv-store/
├── cmd/
│   ├── server/main.go       # HTTP server
│   ├── tcpserver/main.go    # TCP server
│   └── cli/main.go          # CLI client
├── internal/
│   ├── engine/              # KV state machine (wal.log + snapshot.gob)
│   │   ├── engine.go
│   │   ├── wal.go
│   │   ├── snapshot.go
│   │   ├── metrics.go
│   │   └── *_test.go
│   ├── raftlog/             # Consensus append-only log (raft.log)
│   │   ├── raft.go
│   │   └── raft_test.go
│   └── transport/
│       ├── http.go
│       └── tcp.go
├── pkg/                     # Public API for importers (go-kv-dist)
│   ├── engine/
│   └── raftlog/
├── docs/
│   └── architecture.md
├── go.mod
└── README.md
```

---

## 8. Implementation Phases

| Phase | Goal |
|---|---|
| 1 | In-memory KV |
| 2 | WAL |
| 3 | Recovery |
| 4 | Snapshot |
| 5 | Polish (HTTP/TCP, metrics, CLI) |
| 6 | RaftLog + `pkg/` exports for distributed replicas |
| 7 | Raft snapshot hooks (`SnapshotData`, `RestoreSnapshot`) |

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
