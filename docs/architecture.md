# Durable KV Store Architecture

This document summarizes the implemented architecture and runtime behavior.

## Core Layers

- **Engine (`internal/engine`)**
  - In-memory map (`map[string][]byte`) guarded by `sync.RWMutex`.
  - Public operations: `Set`, `Get`, `Delete`, `Close`.
  - Metrics counters tracked with `sync/atomic`.
- **Durability**
  - Write-Ahead Log (`wal.log`) with CRC32-protected records.
  - Snapshot file (`snapshot.gob`) written with temp-file + atomic rename.
  - Compaction truncates/resets WAL after successful snapshot.
- **Recovery**
  - Startup order:
    1. Load snapshot if present.
    2. Replay WAL records.
    3. Apply valid prefix only; stop on truncated/corrupt tail.
- **Transport**
  - HTTP server (`internal/transport/http.go`) for REST-like API.
  - TCP server (`internal/transport/tcp.go`) using newline-delimited JSON messages.

## Durability Rule

WAL append happens before in-memory mutation for writes and deletes.

## Sync Policies

- `SyncNone`: highest throughput, least durability.
- `SyncAlways`: fsync for each append.
- `SyncPeriodic`: background ticker flush/sync using configured interval.

## Exposed Endpoints / Commands

- HTTP:
  - `PUT /keys/{key}`
  - `GET /keys/{key}`
  - `DELETE /keys/{key}`
  - `GET /health`
  - `GET /metrics`
- TCP JSON ops:
  - `set`, `get`, `delete`, `ping`
- CLI:
  - `cmd/cli` sends one TCP request and prints result.

## Runtime Processes

- `cmd/server`: HTTP server with graceful shutdown.
- `cmd/tcpserver`: TCP server with signal-based shutdown.

