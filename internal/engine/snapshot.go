package engine

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type Snapshot struct {
	Data map[string][]byte
}

func writeSnapshotFile(dataDir string, data map[string][]byte) error {

	snap := Snapshot{Data: data}
	finalPath := filepath.Join(dataDir, "snapshot.gob")
	tmpPath := filepath.Join(dataDir, "snapshot.tmp")

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create tmp snapshot: %w", err)
	}

	cleanupOnErr := func(cause error) error {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return cause
	}

	enc := gob.NewEncoder(f)
	if err := enc.Encode(&snap); err != nil {
		return cleanupOnErr(fmt.Errorf("encode snapshot: %w", err))
	}

	if err := f.Sync(); err != nil {
		return cleanupOnErr(fmt.Errorf("sync snapshot: %w", err))
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close tmp snapshot: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename snapshot: %w", err)
	}

	return nil
}

func (e *Engine) loadSnapshot() (map[string][]byte, error) {
	path := filepath.Join(e.config.DataDir, "snapshot.gob")
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return make(map[string][]byte), nil

		}
		return nil, fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close()

	var snap Snapshot
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}

	out := make(map[string][]byte, len(snap.Data))
	for k, v := range snap.Data {
		out[k] = bytes.Clone(v)
	}
	return out, nil
}

func (e *Engine) saveSnapshot() error {

	e.mu.RLock()
	cloned := make(map[string][]byte, len(e.index))
	for k, v := range e.index {
		cloned[k] = bytes.Clone(v)
	}
	e.mu.RUnlock()

	return writeSnapshotFile(e.config.DataDir, cloned)
}

// Returns a clone of the in-memory KV map for raft snapshot encoding.
func (e *Engine) SnapshotData() (map[string][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, ErrClosed
	}

	cloned := make(map[string][]byte, len(e.index))
	for k, v := range e.index {
		cloned[k] = bytes.Clone(v)
	}
	return cloned, nil
}

// Replaces the in-memory map, writes snapshot.gob, and truncates the WAL. Used when installing a raft snapshot from a peer.
func (e *Engine) RestoreSnapshot(data map[string][]byte) error {
	if e.wal == nil {
		return errors.New("wal is nil")
	}

	cloned := make(map[string][]byte, len(data))
	for k, v := range data {
		cloned[k] = bytes.Clone(v)
	}

	if err := writeSnapshotFile(e.config.DataDir, cloned); err != nil {
		return fmt.Errorf("restore snapshot: write file: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}

	e.index = cloned

	if err := e.truncateWAL(); err != nil {
		return fmt.Errorf("restore snapshot: truncate wal: %w", err)
	}
	return nil
}

func (e *Engine) snapshotAndCompact() error {

	if e.wal == nil {
		return errors.New("wal is nil")
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	cloned := make(map[string][]byte, len(e.index))
	for k, v := range e.index {
		cloned[k] = bytes.Clone(v)
	}
	e.mu.Unlock()

	if err := writeSnapshotFile(e.config.DataDir, cloned); err != nil {
		return fmt.Errorf("compact: write snapshot: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}

	if err := e.truncateWAL(); err != nil {
		return fmt.Errorf("compact: truncate wal: %w", err)
	}
	e.metrics.IncCompaction()
	return nil
}

func (e *Engine) truncateWAL() error {
	if e.wal == nil {
		return errors.New("wal is nil")
	}

	// Flush current buffered data first.
	if err := e.wal.Sync(); err != nil {
		return fmt.Errorf("sync wal before truncate: %w", err)
	}

	// Close old append handle (important on Windows).
	if err := e.wal.file.Close(); err != nil {
		return fmt.Errorf("close wal before truncate: %w", err)
	}

	// Reopen/truncate as a fresh file.
	f, err := os.OpenFile(e.wal.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("reopen truncated wal: %w", err)
	}

	e.wal.file = f
	e.wal.buf = bufio.NewWriter(f)
	return nil
}
