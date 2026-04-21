package engine

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Snapshot struct {
	Data map[string][]byte
}

// writeSnapshotFile writes snapshot.gob under dataDir using temp+rename.
// No engine mutex — caller must not pass a map that is still being mutated.
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

func (e *Engine) truncateWAL() error {

	if e.wal == nil {
		return errors.New("wal is nil")
	}

	if err := e.wal.Sync(); err != nil {
		return fmt.Errorf("sync wal before truncate: %w", err)
	}

	if err := e.wal.file.Truncate(0); err != nil {
		return fmt.Errorf("truncate wal file: %w", err)
	}

	if _, err := e.wal.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek wal after truncate: %w", err)
	}

	e.wal.buf.Reset(e.wal.file)
	return nil
}
