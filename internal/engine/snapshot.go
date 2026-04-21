package engine

import (
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
	clonedIndex := make(map[string][]byte, len(e.index))
	for k, v := range e.index {
		clonedIndex[k] = bytes.Clone(v)
	}
	e.mu.RUnlock()

	snap := Snapshot{Data: clonedIndex}
	finalPath := filepath.Join(e.config.DataDir, "snapshot.gob")
	tmpPath := filepath.Join(e.config.DataDir, "snapshot.tmp")

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
