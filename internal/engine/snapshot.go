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
