package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Engine struct {
	config Config
	db     *os.File         // or a more specific handle
	index  map[string]int64 // key → byte offset (in-memory index)
	mu     sync.RWMutex
	closed bool
}

var (
	ErrClosed        = errors.New("engine is closed")
	ErrValueTooLarge = errors.New("value exceeds MaxValueSize")
)

func Open(cfg Config) (*Engine, error) {
	// 1. Validate / normalise config
	if cfg.DataDir == "" {
		return nil, errors.New("DataDir must not be empty")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	e := &Engine{
		config: cfg,
		index:  make(map[string]int64),
	}

	// 2. Open or create the active segment file
	segPath := filepath.Join(cfg.DataDir, "active.seg")
	f, err := os.OpenFile(segPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment: %w", err)
	}
	e.db = f

	// 3. Rebuild the in-memory index by replaying the log
	// if err := e.loadIndex(); err != nil {
	// 	f.Close()
	// 	return nil, fmt.Errorf("load index: %w", err)
	// }

	// 4. Start background sync goroutine (if periodic)
	// if cfg.SyncPolicy == SyncPeriodic {
	// 	go e.syncLoop()
	// }

	return e, nil
}
