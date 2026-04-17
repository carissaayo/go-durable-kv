package engine

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

type Engine struct {
	config Config
	db     *os.File // or a more specific handle
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
	}

	return e, nil
}
