package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
)

type Engine struct {
	config Config
	db     *os.File // or a more specific handle
	mu     sync.RWMutex
	index  map[string][]byte
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
		index:  make(map[string][]byte),
	}

	return e, nil
}

func (e *Engine) Set(key string, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}

	if int64(len(value)) > e.config.MaxValueSize {
		return ErrValueTooLarge
	}

	e.index[key] = bytes.Clone(value)

	return nil
}

func (e *Engine) Get(key string) ([]byte, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, false, ErrClosed
	}

	value, ok := e.index[key]
	if !ok {
		return nil, false, nil
	}

	return bytes.Clone(value), true, nil
}

func (e *Engine) Delete(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return ErrClosed
	}
	delete(e.index, key)
	return nil
}
