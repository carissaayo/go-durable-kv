package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Engine struct {
	config Config
	db     *os.File // or a more specific handle
	mu     sync.RWMutex
	index  map[string][]byte
	closed bool
	wal    *WAL
}

var (
	ErrClosed              = errors.New("engine is closed")
	ErrValueTooLarge       = errors.New("value exceeds MaxValueSize")
	ErrKeyTooLarge         = errors.New("key exceeds uint32 WAL limit")
	ErrFailedToAppendToWAL = errors.New("unable to append to WAL")
)

func Open(cfg Config) (*Engine, error) {
	// 1. Validate / normalise config
	if cfg.DataDir == "" {
		return nil, errors.New("DataDir must not be empty")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	walPath := filepath.Join(cfg.DataDir, "wal.log")

	wal, err := OpenWAL(walPath, cfg.SyncPolicy)
	if err != nil {
		return nil, fmt.Errorf("opening WAL: %w", err)
	}

	e := &Engine{
		config: cfg,
		index:  make(map[string][]byte),
		wal:    wal,
	}

	snap, err := e.loadSnapshot()
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("loading snapshot: %w", err)
	}

	e.index = snap

	if err := e.wal.Replay(func(rec *Record) error {
		switch rec.Op {
		case OpSet:
			e.index[rec.Key] = bytes.Clone(rec.Value)
		case OpDelete:
			delete(e.index, rec.Key)
		default:
			return ErrUnknownOp
		}
		return nil
	}); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("replay wal: %w", err)
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

	if err := e.wal.Append(OpSet, key, value); err != nil {
		return ErrFailedToAppendToWAL
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

	if err := e.wal.Append(OpDelete, key, nil); err != nil {
		return fmt.Errorf("%w: %v", ErrFailedToAppendToWAL, err)
	}
	delete(e.index, key)
	return nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil // idempotent close
	}

	e.closed = true

	if e.wal != nil {
		if err := e.wal.Close(); err != nil {
			return err

		}

	}

	if e.db != nil {
		if err := e.db.Close(); err != nil {
			return err
		}
		e.db = nil
	}

	return nil
}
