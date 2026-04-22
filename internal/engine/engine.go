package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrClosed              = errors.New("engine is closed")
	ErrValueTooLarge       = errors.New("value exceeds MaxValueSize")
	ErrKeyTooLarge         = errors.New("key exceeds uint32 WAL limit")
	ErrFailedToAppendToWAL = errors.New("unable to append to WAL")
)

type Engine struct {
	config Config
	db     *os.File
	mu     sync.RWMutex
	index  map[string][]byte
	closed bool
	wal    *WAL
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func Open(cfg Config) (*Engine, error) {

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
		stopCh: make(chan struct{}),
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

	if cfg.SyncPolicy == SyncPeriodic {
		e.startSyncLoop()
	}
	return e, nil
}

func (e *Engine) Set(key string, value []byte) error {
	var shouldCompact bool

	e.mu.Lock()

	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}

	if int64(len(value)) > e.config.MaxValueSize {
		e.mu.Unlock()
		return ErrValueTooLarge
	}

	if err := e.wal.Append(OpSet, key, value); err != nil {
		e.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrFailedToAppendToWAL, err)
	}

	e.index[key] = bytes.Clone(value)

	if e.config.MaxWALSizeBytes > 0 && e.wal != nil && e.wal.file != nil {
		info, err := e.wal.file.Stat()
		if err != nil {
			e.mu.Unlock()
			return fmt.Errorf("stat wal: %w", err)
		}

		currentWALBytes := info.Size() + int64(e.wal.buf.Buffered())
		shouldCompact = currentWALBytes >= e.config.MaxWALSizeBytes
	}

	e.mu.Unlock()

	if shouldCompact {
		if err := e.snapshotAndCompact(); err != nil {
			return fmt.Errorf("auto-compact after set: %w", err)
		}
	}

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

	if e.closed {
		e.mu.Unlock()
		return nil
	}

	e.closed = true

	periodic := e.config.SyncPolicy == SyncPeriodic
	e.mu.Unlock()

	if periodic {
		e.stopSyncLoop()
	}

	e.mu.Lock()
	defer e.mu.Unlock()

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

func (e *Engine) startSyncLoop() {

	if e.config.SyncInterval <= 0 {
		e.config.SyncInterval = time.Second
	}

	e.wg.Add(1)
	go e.syncLoop()
}

func (e *Engine) syncLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.mu.Lock()
			if e.closed {
				e.mu.Unlock()
				return
			}
			if err := e.wal.Sync(); err != nil {
				fmt.Printf("periodic sync error: %v\n", err)
			}
			e.mu.Unlock()

		case <-e.stopCh:
			return
		}

	}
}

func (e *Engine) stopSyncLoop() {
	close(e.stopCh)
	e.wg.Wait()
}
