package engine

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppend_WritesRecordToFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWAL(path, SyncNone)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.Append(OpSet, "k1", []byte("v1")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// SyncNone buffers writes; flush explicitly for this test.
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer f.Close()

	rec, err := decodeRecord(f)
	if err != nil {
		t.Fatalf("decodeRecord() error = %v", err)
	}

	if rec.Op != OpSet {
		t.Fatalf("record op = %v, want %v", rec.Op, OpSet)
	}
	if rec.Key != "k1" {
		t.Fatalf("record key = %q, want %q", rec.Key, "k1")
	}
	if !bytes.Equal(rec.Value, []byte("v1")) {
		t.Fatalf("record value = %q, want %q", rec.Value, []byte("v1"))
	}

	// Should be end-of-file after one record.
	_, err = decodeRecord(f)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("second decode error = %v, want io.EOF", err)
	}
}

func TestWALAppend_SyncAlways_PersistsImmediately(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWAL(path, SyncAlways)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer func() { _ = w.Close() }()

	if err := w.Append(OpSet, "k2", []byte("v2")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// With SyncAlways, append should flush+fsync before returning.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() error = %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("wal file is empty after SyncAlways append")
	}
}

func TestEngine_SetFailsWhenWALAppendFails_MapUnchanged(t *testing.T) {
	t.Parallel()

	e := &Engine{
		config: Config{
			MaxValueSize: 1 << 20,
		},
		index: map[string][]byte{
			"k": []byte("old"),
		},
		wal: nil, // forces Append failure path
	}

	err := e.Set("k", []byte("new"))
	if !errors.Is(err, ErrFailedToAppendToWAL) {
		t.Fatalf("Set() error = %v, want %v", err, ErrFailedToAppendToWAL)
	}

	got := e.index["k"]
	if !bytes.Equal(got, []byte("old")) {
		t.Fatalf("value mutated on WAL failure; got %q, want %q", got, []byte("old"))
	}
}

func TestEngine_DeleteFailsWhenWALAppendFails_MapUnchanged(t *testing.T) {
	t.Parallel()

	e := &Engine{
		config: Config{
			MaxValueSize: 1 << 20,
		},
		index: map[string][]byte{
			"k": []byte("v"),
		},
		wal: nil, // forces Append failure path
	}

	err := e.Delete("k")
	if !errors.Is(err, ErrFailedToAppendToWAL) {
		t.Fatalf("Delete() error = %v, want %v", err, ErrFailedToAppendToWAL)
	}

	got, ok := e.index["k"]
	if !ok {
		t.Fatalf("key deleted despite WAL append failure")
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("value changed on delete failure; got %q, want %q", got, []byte("v"))
	}
}
