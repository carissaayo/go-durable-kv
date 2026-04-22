package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshot_SaveAndLoad_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	e := &Engine{
		config: DefaultConfig(dir),
		index: map[string][]byte{
			"a": []byte("1"),
			"b": []byte("2"),
		},
	}

	if err := e.saveSnapshot(); err != nil {
		t.Fatalf("saveSnapshot() error = %v", err)
	}

	loaded, err := e.loadSnapshot()
	if err != nil {
		t.Fatalf("loadSnapshot() error = %v", err)
	}

	if string(loaded["a"]) != "1" || string(loaded["b"]) != "2" {
		t.Fatalf("loaded map mismatch: got=%v", loaded)
	}
}

func TestSnapshotAndCompact_TruncatesWAL_AndStateRecovers(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.SyncPolicy = SyncAlways
	cfg.MaxWALSizeBytes = 1 << 62 // disable auto-compact in Set for this test

	e1, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #1 error = %v", err)
	}

	if err := e1.Set("k1", []byte("v1")); err != nil {
		t.Fatalf("Set k1 error = %v", err)
	}
	if err := e1.Set("k2", []byte("v2")); err != nil {
		t.Fatalf("Set k2 error = %v", err)
	}

	if err := e1.snapshotAndCompact(); err != nil {
		t.Fatalf("snapshotAndCompact() error = %v", err)
	}

	walPath := filepath.Join(dir, "wal.log")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal error = %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("wal not truncated, size=%d", info.Size())
	}

	if err := e1.Close(); err != nil {
		t.Fatalf("Close #1 error = %v", err)
	}

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #2 error = %v", err)
	}
	defer e2.Close()

	v1, ok, err := e2.Get("k1")
	if err != nil || !ok || string(v1) != "v1" {
		t.Fatalf("k1 mismatch: ok=%v err=%v val=%q", ok, err, v1)
	}
	v2, ok, err := e2.Get("k2")
	if err != nil || !ok || string(v2) != "v2" {
		t.Fatalf("k2 mismatch: ok=%v err=%v val=%q", ok, err, v2)
	}
}

func TestOpen_LoadsSnapshot_ThenReplaysWALTail(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.SyncPolicy = SyncAlways
	cfg.MaxWALSizeBytes = 1 << 62 // disable auto-compact

	e1, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #1 error = %v", err)
	}

	// Baseline data
	if err := e1.Set("a", []byte("1")); err != nil {
		t.Fatalf("Set a error = %v", err)
	}
	if err := e1.Set("b", []byte("old")); err != nil {
		t.Fatalf("Set b old error = %v", err)
	}

	// Checkpoint baseline
	if err := e1.snapshotAndCompact(); err != nil {
		t.Fatalf("snapshotAndCompact error = %v", err)
	}

	// Tail writes after snapshot (must come from WAL replay)
	if err := e1.Set("b", []byte("new")); err != nil {
		t.Fatalf("Set b new error = %v", err)
	}
	if err := e1.Set("c", []byte("3")); err != nil {
		t.Fatalf("Set c error = %v", err)
	}

	if err := e1.Close(); err != nil {
		t.Fatalf("Close #1 error = %v", err)
	}

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #2 error = %v", err)
	}
	defer e2.Close()

	assertKV := func(key, want string) {
		t.Helper()
		got, ok, err := e2.Get(key)
		if err != nil {
			t.Fatalf("Get %q error = %v", key, err)
		}
		if !ok {
			t.Fatalf("Get %q not found", key)
		}
		if string(got) != want {
			t.Fatalf("Get %q = %q, want %q", key, got, want)
		}
	}

	assertKV("a", "1")
	assertKV("b", "new")
	assertKV("c", "3")
}
