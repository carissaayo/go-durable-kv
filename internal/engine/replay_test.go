package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReplay_RestoresSetAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.SyncPolicy = SyncAlways

	e1, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}

	if err := e1.Set("k1", []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer e2.Close()

	got, ok, err := e2.Get("k1")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !ok {
		t.Fatalf("expected key to exist after replay")
	}
	if string(got) != "v1" {
		t.Fatalf("value mismatch: got %q want %q", got, "v1")
	}
}

func TestReplay_RestoresDeleteAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.SyncPolicy = SyncAlways

	e1, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}

	if err := e1.Set("k1", []byte("v1")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := e1.Delete("k1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer e2.Close()

	_, ok, err := e2.Get("k1")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if ok {
		t.Fatalf("expected key to be deleted after replay")
	}
}

func TestReplay_LastWriteWinsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.SyncPolicy = SyncAlways

	e1, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}

	if err := e1.Set("k", []byte("v1")); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	if err := e1.Set("k", []byte("v2")); err != nil {
		t.Fatalf("Set v2: %v", err)
	}
	if err := e1.Set("k", []byte("v3")); err != nil {
		t.Fatalf("Set v3: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer e2.Close()

	got, ok, err := e2.Get("k")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if string(got) != "v3" {
		t.Fatalf("value mismatch: got %q want %q", got, "v3")
	}
}

func TestReplay_IgnoresTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.SyncPolicy = SyncAlways

	e1, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}

	if err := e1.Set("good", []byte("record")); err != nil {
		t.Fatalf("Set good: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// Append a partial/torn record tail manually.
	walPath := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open wal append: %v", err)
	}
	_, err = f.Write([]byte{byte(OpSet), 0x00, 0x00}) // intentionally incomplete header
	_ = f.Close()
	if err != nil {
		t.Fatalf("append partial tail: %v", err)
	}

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer e2.Close()

	got, ok, err := e2.Get("good")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if !ok {
		t.Fatalf("expected valid prefix record to survive replay")
	}
	if string(got) != "record" {
		t.Fatalf("value mismatch: got %q want %q", got, "record")
	}
}

func TestReplay_StopsOnCorruptTailCRC(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.SyncPolicy = SyncAlways

	e1, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}

	if err := e1.Set("keep", []byte("me")); err != nil {
		t.Fatalf("Set keep: %v", err)
	}
	if err := e1.Set("later", []byte("value")); err != nil {
		t.Fatalf("Set later: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// Corrupt the last byte (likely in the CRC of the last record).
	walPath := filepath.Join(dir, "wal.log")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("wal should not be empty")
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0o644); err != nil {
		t.Fatalf("write corrupted wal: %v", err)
	}

	e2, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer e2.Close()

	// At least the valid prefix should be present.
	got, ok, err := e2.Get("keep")
	if err != nil {
		t.Fatalf("Get keep: %v", err)
	}
	if !ok || string(got) != "me" {
		t.Fatalf("expected valid prefix to be replayed, got ok=%v val=%q", ok, got)
	}

	// The last record may not be applied because replay stops at corruption.
	_, ok, err = e2.Get("later")
	if err != nil {
		t.Fatalf("Get later: %v", err)
	}
	if ok {
		t.Fatalf("expected last (corrupted-tail) record to be skipped")
	}
}
