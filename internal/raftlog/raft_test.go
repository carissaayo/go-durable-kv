package raftlog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/carissaayo/go-durable-kv/internal/engine"
)

func frameSize(payloadLen int) int64 {
	return int64(lenSize + payloadLen + crcSize)
}

func TestAppend_ScanRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft.log")
	log, err := OpenRaftLog(path, engine.SyncNone)
	if err != nil {
		t.Fatalf("OpenRaftLog() error = %v", err)
	}

	off1, err := log.Append([]byte("alpha"))
	if err != nil {
		t.Fatalf("Append(alpha) error = %v", err)
	}
	if off1 != 0 {
		t.Fatalf("first offset = %d, want 0", off1)
	}

	off2, err := log.Append([]byte("beta"))
	if err != nil {
		t.Fatalf("Append(beta) error = %v", err)
	}
	if off2 != frameSize(len("alpha")) {
		t.Fatalf("second offset = %d, want %d", off2, frameSize(len("alpha")))
	}

	var got [][]byte
	if err := log.Scan(func(_ int64, payload []byte) error {
		got = append(got, bytes.Clone(payload))
		return nil
	}); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("scan count = %d, want 2", len(got))
	}
	if !bytes.Equal(got[0], []byte("alpha")) || !bytes.Equal(got[1], []byte("beta")) {
		t.Fatalf("scan payloads = %q, %q; want alpha, beta", got[0], got[1])
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestAppend_ReadAt(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft.log")
	log, err := OpenRaftLog(path, engine.SyncNone)
	if err != nil {
		t.Fatalf("OpenRaftLog() error = %v", err)
	}
	defer func() { _ = log.Close() }()

	off, err := log.Append([]byte("record"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	payload, next, err := log.ReadAt(off)
	if err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	if !bytes.Equal(payload, []byte("record")) {
		t.Fatalf("payload = %q, want %q", payload, []byte("record"))
	}
	if next != frameSize(len("record")) {
		t.Fatalf("next offset = %d, want %d", next, frameSize(len("record")))
	}
}

func TestAppend_SyncAlways_PersistsImmediately(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft.log")
	log, err := OpenRaftLog(path, engine.SyncAlways)
	if err != nil {
		t.Fatalf("OpenRaftLog() error = %v", err)
	}
	defer func() { _ = log.Close() }()

	if _, err := log.Append([]byte("durable")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() error = %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("raft.log empty after SyncAlways append")
	}
}

func TestOpen_RepairTruncatedTail(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "raft.log")

	log1, err := OpenRaftLog(path, engine.SyncAlways)
	if err != nil {
		t.Fatalf("OpenRaftLog #1 error = %v", err)
	}
	if _, err := log1.Append([]byte("good")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open for torn tail: %v", err)
	}
	if _, err := f.Write([]byte{0x00, 0x00, 0x00, 0x05, 'b'}); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close torn tail file: %v", err)
	}

	log2, err := OpenRaftLog(path, engine.SyncAlways)
	if err != nil {
		t.Fatalf("OpenRaftLog #2 error = %v", err)
	}
	defer func() { _ = log2.Close() }()

	var payloads [][]byte
	if err := log2.Scan(func(_ int64, payload []byte) error {
		payloads = append(payloads, bytes.Clone(payload))
		return nil
	}); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	if len(payloads) != 1 {
		t.Fatalf("record count = %d, want 1", len(payloads))
	}
	if !bytes.Equal(payloads[0], []byte("good")) {
		t.Fatalf("payload = %q, want %q", payloads[0], []byte("good"))
	}
}

func TestReopen_PreservesRecords(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft.log")

	log1, err := OpenRaftLog(path, engine.SyncAlways)
	if err != nil {
		t.Fatalf("OpenRaftLog #1 error = %v", err)
	}
	if _, err := log1.Append([]byte("one")); err != nil {
		t.Fatalf("Append(one) error = %v", err)
	}
	if _, err := log1.Append([]byte("two")); err != nil {
		t.Fatalf("Append(two) error = %v", err)
	}
	if err := log1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	log2, err := OpenRaftLog(path, engine.SyncAlways)
	if err != nil {
		t.Fatalf("OpenRaftLog #2 error = %v", err)
	}
	defer func() { _ = log2.Close() }()

	off, err := log2.Append([]byte("three"))
	if err != nil {
		t.Fatalf("Append(three) error = %v", err)
	}
	wantOff := frameSize(len("one")) + frameSize(len("two"))
	if off != wantOff {
		t.Fatalf("append offset = %d, want %d", off, wantOff)
	}

	var payloads [][]byte
	if err := log2.Scan(func(_ int64, payload []byte) error {
		payloads = append(payloads, bytes.Clone(payload))
		return nil
	}); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	want := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	if len(payloads) != len(want) {
		t.Fatalf("record count = %d, want %d", len(payloads), len(want))
	}
	for i := range want {
		if !bytes.Equal(payloads[i], want[i]) {
			t.Fatalf("payload[%d] = %q, want %q", i, payloads[i], want[i])
		}
	}
}

func TestAppend_InvalidPayload(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft.log")
	log, err := OpenRaftLog(path, engine.SyncNone)
	if err != nil {
		t.Fatalf("OpenRaftLog() error = %v", err)
	}
	defer func() { _ = log.Close() }()

	if _, err := log.Append(nil); err == nil {
		t.Fatal("Append(nil) error = nil, want error")
	}
	if _, err := log.Append([]byte{}); err == nil {
		t.Fatal("Append(empty) error = nil, want error")
	}
}

func TestReadAt_NegativeOffset(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft.log")
	log, err := OpenRaftLog(path, engine.SyncNone)
	if err != nil {
		t.Fatalf("OpenRaftLog() error = %v", err)
	}
	defer func() { _ = log.Close() }()

	if _, _, err := log.ReadAt(-1); err == nil {
		t.Fatal("ReadAt(-1) error = nil, want error")
	}
}

func TestSync_FlushesBufferedAppend(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft.log")
	log, err := OpenRaftLog(path, engine.SyncNone)
	if err != nil {
		t.Fatalf("OpenRaftLog() error = %v", err)
	}
	defer func() { _ = log.Close() }()

	if _, err := log.Append([]byte("buffered")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() before sync error = %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("file size before sync = %d, want 0", len(data))
	}

	if err := log.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile() after sync error = %v", err)
	}
	if len(data) == 0 {
		t.Fatal("file still empty after Sync()")
	}
}
