package raftlog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/carissaayo/go-durable-kv/internal/engine"
)

var errTornRecord = errors.New("raftlog: torn or corrupt record")

type RaftLog struct {
	mu         sync.Mutex
	file       *os.File
	buf        *bufio.Writer
	path       string
	syncPolicy engine.SyncPolicy

	offset int64 // byte offset of the next Append (= end of last valid record)
}

const (
	lenSize    = 4
	crcSize    = 4
	maxPayload = 64 << 20 // 64 MiB
)

// OpenRaftLog opens (or creates) the log file at path, performs tail repair to remove any partial write left by a previous crash, and positions the write cursor at the first byte after the last valid record.
func OpenRaftLog(path string, syncPolicy engine.SyncPolicy) (*RaftLog, error) {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft dir %q: %w", dir, err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("raftlog: open %q: %w", path, err)
	}

	// Walk every record to find the last clean boundary.
	lastGood, err := repairTail(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("raftlog: tail repair: %w", err)
	}

	// Remove the bytes left by a partial write. On a clean file lastGood == file size,
	if err := f.Truncate(lastGood); err != nil {
		f.Close()
		return nil, fmt.Errorf("raftlog: truncate to %d: %w", lastGood, err)
	}

	// Park the write cursor at the end of the last valid record.
	if _, err := f.Seek(lastGood, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("raftlog: seek to %d: %w", lastGood, err)
	}

	return &RaftLog{
		file:       f,
		buf:        bufio.NewWriter(f),
		path:       path,
		syncPolicy: syncPolicy,
		offset:     lastGood,
	}, nil

}

// repairTail walks the file from byte 0, record by record, and returns the byte offset immediately after the last complete, checksum-verified record.
func repairTail(f *os.File) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	r := bufio.NewReader(f)

	var lastGood int64
	for {
		_, frameSize, err := readFrame(r)
		if errors.Is(err, errTornRecord) {
			return lastGood, nil // clean end or torn tail — stop here
		}
		if err != nil {
			return 0, err // real I/O error
		}
		lastGood += frameSize
	}
}

// Append encodes payload as a length-prefixed, checksummed record, writes it to the log, and returns the byte offset at which the record starts.
func (l *RaftLog) Append(payload []byte) (offset int64, err error) {

	l.mu.Lock()
	defer l.mu.Unlock()

	// Payload guards
	if l.file == nil || l.buf == nil {
		return 0, errors.New("raftlog not initialized")
	}
	if len(payload) == 0 || len(payload) > maxPayload {
		return 0, errors.New("invalid payload size")
	}

	startOffset := l.offset

	// build the record
	var lenBuf [lenSize]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))

	h := crc32.NewIEEE()
	h.Write(lenBuf[:])
	h.Write(payload)

	var crcBuf [crcSize]byte
	binary.BigEndian.PutUint32(crcBuf[:], h.Sum32())

	if _, err := l.buf.Write(lenBuf[:]); err != nil {
		return 0, fmt.Errorf("raftlog write length: %w", err)
	}
	if _, err := l.buf.Write(payload); err != nil {
		return 0, fmt.Errorf("raftlog write payload: %w", err)
	}
	if _, err := l.buf.Write(crcBuf[:]); err != nil {
		return 0, fmt.Errorf("raftlog write crc: %w", err)
	}

	if l.syncPolicy == engine.SyncAlways {
		if err := l.syncLocked(); err != nil {
			return 0, fmt.Errorf("raftlog sync: %w", err)
		}
	}

	recordSize := int64(lenSize) + int64(len(payload)) + int64(crcSize)
	l.offset = startOffset + recordSize

	return startOffset, nil
}

// Walks every complete record from byte 0 (for index rebuild on startup). Flushes buffered writes first so in-process appends are visible.
func (l *RaftLog) Scan(apply func(offset int64, payload []byte) error) error {

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.path == "" || l.file == nil {
		return errors.New("raftlog not initialized")
	}

	if err := l.buf.Flush(); err != nil {
		return fmt.Errorf("raftlog: flush before scan: %w", err)
	}

	f, err := os.Open(l.path)
	if err != nil {
		return fmt.Errorf("raftlog: open for scan: %w", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var currentOffset int64

	for {
		payload, frameSize, err := readFrame(r)

		if errors.Is(err, errTornRecord) {
			return nil // clean stop at tail (same as repairTail)
		}

		if err != nil {
			return fmt.Errorf("raftlog: scan: %w", err)
		}

		recordStart := currentOffset
		currentOffset += frameSize

		if err := apply(recordStart, payload); err != nil {
			return err
		}
	}
}

// Reads one record at the given byte offset (from Append or Scan).
func (l *RaftLog) ReadAt(offset int64) (payload []byte, nextOffset int64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.path == "" || l.file == nil {
		return nil, 0, errors.New("raftlog not initialized")
	}

	if offset < 0 {
		return nil, 0, errors.New("raftlog: negative offset")
	}

	if err := l.buf.Flush(); err != nil {
		return nil, 0, fmt.Errorf("raftlog: flush before read: %w", err)
	}

	f, err := os.Open(l.path)
	if err != nil {
		return nil, 0, fmt.Errorf("raftlog: open for read: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("raftlog: seek to %d: %w", offset, err)
	}

	raw, frameSize, err := readFrame(bufio.NewReader(f))
	if err != nil {
		return nil, 0, fmt.Errorf("raftlog: read at %d: %w", offset, err)
	}

	return bytes.Clone(raw), offset + frameSize, nil
}

// syncLocked flushes and fsyncs. Caller must hold l.mu.
func (l *RaftLog) syncLocked() error {
	if l.buf == nil || l.file == nil {
		return errors.New("raftlog not initialized")
	}
	if err := l.buf.Flush(); err != nil {
		return err
	}
	return l.file.Sync()
}

func (l *RaftLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.syncLocked()
}

func (l *RaftLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.syncLocked(); err != nil {
		return err
	}
	return l.file.Close()
}

func readFrame(r io.Reader) (payload []byte, frameSize int64, err error) {
	var lenBuf [lenSize]byte

	_, err = io.ReadFull(r, lenBuf[:])
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, 0, errTornRecord
	}

	if err != nil {
		return nil, 0, fmt.Errorf("read length: %w", err)
	}

	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen == 0 || payloadLen > maxPayload {
		return nil, 0, errTornRecord
	}

	payload = make([]byte, payloadLen)
	if _, err = io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, errTornRecord
		}
		return nil, 0, fmt.Errorf("read payload: %w", err)
	}

	var crcBuf [crcSize]byte
	if _, err = io.ReadFull(r, crcBuf[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, errTornRecord
		}
		return nil, 0, fmt.Errorf("read crc: %w", err)
	}

	h := crc32.NewIEEE()
	h.Write(lenBuf[:])
	h.Write(payload)
	if h.Sum32() != binary.BigEndian.Uint32(crcBuf[:]) {
		return nil, 0, errTornRecord
	}

	frameSize = int64(lenSize) + int64(payloadLen) + int64(crcSize)

	return payload, frameSize, nil
}
