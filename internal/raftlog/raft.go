package raftlog

import (
	"bufio"
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
		return nil, fmt.Errorf("create wal dir %q: %w", dir, err)
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

	var (
		currentOffset  int64
		lastGoodOffset int64
	)

	for {

		// Check length prefix
		var lenBuf [lenSize]byte

		_, err := io.ReadFull(r, lenBuf[:])
		if err == io.EOF {
			// Zero bytes read — file ends on a clean boundary.
			break
		}
		if err == io.ErrUnexpectedEOF {
			// 1–3 bytes read — length prefix never finished writing.
			break
		}
		if err != nil {
			return 0, err
		}

		// Check the declared length; zero is never valid and a greater value means the feld itself is corrupt
		payloadLen := binary.BigEndian.Uint32(lenBuf[:])

		if payloadLen == 0 || payloadLen > maxPayload {
			break
		}

		// check the payload
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			// File ended mid-payload.
			break
		}

		// CRC32 checksum
		var crcBuf [crcSize]byte
		if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
			// File ended mid-checksum.
			break
		}

		// checksum check
		h := crc32.NewIEEE()
		h.Write(lenBuf[:])
		h.Write(payload)
		if h.Sum32() != binary.BigEndian.Uint32(crcBuf[:]) {
			// Mismatch at the tail = crash during the checksum write.
			break
		}

		currentOffset += int64(lenSize) + int64(payloadLen) + int64(crcSize)

		lastGoodOffset = currentOffset
	}
	return lastGoodOffset, nil
}

// Append encodes payload as a length-prefixed, checksummed record, writes it to the log, and returns the byte offset at which the record starts.
func (l *RaftLog) Append(payload []byte) (offset int64, err error) {

	l.mu.Lock()
	defer l.mu.Unlock()

	// Payload guards
	if l == nil || l.file == nil || l.buf == nil {
		return 0, errors.New("raftlog not initiated")
	}

	// repairTail rejects payloadLen == 0
	if len(payload) == 0 {
		return 0, errors.New("empty payload")
	}

	if len(payload) > maxPayload {
		return 0, errors.New("payload too large")
	}

	return 0, nil
}
