package raftlog

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/carissaayo/go-durable-kv/internal/engine"
)

type RaftLog struct {
	file       *os.File
	buf        *bufio.Writer
	path       string
	syncPolicy engine.SyncPolicy

	nextOffset int64 // byte offset where the next record will start
}

const (
	lenSize    = 4
	crcSize    = 4
	maxPayload = 64 << 20 // 64 MiB
)

// OpenRaftLog opens (or creates) the log file at path, performs tail repair to remove any partial write left by a previous crash, and positions the write cursor at the first byte after the last valid record.
func OpenRaftLog(path string, syncPolicy engine.SyncPolicy) (*RaftLog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("raftlog: open %q: %w", path, err)
	}

	return nil, nil

}

// repairTail walks the file from byte 0, record by record, and returns the byte offset immediately after the last complete, checksum-verified record.
func repairTail(f *os.File) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, nil
	}

	r := bufio.NewReader(f)

	currentOffset := 0
	lastGoodOffest := 0

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
	}
	return 0, nil
}
