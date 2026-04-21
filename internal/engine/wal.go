package engine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
)

type Op byte

const (
	OpSet            Op = 0x01
	OpDelete         Op = 0x02
	recordHeaderSize    = 1 + 4 + 4 // op + keyLen + valLen
	recordCRCSize       = 4
)

var (
	ErrCorrupted = errors.New("record corrupted: crc mismatch")
	ErrUnknownOp = errors.New("unknown op byte")
)

type WAL struct {
	file       *os.File
	buf        *bufio.Writer
	path       string
	syncPolicy SyncPolicy
}

type Record struct {
	Op    Op
	Key   string
	Value []byte
}

// encodeRecord layout:
// [0]        : op (1 byte)
// [1:5]      : keyLen (uint32, big-endian)
// [5:9]      : valLen (uint32, big-endian)
// [9:..]     : key bytes
// [...:..]   : value bytes
// [last 4]   : crc32 over all preceding bytes

func encodeRecord(op Op, key string, value []byte) ([]byte, error) {
	keyBytes := []byte(key)
	keyLen := len(keyBytes)
	valLen := len(value)

	if keyLen > math.MaxUint32 {
		return nil, ErrKeyTooLarge
	}

	if valLen > math.MaxUint32 {
		return nil, ErrValueTooLarge
	}

	dataLen := recordHeaderSize + keyLen + valLen
	buf := make([]byte, dataLen+recordCRCSize)

	// write op
	buf[0] = byte(op)

	// write key and value lengths
	binary.BigEndian.PutUint32(buf[1:5], uint32(keyLen))
	binary.BigEndian.PutUint32(buf[5:9], uint32(valLen))

	// payload
	copy(buf[9:], keyBytes)
	copy(buf[9+keyLen:], value)

	// CRC over everything except trailing CRC field
	checksum := crc32.ChecksumIEEE(buf[:dataLen])
	binary.BigEndian.PutUint32(buf[dataLen:], checksum)
	return buf, nil
}

func decodeRecord(r io.Reader) (*Record, error) {
	// read op (1 byte)
	var opByte [1]byte
	if _, err := io.ReadFull(r, opByte[:]); err != nil {
		// io.EOF can mean clean end-of-log; caller decides replay policy.
		return nil, err
	}

	// keyLen (4 bytes)
	var keyLenBuf [4]byte
	if _, err := io.ReadFull(r, keyLenBuf[:]); err != nil {
		return nil, fmt.Errorf("read key len: %w", err)
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf[:])

	// valLen (4 bytes)
	var valLenBuf [4]byte
	if _, err := io.ReadFull(r, valLenBuf[:]); err != nil {
		return nil, fmt.Errorf("read val len: %w", err)
	}
	valLen := binary.BigEndian.Uint32(valLenBuf[:])

	// Protect against overflow / huge allocation before make().
	totalPayload64 := uint64(keyLen) + uint64(valLen)
	if totalPayload64 > uint64(math.MaxInt) {
		return nil, fmt.Errorf("record payload too large: keyLen=%d valLen=%d", keyLen, valLen)
	}
	totalPayload := int(totalPayload64)

	// read key + value
	payload := make([]byte, totalPayload)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	// read CRC32 (4 bytes)
	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, fmt.Errorf("read crc: %w", err)
	}
	storedCRC := binary.BigEndian.Uint32(crcBuf[:])

	// validate CRC — recompute over everything before the CRC
	h := crc32.NewIEEE()
	h.Write(opByte[:])
	h.Write(keyLenBuf[:])
	h.Write(valLenBuf[:])
	h.Write(payload)
	if h.Sum32() != storedCRC {
		return nil, ErrCorrupted
	}

	// validate op
	op := Op(opByte[0])
	if op != OpSet && op != OpDelete {
		return nil, ErrUnknownOp
	}

	return &Record{
		Op:    op,
		Key:   string(payload[:keyLen]),
		Value: bytes.Clone(payload[keyLen:]),
	}, nil
}

func OpenWAL(path string, syncPolicy SyncPolicy) (*WAL, error) {

	// Ensure the WAL directory exists, even if caller forgot to create it.
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir %q: %w", dir, err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{
		file:       f,
		buf:        bufio.NewWriter(f),
		path:       path,
		syncPolicy: syncPolicy,
	}, nil
}

func (w *WAL) Append(op Op, key string, value []byte) error {

	if w == nil {
		return errors.New("wal is nil")
	}
	if w.buf == nil || w.file == nil {
		return errors.New("wal is not initialized")
	}

	record, err := encodeRecord(op, key, value)
	if err != nil {
		return fmt.Errorf("record encode: %w", err)
	}

	if _, err := w.buf.Write(record); err != nil {
		return fmt.Errorf("wal write: %w", err)
	}

	if w.syncPolicy == SyncAlways {
		if err := w.Sync(); err != nil {
			return fmt.Errorf("wal sync: %w", err)
		}
	}

	return nil
}

func (w *WAL) Sync() error {
	if err := w.buf.Flush(); err != nil { // flush bufio buffer → OS
		return err
	}
	return w.file.Sync() // fsync → disk
}
func (w *WAL) Close() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}
