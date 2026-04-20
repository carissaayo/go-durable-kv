package engine

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
)

type Op byte

const (
	Opset            Op = 0x01
	OpDelete         Op = 0x02
	recordHeaderSize    = 1 + 4 + 4 // op + keyLen + valLen
	recordCRCSize       = 4
)

type WAL struct {
	file       *os.File
	buf        *bufio.Writer
	path       string
	syncPolicy SyncPolicy
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
func (wal *WAL) OpenWAL(path string, policy string) (*WAL, error)

func (wal *WAL) Append(op Op, key string, val []byte) error

func (wal *WAL) Close() error
