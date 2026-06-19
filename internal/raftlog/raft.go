package raftlog

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

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

// crcTable is computed once at init time using the Castagnoli polynomial,
// which has better error-detection properties than the IEEE polynomial.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

func OpenRaftLog(path string, syncPolicy engine.SyncPolicy) (*RaftLog, error) {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create raft dir %q: %w", dir, err)
	}

	return nil, nil

}
