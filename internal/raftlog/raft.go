package raftlog

import (
	"bufio"
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

func OpenRaftLog(path string, syncPolicy engine.SyncPolicy) (*RaftLog, error)
