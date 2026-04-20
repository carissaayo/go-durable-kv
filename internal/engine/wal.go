package engine

import (
	"bufio"
	"os"
)

type Op byte

const (
	Opset    = 0x01
	OpDelete = 0x02
)

type WAL struct {
	file       *os.File
	buf        *bufio.Writer
	path       string
	syncPolicy SyncPolicy
}

func (wal *WAL) OpenWAL(path string, policy string) (*WAL, error)

func (wal *WAL) Append(op Op, key string, val []byte) error

func (wal *WAL) Close() error
