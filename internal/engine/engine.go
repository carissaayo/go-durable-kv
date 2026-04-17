package engine

import (
	"os"
	"sync"
)

type Engine struct {
	config Config
	db     *os.File         // or a more specific handle
	index  map[string]int64 // key → byte offset (in-memory index)
	mu     sync.RWMutex
	closed bool
}
