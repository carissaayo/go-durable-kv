package engine

import "time"

type SyncPolicy int

const (
	SyncNone     SyncPolicy = iota // never fsync (fastest, least durable)
	SyncAlways                     // fsync after every write (slowest, safest)
	SyncPeriodic                   // fsync every N seconds (balanced)
)

type Config struct {
	DataDir         string
	SyncPolicy      SyncPolicy
	SyncInterval    time.Duration
	MaxValueSize    int64
	MaxWALSizeBytes int64
}

func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:         dataDir,
		SyncPolicy:      SyncPeriodic,
		SyncInterval:    time.Second,
		MaxValueSize:    1 << 20,  // 1 MB
		MaxWALSizeBytes: 64 << 20, // 64 MB
	}
}
