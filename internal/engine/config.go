package engine

import "time"

type SyncPolicy int

const (
	SyncNone     SyncPolicy = iota // never fsync (fastest, least durable)
	SyncAlways                     // fsync after every write (slowest, safest)
	SyncPeriodic                   // fsync every N seconds (balanced)
)

type Config struct {
	// Paths
	DataDir string // directory for segment/log files

	// Sync / durability
	SyncPolicy   SyncPolicy
	SyncInterval time.Duration // only meaningful for SyncPeriodic

	// Sizing
	MaxValueSize int64 // reject values larger than this

}

func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:      dataDir,
		SyncPolicy:   SyncPeriodic,
		SyncInterval: time.Second,
		MaxValueSize: 1 << 20, // 1 MB
	}
}
