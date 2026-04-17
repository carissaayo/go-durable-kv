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
	DataDir   string // directory for segment/log files
	IndexPath string // optional: where to persist the index snapshot

	// Sync / durability
	SyncPolicy   SyncPolicy
	SyncInterval time.Duration // only meaningful for SyncPeriodic

	// Sizing
	MaxSegmentSize int64 // rotate log file after this many bytes
	MaxValueSize   int64 // reject values larger than this

	// Compaction
	CompactOnOpen bool    // rebuild merged file at startup
	CompactRatio  float64 // trigger compaction when dead ratio exceeds this
}

func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:        dataDir,
		SyncPolicy:     SyncPeriodic,
		SyncInterval:   time.Second,
		MaxSegmentSize: 64 << 20, // 64 MB
		MaxValueSize:   1 << 20,  // 1 MB
		CompactOnOpen:  false,
		CompactRatio:   0.5,
	}
}
