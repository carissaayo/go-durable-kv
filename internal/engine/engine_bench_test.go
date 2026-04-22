package engine

import (
	"fmt"
	"testing"
)

func newBenchEngine(b *testing.B, syncPolicy SyncPolicy) *Engine {
	b.Helper()

	cfg := DefaultConfig(b.TempDir())
	cfg.SyncPolicy = syncPolicy
	cfg.MaxWALSizeBytes = 1 << 62 // effectively disable compaction in benches

	e, err := Open(cfg)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	b.Cleanup(func() {
		if err := e.Close(); err != nil {
			b.Fatalf("Close() error = %v", err)
		}
	})
	return e
}

func BenchmarkEngineSet_SyncNone(b *testing.B) {
	e := newBenchEngine(b, SyncNone)
	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i)
		if err := e.Set(key, value); err != nil {
			b.Fatalf("Set() error = %v", err)
		}
	}
}

func BenchmarkEngineSet_SyncAlways(b *testing.B) {
	e := newBenchEngine(b, SyncAlways)
	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i)
		if err := e.Set(key, value); err != nil {
			b.Fatalf("Set() error = %v", err)
		}
	}
}

func BenchmarkEngineGet_Hit(b *testing.B) {
	e := newBenchEngine(b, SyncNone)

	// Seed data.
	const nKeys = 4096
	for i := 0; i < nKeys; i++ {
		if err := e.Set(fmt.Sprintf("k-%d", i), []byte("value")); err != nil {
			b.Fatalf("seed Set() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i%nKeys)
		_, found, err := e.Get(key)
		if err != nil {
			b.Fatalf("Get() error = %v", err)
		}
		if !found {
			b.Fatalf("Get() miss for existing key %q", key)
		}
	}
}

func BenchmarkEngineGet_Miss(b *testing.B) {
	e := newBenchEngine(b, SyncNone)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := e.Get(fmt.Sprintf("missing-%d", i))
		if err != nil {
			b.Fatalf("Get() error = %v", err)
		}
	}
}

func BenchmarkEngineDelete(b *testing.B) {
	e := newBenchEngine(b, SyncNone)

	// Seed keys that we'll delete.
	keys := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("k-%d", i)
		keys[i] = k
		if err := e.Set(k, []byte("value")); err != nil {
			b.Fatalf("seed Set() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := e.Delete(keys[i]); err != nil {
			b.Fatalf("Delete() error = %v", err)
		}
	}
}
