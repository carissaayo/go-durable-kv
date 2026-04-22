package engine

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	sets          atomic.Uint64
	gets          atomic.Uint64
	getHits       atomic.Uint64
	getMisses     atomic.Uint64
	deletes       atomic.Uint64
	walAppends    atomic.Uint64
	compactions   atomic.Uint64
	replayRecords atomic.Uint64

	replayDurationNs atomic.Int64
	startedUnix      atomic.Int64
}

type MetricsSnapshot struct {
	Sets             uint64 `json:"sets"`
	Gets             uint64 `json:"gets"`
	GetHits          uint64 `json:"get_hits"`
	GetMisses        uint64 `json:"get_misses"`
	Deletes          uint64 `json:"deletes"`
	WALAppends       uint64 `json:"wal_appends"`
	Compactions      uint64 `json:"compactions"`
	ReplayRecords    uint64 `json:"replay_records"`
	ReplayDurationNs int64  `json:"replay_duration_ns"`
	UptimeSeconds    int64  `json:"uptime_seconds"`
}

func NewMetrics() *Metrics {
	m := &Metrics{}
	m.startedUnix.Store(time.Now().Unix())
	return m
}

func (m *Metrics) IncSet()                   { m.sets.Add(1) }
func (m *Metrics) IncGet()                   { m.gets.Add(1) }
func (m *Metrics) IncGetHit()                { m.getHits.Add(1) }
func (m *Metrics) IncGetMiss()               { m.getMisses.Add(1) }
func (m *Metrics) IncDelete()                { m.deletes.Add(1) }
func (m *Metrics) IncWALAppend()             { m.walAppends.Add(1) }
func (m *Metrics) IncCompaction()            { m.compactions.Add(1) }
func (m *Metrics) AddReplayRecords(n uint64) { m.replayRecords.Add(n) }
func (m *Metrics) SetReplayDuration(d time.Duration) {
	m.replayDurationNs.Store(d.Nanoseconds())
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	started := m.startedUnix.Load()
	return MetricsSnapshot{
		Sets:             m.sets.Load(),
		Gets:             m.gets.Load(),
		GetHits:          m.getHits.Load(),
		GetMisses:        m.getMisses.Load(),
		Deletes:          m.deletes.Load(),
		WALAppends:       m.walAppends.Load(),
		Compactions:      m.compactions.Load(),
		ReplayRecords:    m.replayRecords.Load(),
		ReplayDurationNs: m.replayDurationNs.Load(),
		UptimeSeconds:    time.Now().Unix() - started,
	}
}
