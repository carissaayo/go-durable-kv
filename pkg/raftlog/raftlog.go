// Package raftlog exposes the consensus append-only log for importers such as go-kv-dist.
// Implementation lives in internal/raftlog.
package raftlog

import (
	"github.com/carissaayo/go-durable-kv/internal/engine"
	ir "github.com/carissaayo/go-durable-kv/internal/raftlog"
)

type RaftLog = ir.RaftLog

type SyncPolicy = engine.SyncPolicy

const (
	SyncNone     = engine.SyncNone
	SyncAlways   = engine.SyncAlways
	SyncPeriodic = engine.SyncPeriodic
)

func OpenRaftLog(path string, syncPolicy SyncPolicy) (*RaftLog, error) {
	return ir.OpenRaftLog(path, syncPolicy)
}
