// Package engine exposes the durable KV state machine for importers such as go-kv-dist.
package engine

import ie "github.com/carissaayo/go-durable-kv/internal/engine"

type Engine = ie.Engine
type Config = ie.Config
type SyncPolicy = ie.SyncPolicy

const (
	SyncNone     = ie.SyncNone
	SyncAlways   = ie.SyncAlways
	SyncPeriodic = ie.SyncPeriodic
)

var (
	ErrClosed        = ie.ErrClosed
	ErrValueTooLarge = ie.ErrValueTooLarge
	ErrKeyTooLarge   = ie.ErrKeyTooLarge
)

var (
	Open          = ie.Open
	DefaultConfig = ie.DefaultConfig
)
