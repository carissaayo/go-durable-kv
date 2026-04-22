package engine

import (
	"errors"
	"testing"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()

	cfg := DefaultConfig(t.TempDir())
	e, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	return e
}

func TestSet_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, e *Engine)
		key       string
		value     []byte
		wantErr   error
		wantFound bool
		wantValue []byte
	}{
		{
			name:      "set new key",
			key:       "k1",
			value:     []byte("v1"),
			wantErr:   nil,
			wantFound: true,
			wantValue: []byte("v1"),
		},
		{
			name: "overwrite existing key",
			setup: func(t *testing.T, e *Engine) {
				t.Helper()
				if err := e.Set("k1", []byte("old")); err != nil {
					t.Fatalf("setup Set() error = %v", err)
				}
			},
			key:       "k1",
			value:     []byte("new"),
			wantErr:   nil,
			wantFound: true,
			wantValue: []byte("new"),
		},
		{
			name: "closed engine returns ErrClosed",
			setup: func(t *testing.T, e *Engine) {
				t.Helper()
				if err := e.Close(); err != nil {
					t.Fatalf("setup Close() error = %v", err)
				}
			},
			key:       "k1",
			value:     []byte("v1"),
			wantErr:   ErrClosed,
			wantFound: false,
			wantValue: nil,
		},
		{
			name: "value too large returns ErrValueTooLarge",
			setup: func(t *testing.T, e *Engine) {
				t.Helper()
				e.config.MaxValueSize = 2
			},
			key:       "k1",
			value:     []byte("too-big"),
			wantErr:   ErrValueTooLarge,
			wantFound: false,
			wantValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEngine(t)

			if tt.setup != nil {
				tt.setup(t, e)
			}

			err := e.Set(tt.key, tt.value)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Set() error = %v, want %v", err, tt.wantErr)
			}

			if tt.wantErr != nil {
				return
			}

			got, found, err := e.Get(tt.key)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if found != tt.wantFound {
				t.Fatalf("Get() found = %v, want %v", found, tt.wantFound)
			}
			if string(got) != string(tt.wantValue) {
				t.Fatalf("Get() value = %q, want %q", got, tt.wantValue)
			}
		})
	}
}

func TestGet_TableDriven(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, e *Engine)
		key       string
		wantErr   error
		wantFound bool
		wantValue []byte
	}{
		{
			name: "existing key",
			setup: func(t *testing.T, e *Engine) {
				t.Helper()
				if err := e.Set("k1", []byte("v1")); err != nil {
					t.Fatalf("setup Set() error = %v", err)
				}
			},
			key:       "k1",
			wantErr:   nil,
			wantFound: true,
			wantValue: []byte("v1"),
		},
		{
			name:      "missing key",
			key:       "missing",
			wantErr:   nil,
			wantFound: false,
			wantValue: nil,
		},
		{
			name: "closed engine returns ErrClosed",
			setup: func(t *testing.T, e *Engine) {
				t.Helper()
				if err := e.Close(); err != nil {
					t.Fatalf("setup Close() error = %v", err)
				}
			},
			key:       "k1",
			wantErr:   ErrClosed,
			wantFound: false,
			wantValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEngine(t)

			if tt.setup != nil {
				tt.setup(t, e)
			}

			got, found, err := e.Get(tt.key)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Get() error = %v, want %v", err, tt.wantErr)
			}
			if found != tt.wantFound {
				t.Fatalf("Get() found = %v, want %v", found, tt.wantFound)
			}
			if string(got) != string(tt.wantValue) {
				t.Fatalf("Get() value = %q, want %q", got, tt.wantValue)
			}
		})
	}
}

func TestDelete_TableDriven(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T, e *Engine)
		key        string
		wantErr    error
		afterFound bool
	}{
		{
			name: "delete existing key",
			setup: func(t *testing.T, e *Engine) {
				t.Helper()
				if err := e.Set("k1", []byte("v1")); err != nil {
					t.Fatalf("setup Set() error = %v", err)
				}
			},
			key:        "k1",
			wantErr:    nil,
			afterFound: false,
		},
		{
			name:       "delete missing key is no-op",
			key:        "missing",
			wantErr:    nil,
			afterFound: false,
		},
		{
			name: "closed engine returns ErrClosed",
			setup: func(t *testing.T, e *Engine) {
				t.Helper()
				if err := e.Close(); err != nil {
					t.Fatalf("setup Close() error = %v", err)
				}
			},
			key:        "k1",
			wantErr:    ErrClosed,
			afterFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEngine(t)

			if tt.setup != nil {
				tt.setup(t, e)
			}

			err := e.Delete(tt.key)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Delete() error = %v, want %v", err, tt.wantErr)
			}

			if tt.wantErr != nil {
				return
			}

			_, found, err := e.Get(tt.key)
			if err != nil {
				t.Fatalf("Get() after Delete() error = %v", err)
			}
			if found != tt.afterFound {
				t.Fatalf("Get() after Delete() found = %v, want %v", found, tt.afterFound)
			}
		})
	}
}

func TestSetInputIsolation(t *testing.T) {
	e := newTestEngine(t)

	in := []byte("hello")
	if err := e.Set("k1", in); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	in[0] = 'X' // mutate caller slice after Set

	got, found, err := e.Get("k1")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !found {
		t.Fatalf("Get() found = false, want true")
	}
	if string(got) != "hello" {
		t.Fatalf("stored value mutated unexpectedly, got %q", got)
	}

	// also verify Get returns a copy
	got[0] = 'Y'
	got2, found2, err := e.Get("k1")
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	if !found2 {
		t.Fatalf("second Get() found = false, want true")
	}
	if string(got2) != "hello" {
		t.Fatalf("Get() did not return copy, second value = %q", got2)
	}
}

func TestClose_Idempotent(t *testing.T) {
	e := newTestEngine(t)

	if err := e.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}
