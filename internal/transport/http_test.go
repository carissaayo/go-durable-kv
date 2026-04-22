package transport

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/carissaayo/go-durable-kv/internal/engine"
)

func newTestServer(t *testing.T, mutateCfg func(*engine.Config)) *Server {
	t.Helper()

	cfg := engine.DefaultConfig(t.TempDir())
	if mutateCfg != nil {
		mutateCfg(&cfg)
	}

	e, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("engine.Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = e.Close()
	})

	return NewServer(e)
}

func doRequest(t *testing.T, h http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestServer_SingleRequest_TableDriven(t *testing.T) {
	type tc struct {
		name         string
		server       func(t *testing.T) *Server
		method       string
		path         string
		body         []byte
		wantStatus   int
		wantContains string
	}

	tests := []tc{
		{
			name:         "health returns 200 ok",
			server:       func(t *testing.T) *Server { return newTestServer(t, nil) },
			method:       http.MethodGet,
			path:         "/health",
			wantStatus:   http.StatusOK,
			wantContains: "ok",
		},
		{
			name:         "get missing key returns 404",
			server:       func(t *testing.T) *Server { return newTestServer(t, nil) },
			method:       http.MethodGet,
			path:         "/keys/missing",
			wantStatus:   http.StatusNotFound,
			wantContains: "data not found",
		},
		{
			name:       "delete missing key is 204",
			server:     func(t *testing.T) *Server { return newTestServer(t, nil) },
			method:     http.MethodDelete,
			path:       "/keys/missing",
			wantStatus: http.StatusNoContent,
		},
		{
			name: "put oversized value returns 413",
			server: func(t *testing.T) *Server {
				return newTestServer(t, func(cfg *engine.Config) {
					cfg.MaxValueSize = 3
				})
			},
			method:       http.MethodPut,
			path:         "/keys/k1",
			body:         []byte("toolarge"),
			wantStatus:   http.StatusRequestEntityTooLarge,
			wantContains: engine.ErrValueTooLarge.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.server(t)
			rr := doRequest(t, s, tt.method, tt.path, tt.body)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%q", rr.Code, tt.wantStatus, rr.Body.String())
			}

			if tt.wantContains != "" && !strings.Contains(rr.Body.String(), tt.wantContains) {
				t.Fatalf("body = %q, want to contain %q", rr.Body.String(), tt.wantContains)
			}
		})
	}
}

func TestServer_PutThenGet(t *testing.T) {
	s := newTestServer(t, nil)

	putResp := doRequest(t, s, http.MethodPut, "/keys/user1", []byte("alice"))
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d; body=%q", putResp.Code, http.StatusNoContent, putResp.Body.String())
	}

	getResp := doRequest(t, s, http.MethodGet, "/keys/user1", nil)
	if getResp.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d; body=%q", getResp.Code, http.StatusOK, getResp.Body.String())
	}

	got, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("reading GET body: %v", err)
	}
	if string(got) != "alice" {
		t.Fatalf("GET body = %q, want %q", got, "alice")
	}
}

func TestServer_DeleteThenGetNotFound(t *testing.T) {
	s := newTestServer(t, nil)

	putResp := doRequest(t, s, http.MethodPut, "/keys/k1", []byte("v1"))
	if putResp.Code != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want %d", putResp.Code, http.StatusNoContent)
	}

	delResp := doRequest(t, s, http.MethodDelete, "/keys/k1", nil)
	if delResp.Code != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want %d; body=%q", delResp.Code, http.StatusNoContent, delResp.Body.String())
	}

	getResp := doRequest(t, s, http.MethodGet, "/keys/k1", nil)
	if getResp.Code != http.StatusNotFound {
		t.Fatalf("GET-after-delete status = %d, want %d; body=%q", getResp.Code, http.StatusNotFound, getResp.Body.String())
	}
}

func TestServer_MetricsRoute(t *testing.T) {
	s := newTestServer(t, nil) // your existing helper

	_ = doRequest(t, s, http.MethodPut, "/keys/a", []byte("one"))
	_ = doRequest(t, s, http.MethodGet, "/keys/a", nil)       // hit
	_ = doRequest(t, s, http.MethodGet, "/keys/missing", nil) // miss
	_ = doRequest(t, s, http.MethodDelete, "/keys/a", nil)

	rr := doRequest(t, s, http.MethodGet, "/metrics", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", rr.Code, rr.Body.String())
	}

	var m engine.MetricsSnapshot
	if err := json.NewDecoder(rr.Body).Decode(&m); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}

	if m.Sets < 1 || m.Gets < 2 || m.GetHits < 1 || m.GetMisses < 1 || m.Deletes < 1 {
		t.Fatalf("unexpected metrics: %+v", m)
	}
}
