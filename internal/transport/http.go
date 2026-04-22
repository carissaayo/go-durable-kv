package transport

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/carissaayo/go-durable-kv/internal/engine"
)

type Server struct {
	engine *engine.Engine
	mux    *http.ServeMux
}

func NewServer(e *engine.Engine) *Server {
	s := &Server{
		engine: e,
		mux:    http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("PUT /keys/{key}", s.handlePut)
	s.mux.HandleFunc("GET /keys/{key}", s.handleGet)
	s.mux.HandleFunc("DELETE /keys/{key}", s.handleDelete)
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	body, err := io.ReadAll(r.Body)

	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	if err := s.engine.Set(key, body); err != nil {
		switch {
		case errors.Is(err, engine.ErrValueTooLarge):
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge) // 413
		case errors.Is(err, engine.ErrClosed):
			http.Error(w, "engine is closed", http.StatusInternalServerError)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.WriteHeader(http.StatusNoContent) // 204
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, found, err := s.engine.Get(key)
	if errors.Is(err, engine.ErrClosed) {
		http.Error(w, "engine is closed", http.StatusInternalServerError)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !found {
		http.Error(w, "data not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := s.engine.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent) // 204
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m := s.engine.MetricsSnapshot()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(m); err != nil {
		http.Error(w, "encode metrics failed", http.StatusInternalServerError)
		return
	}
}
