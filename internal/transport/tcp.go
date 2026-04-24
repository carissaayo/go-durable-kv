package transport

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"strings"

	"github.com/carissaayo/go-durable-kv/internal/engine"
)

// TCPRequest is a single newline-delimited JSON request.
type TCPRequest struct {
	Op    string `json:"op"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// TCPResponse is a single newline-delimited JSON response.
type TCPResponse struct {
	OK    bool   `json:"ok"`
	Found bool   `json:"found,omitempty"`
	Value string `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}

type TCPServer struct {
	engine *engine.Engine
	ln     net.Listener
}

func NewTCPServer(e *engine.Engine) *TCPServer {
	return &TCPServer{engine: e}
}

func (s *TCPServer) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *TCPServer) Close() error {
	if s.ln != nil {
		return s.ln.Close()
	}
	return nil
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	enc := json.NewEncoder(writer)

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}

		var req TCPRequest
		if err := json.Unmarshal(line, &req); err != nil {
			_ = enc.Encode(TCPResponse{OK: false, Error: "invalid json"})
			_ = writer.Flush()
			continue
		}

		resp := s.execute(req)
		_ = enc.Encode(resp)
		_ = writer.Flush()
	}
}

func (s *TCPServer) execute(req TCPRequest) TCPResponse {
	switch strings.ToLower(strings.TrimSpace(req.Op)) {
	case "set":
		if req.Key == "" {
			return TCPResponse{OK: false, Error: "key is required"}
		}
		if err := s.engine.Set(req.Key, []byte(req.Value)); err != nil {
			return TCPResponse{OK: false, Error: err.Error()}
		}
		return TCPResponse{OK: true}

	case "get":
		if req.Key == "" {
			return TCPResponse{OK: false, Error: "key is required"}
		}
		val, found, err := s.engine.Get(req.Key)
		if err != nil {
			return TCPResponse{OK: false, Error: err.Error()}
		}
		if !found {
			return TCPResponse{OK: true, Found: false}
		}
		return TCPResponse{OK: true, Found: true, Value: string(val)}

	case "delete":
		if req.Key == "" {
			return TCPResponse{OK: false, Error: "key is required"}
		}
		if err := s.engine.Delete(req.Key); err != nil {
			return TCPResponse{OK: false, Error: err.Error()}
		}
		return TCPResponse{OK: true}

	case "ping":
		return TCPResponse{OK: true, Value: "pong"}

	default:
		return TCPResponse{OK: false, Error: "unknown op"}
	}
}

