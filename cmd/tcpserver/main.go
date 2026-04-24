package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/carissaayo/go-durable-kv/internal/engine"
	"github.com/carissaayo/go-durable-kv/internal/transport"
)

func main() {
	cfg := engine.DefaultConfig("./data")

	e, err := engine.Open(cfg)
	if err != nil {
		fmt.Printf("error opening engine: %v\n", err)
		return
	}
	defer func() {
		if err := e.Close(); err != nil {
			fmt.Printf("engine close error: %v\n", err)
		}
	}()

	s := transport.NewTCPServer(e)
	go func() {
		if err := s.ListenAndServe(":5000"); err != nil {
			fmt.Printf("tcp server error: %v\n", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	sig := <-sigCh
	fmt.Printf("received signal %v, shutting down tcp server...\n", sig)
	_ = s.Close()
}

