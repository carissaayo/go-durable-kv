package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	handler := transport.NewServer(e)

	srv := &http.Server{
		Addr:    ":4000",
		Handler: handler,
	}

	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		fmt.Printf("received signal %v, shutting down...\n", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			fmt.Printf("http shutdown error: %v\n", err)
		}
	case err := <-serverErr:
		if err != nil {
			fmt.Printf("server error: %v\n", err)
		}
	}

	if err := e.Close(); err != nil {
		fmt.Printf("engine close error: %v\n", err)
		os.Exit(1)
	}
}
