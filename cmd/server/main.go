package main

import (
	"fmt"
	"net/http"

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
	defer e.Close()

	tp := transport.NewServer(e)

	if err := http.ListenAndServe(":4000", tp); err != nil {
		fmt.Printf("server error: %v\n", err)
	}
}
