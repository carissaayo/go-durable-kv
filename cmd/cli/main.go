package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/carissaayo/go-durable-kv/internal/transport"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5000", "TCP server address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
		return
	}

	req, err := buildRequest(args)
	if err != nil {
		fmt.Println(err)
		usage()
		os.Exit(1)
	}

	resp, err := send(*addr, req)
	if err != nil {
		fmt.Printf("request failed: %v\n", err)
		os.Exit(1)
	}

	if !resp.OK {
		fmt.Printf("error: %s\n", resp.Error)
		os.Exit(1)
	}

	switch req.Op {
	case "get":
		if !resp.Found {
			fmt.Println("(not found)")
			return
		}
		fmt.Println(resp.Value)
	case "ping":
		fmt.Println(resp.Value)
	default:
		fmt.Println("ok")
	}
}

func buildRequest(args []string) (transport.TCPRequest, error) {
	switch args[0] {
	case "set":
		if len(args) != 3 {
			return transport.TCPRequest{}, fmt.Errorf("set requires: set <key> <value>")
		}
		return transport.TCPRequest{Op: "set", Key: args[1], Value: args[2]}, nil
	case "get":
		if len(args) != 2 {
			return transport.TCPRequest{}, fmt.Errorf("get requires: get <key>")
		}
		return transport.TCPRequest{Op: "get", Key: args[1]}, nil
	case "delete":
		if len(args) != 2 {
			return transport.TCPRequest{}, fmt.Errorf("delete requires: delete <key>")
		}
		return transport.TCPRequest{Op: "delete", Key: args[1]}, nil
	case "ping":
		return transport.TCPRequest{Op: "ping"}, nil
	default:
		return transport.TCPRequest{}, fmt.Errorf("unknown command: %s", args[0])
	}
}

func send(addr string, req transport.TCPRequest) (transport.TCPResponse, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return transport.TCPResponse{}, err
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	if err := enc.Encode(req); err != nil {
		return transport.TCPResponse{}, err
	}

	dec := json.NewDecoder(bufio.NewReader(conn))
	var resp transport.TCPResponse
	if err := dec.Decode(&resp); err != nil {
		return transport.TCPResponse{}, err
	}
	return resp, nil
}

func usage() {
	fmt.Println("Usage:")
	fmt.Println("  go run ./cmd/cli --addr 127.0.0.1:5000 set <key> <value>")
	fmt.Println("  go run ./cmd/cli --addr 127.0.0.1:5000 get <key>")
	fmt.Println("  go run ./cmd/cli --addr 127.0.0.1:5000 delete <key>")
	fmt.Println("  go run ./cmd/cli --addr 127.0.0.1:5000 ping")
}

