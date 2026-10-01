package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/zpaden/maestro/internal/api"
	"github.com/zpaden/maestro/internal/config"
	"github.com/zpaden/maestro/internal/hub"
)

func TestHTTPListenerLoopbackRestriction(t *testing.T) {
	for _, tc := range []struct {
		addr    string
		allowed bool
	}{{"127.0.0.1:0", true}, {"[::1]:0", true}, {"0.0.0.0:0", false}, {"[::]:0", false}} {
		t.Run(tc.addr, func(t *testing.T) {
			l, err := net.Listen("tcp", tc.addr)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			if got := validateHTTPListener(true, l.Addr()); (got == nil) != tc.allowed {
				t.Fatalf("listener %v: %v", l.Addr(), got)
			}
			if err := validateHTTPListener(false, l.Addr()); err != nil {
				t.Fatalf("default mode: %v", err)
			}
		})
	}
}

// On shutdown, a pending HTTP→executor RPC must finish while
// the process is shutting down, not hold HTTP Shutdown until its deadline.
func TestServeShutdownReleasesPendingHTTPRPC(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(log, 30*time.Second)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, l, &http.Server{Handler: api.New(config.Config{ConductorKey: "key"}, h, log).Handler()}, h)
	}()
	t.Cleanup(func() { stop(); _ = l.Close() })
	peerCtx, peerStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer peerStop()
	c, _, err := websocket.Dial(peerCtx, "ws://"+l.Addr().String()+"/websocket/app/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	_, data, err := c.Read(peerCtx)
	if err != nil {
		t.Fatal(err)
	}
	var handshake struct {
		Type string `json:"type"`
		ID   string `json:"request_id"`
	}
	if err := json.Unmarshal(data, &handshake); err != nil || handshake.Type != "executor_info" {
		t.Fatalf("handshake %s: %v", data, err)
	}
	reply, _ := json.Marshal(map[string]string{"type": "executor_info", "request_id": handshake.ID, "executor_id": "blocked-peer", "application_version": "v1"})
	if err := c.Write(peerCtx, websocket.MessageText, reply); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for len(h.Executors()) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(h.Executors()) != 1 {
		t.Fatal("executor did not register")
	}

	httpCtx, httpStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpStop()
	httpReq, err := http.NewRequestWithContext(httpCtx, "GET", "http://"+l.Addr().String()+"/api/app/workflows", nil)
	if err != nil {
		t.Fatal(err)
	}
	responses := make(chan error, 1)
	go func() {
		resp, err := http.DefaultClient.Do(httpReq)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode < 400 {
				err = errors.New("pending RPC returned success during shutdown")
			}
		}
		responses <- err
	}()
	_, data, err = c.Read(peerCtx)
	if err != nil {
		t.Fatal(err)
	}
	var pending struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &pending); err != nil || pending.Type != "list_workflows" {
		t.Fatalf("pending request %s: %v", data, err)
	}
	// The executor intentionally never replies. A 30s RPC timeout cannot
	// substitute for releasing the HTTP handler during process shutdown.
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown did not join waiting RPC: %v", err)
		}
	case <-time.After(2 * time.Second):
		httpStop() // unblock old implementations before failing the test
		t.Fatal("shutdown waited for a pending executor RPC")
	}
	select {
	case err := <-responses:
		if err != nil {
			t.Fatalf("waiting HTTP call: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting HTTP call survived shutdown")
	}
}

// Exercise the actual listener/server teardown path, not just Hub.Shutdown.
func TestServeShutdownClosesHijackedExecutor(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(log, time.Second)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, l, &http.Server{Handler: api.New(config.Config{ConductorKey: "key"}, h, log).Handler()}, h)
	}()
	t.Cleanup(func() { stop(); _ = l.Close() })
	dialCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(dialCtx, "ws://"+l.Addr().String()+"/websocket/app/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	_, data, err := c.Read(dialCtx)
	if err != nil {
		t.Fatal(err)
	}
	var req struct {
		Type string `json:"type"`
		ID   string `json:"request_id"`
	}
	if err = json.Unmarshal(data, &req); err != nil || req.Type != "executor_info" {
		t.Fatalf("handshake %s %v", data, err)
	}
	resp, _ := json.Marshal(map[string]string{"type": "executor_info", "request_id": req.ID, "executor_id": "process-peer", "application_version": "v1"})
	if err = c.Write(dialCtx, websocket.MessageText, resp); err != nil {
		t.Fatal(err)
	}
	stop()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server shutdown did not finish")
	}
	readCtx, readStop := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer readStop()
	if _, _, err = c.Read(readCtx); err == nil || strings.Contains(err.Error(), "deadline exceeded") {
		t.Fatalf("hijacked socket survived server shutdown: %v", err)
	}
	// No extra shutdown event may admit a new socket on the stopped listener.
	probeCtx, probeStop := context.WithTimeout(context.Background(), time.Second)
	defer probeStop()
	_, _, err = websocket.Dial(probeCtx, "ws://"+l.Addr().String()+"/websocket/app/key", nil)
	if err == nil {
		t.Fatal("listener still open")
	}
}
