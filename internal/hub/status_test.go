package hub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func waitConnectionStatus(t *testing.T, h *Hub, want ConnectionStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := h.ConnectionStatus()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connection status = %+v, want %+v", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestIsExecutorConnectionClosedUsesSentinelIdentity(t *testing.T) {
	if !IsExecutorConnectionClosed(fmt.Errorf("request failed: %w", errConnClosed)) {
		t.Fatal("wrapped hub connection-closed error was not recognized")
	}
	if IsExecutorConnectionClosed(errors.New(errConnClosed.Error())) {
		t.Fatal("same-text error must not match the private hub sentinel")
	}
}

func TestConnectionStatusTracksHandshakeReadinessAndDisconnect(t *testing.T) {
	h, ts := testHub(t, time.Second)
	if got := h.ConnectionStatus(); got != (ConnectionStatus{}) {
		t.Fatalf("empty hub status = %+v", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ready, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ready.CloseNow()
	_, readyRequest := frame(t, ctx, ready)
	if got := h.ConnectionStatus(); got != (ConnectionStatus{Active: 1, Pending: 1}) {
		t.Fatalf("handshaking status = %+v", got)
	}
	reply(t, ctx, ready, map[string]any{
		"type":                "executor_info",
		"request_id":          readyRequest["request_id"],
		"executor_id":         "ready",
		"application_version": "v1",
	})
	waitConnectionStatus(t, h, ConnectionStatus{Active: 1, Ready: 1})

	pending, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.CloseNow()
	_, _ = frame(t, ctx, pending)
	if got := h.ConnectionStatus(); got != (ConnectionStatus{Active: 2, Ready: 1, Pending: 1}) {
		t.Fatalf("mixed ready and pending status = %+v", got)
	}

	_ = pending.CloseNow()
	_ = ready.CloseNow()
	waitConnectionStatus(t, h, ConnectionStatus{})
}
