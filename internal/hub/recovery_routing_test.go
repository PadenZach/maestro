package hub

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/coder/websocket"
)

func TestRecoveryRequestsMatchApplicationVersion(t *testing.T) {
	h, server := testHub(t, time.Second)
	peer(t, server, "v1-peer")
	registered(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := h.RequestVersion(ctx, "app", "v2", protocol.RecoveryRequest([]string{"dead"})); !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("missing version: %v", err)
	}
	v2, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer v2.CloseNow()
	_, req := frame(t, ctx, v2)
	reply(t, ctx, v2, map[string]any{"type": "executor_info", "request_id": req["request_id"], "executor_id": "v2-peer", "application_version": "v2"})
	registered(t, h, 2)
	done := make(chan result, 1)
	go func() {
		data, err := h.RequestVersion(ctx, "app", "v2", protocol.RecoveryRequest([]string{"dead"}))
		done <- result{data, err}
	}()
	_, req = frame(t, ctx, v2)
	if req["type"] != "recovery" || req["executor_ids"].([]any)[0] != "dead" {
		t.Fatalf("recovery request: %v", req)
	}
	reply(t, ctx, v2, map[string]any{"type": "recovery", "request_id": req["request_id"], "success": true})
	if result := await(t, done); result.err != nil {
		t.Fatal(result.err)
	}
}

func TestRecoverySnapshotIgnoresReplacedSocket(t *testing.T) {
	h, server := testHub(t, time.Second)
	old := peer(t, server, "same-id")
	registered(t, h, 1)
	previous := h.conns("app")[0]
	replacement := peer(t, server, "same-id")
	deadline := time.Now().Add(time.Second)
	for h.conns("app")[0] == previous && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.conns("app")[0] == previous {
		t.Fatal("replacement was not registered")
	}
	_ = old.CloseNow()
	h.deregister(previous) // Deliberately deliver a late stale disconnect.
	live, disconnected := h.RecoverySnapshot()
	if len(live) != 1 || len(disconnected) != 0 {
		t.Fatalf("stale disconnect became a recovery candidate: %v %v", live, disconnected)
	}
	before := time.Now()
	_ = replacement.CloseNow()
	registered(t, h, 0)
	live, disconnected = h.RecoverySnapshot()
	if len(live) != 0 || len(disconnected) != 1 || disconnected[0].ExecutorID != "same-id" || disconnected[0].DisconnectedAt.Before(before) {
		t.Fatalf("lost actual disconnect: %v %v", live, disconnected)
	}
	_, disconnected = h.RecoverySnapshot()
	if len(disconnected) != 0 {
		t.Fatal("disconnect notification was not consumed")
	}
}
