package console_test

import (
	"context"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/hub"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
	"github.com/coder/websocket"
)

// Await registry transitions with a deadline, without sleep-driven assertions.
func awaitAvailability(t *testing.T, h *hub.Hub, ready, active int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s := h.ConnectionStatus()
		if s.Ready == ready && s.Active == active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry status = %+v, want ready=%d active=%d", s, ready, active)
		}
		runtime.Gosched()
	}
}

func TestApplicationAvailabilityRefreshTracksSelectedAppAndReconnect(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	handlers := map[protocol.MessageType]testserver.Responder{protocol.MsgListWorkflows: func(map[string]any) map[string]any { return map[string]any{"output": []any{}} }}
	a := testserver.Connect(t, ts, "app-a", "testkey", "a-1", handlers)
	testserver.Connect(t, ts, "app-b", "testkey", "b-1", handlers)
	awaitAvailability(t, h, 2, 2)
	_, body := testserver.Get(t, ts.URL+"/apps/app-a")
	if !strings.Contains(body, "1 connected executor") || !strings.Contains(body, `class="badge ok">Available`) {
		t.Fatalf("selected app not available: %s", body)
	}
	_ = a.Conn.CloseNow()
	awaitAvailability(t, h, 1, 1)
	_, body = testserver.Get(t, ts.URL+"/apps/app-a")
	if !strings.Contains(body, "No connected executors for this application.") || strings.Contains(body, `class="badge ok">Available`) || !strings.Contains(body, "status-green") {
		t.Fatalf("other app readiness must not make selected app available: %s", body)
	}
	_, home := testserver.Get(t, ts.URL+"/")
	if strings.Contains(home, `href="/apps/app-a"`) || !strings.Contains(home, `href="/apps/app-b"`) {
		t.Fatalf("home must remove offline app and retain connected app: %s", home)
	}
	for _, path := range []string{"/apps/app-a/workflows", "/api/app-a/workflows"} {
		status, errBody := testserver.Get(t, ts.URL+path)
		if status != http.StatusServiceUnavailable || !strings.Contains(errBody, "unavailable") {
			t.Fatalf("offline read %s = %d %s", path, status, errBody)
		}
	}
	_, body = testserver.Get(t, ts.URL+"/apps/app-a/workflows/rows")
	if !strings.Contains(body, "application unavailable") || strings.Contains(body, "No workflows match") {
		t.Fatalf("offline HTMX refresh must surface unavailable: %s", body)
	}
	old := testserver.Connect(t, ts, "app-a", "testkey", "a-1", handlers)
	awaitAvailability(t, h, 2, 2)
	replacement := testserver.Connect(t, ts, "app-a", "testkey", "a-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgListWorkflows: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{map[string]any{"WorkflowUUID": "replacement-confirmed"}}}
		},
	})
	// A replacement-specific read response is the registration barrier; counts
	// alone cannot distinguish old and new connections with the same identity.
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, reply := testserver.Get(t, ts.URL+"/api/app-a/workflows")
		if strings.Contains(reply, "replacement-confirmed") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement did not answer reads: %s", reply)
		}
		runtime.Gosched()
	}
	awaitAvailability(t, h, 2, 2)
	_ = old.Conn.CloseNow()
	awaitAvailability(t, h, 2, 2)
	_, body = testserver.Get(t, ts.URL+"/apps/app-a")
	if !strings.Contains(body, "1 connected executor") || !strings.Contains(body, `class="badge ok">Available`) {
		t.Fatalf("stale close evicted reconnect: %s", body)
	}
	second := testserver.Connect(t, ts, "app-a", "testkey", "a-2", handlers)
	awaitAvailability(t, h, 3, 3)
	_, body = testserver.Get(t, ts.URL+"/apps/app-a")
	if !strings.Contains(body, "2 connected executors") {
		t.Fatalf("app executor count incorrect: %s", body)
	}
	_ = replacement.Conn.CloseNow()
	awaitAvailability(t, h, 2, 2)
	_, body = testserver.Get(t, ts.URL+"/apps/app-a")
	if !strings.Contains(body, "1 connected executor") || !strings.Contains(body, `class="badge ok">Available`) {
		t.Fatalf("losing one executor made app unavailable: %s", body)
	}
	_ = second.Conn.CloseNow()
	awaitAvailability(t, h, 1, 1)
}

func TestApplicationAvailabilityPendingIsNotReadyAndFinalCloseIsRed(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pending, _, err := websocket.Dial(ctx, testserver.WebSocketURL(ts, "/websocket/pending-app/testkey"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.CloseNow()
	if _, _, err = pending.Read(ctx); err != nil {
		t.Fatal(err)
	}
	awaitAvailability(t, h, 0, 1)
	_, home := testserver.Get(t, ts.URL+"/")
	assertPageStatus(t, home, "status-amber", "Application status: connection pending")
	if strings.Contains(home, `href="/apps/pending-app"`) {
		t.Fatalf("pending app presented as ready: %s", home)
	}
	_, body := testserver.Get(t, ts.URL+"/apps/pending-app")
	if !strings.Contains(body, "No connected executors for this application.") {
		t.Fatalf("pending app available: %s", body)
	}
	_ = pending.CloseNow()
	awaitAvailability(t, h, 0, 0)
	_, home = testserver.Get(t, ts.URL+"/")
	assertPageStatus(t, home, "status-red", "Application status: no connections")
}
