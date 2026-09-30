package api_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/zpaden/maestro/internal/protocol"
)

func assertPageStatus(t *testing.T, body, class, label string) {
	t.Helper()
	if !strings.Contains(body, `class="status-dot `+class+`"`) {
		t.Fatalf("page missing %s status indicator: %s", class, body)
	}
	if !strings.Contains(body, `aria-label="`+label+`"`) {
		t.Fatalf("page missing accessible status %q: %s", label, body)
	}
	if strings.Count(body, "Applications online") != 1 {
		t.Fatalf("shared header must say exactly Applications online once: %s", body)
	}
}

func waitPageStatus(t *testing.T, url, class, label string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, body := getBody(t, url)
		if strings.Contains(body, `class="status-dot `+class+`"`) {
			assertPageStatus(t, body, class, label)
			return body
		}
		if time.Now().After(deadline) {
			t.Fatalf("page did not reach %s status: %s", class, body)
		}
		time.Sleep(time.Millisecond)
	}
}

func writeExecutorInfo(t *testing.T, ctx context.Context, c *websocket.Conn, request []byte, id string) {
	t.Helper()
	env, err := protocol.DecodeEnvelope(request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := json.Marshal(map[string]any{
		"type":                "executor_info",
		"request_id":          env.RequestID,
		"executor_id":         id,
		"application_version": "v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write(ctx, websocket.MessageText, response); err != nil {
		t.Fatal(err)
	}
}

func TestUIBrandingAndApplicationStatusLifecycle(t *testing.T) {
	ts, h := newTestServer(t)
	_, body := getBody(t, ts.URL+"/")
	assertPageStatus(t, body, "status-red", "Application status: no connections")
	for _, want := range []string{"<title>Applications · maestro</title>", `<div class="brand">maestro</div>`} {
		if !strings.Contains(body, want) {
			t.Fatalf("home missing %q: %s", want, body)
		}
	}
	for _, old := range []string{"DBOS", "Conductor", "0 Available"} {
		if strings.Contains(body, old) {
			t.Fatalf("home retains old branding %q: %s", old, body)
		}
	}
	if !strings.Contains(body, "No applications connected. Start an executor configured to connect to maestro.") {
		t.Fatalf("empty state is not maestro-branded: %s", body)
	}

	_, css := getBody(t, ts.URL+"/static/app.css")
	for _, class := range []string{".status-red", ".status-amber", ".status-green"} {
		if !strings.Contains(css, class) {
			t.Fatalf("stylesheet missing %s", class)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ready, _, err := websocket.Dial(ctx, wsURL(ts, "/websocket/app/testkey"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ready.CloseNow()
	_, readyRequest, err := ready.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, body = getBody(t, ts.URL+"/")
	assertPageStatus(t, body, "status-amber", "Application status: connection pending")

	writeExecutorInfo(t, ctx, ready, readyRequest, "ready")
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	_, body = getBody(t, ts.URL+"/")
	assertPageStatus(t, body, "status-green", "Application status: connected and ready")

	pending, _, err := websocket.Dial(ctx, wsURL(ts, "/websocket/app/testkey"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pending.CloseNow()
	if _, _, err := pending.Read(ctx); err != nil {
		t.Fatal(err)
	}
	_, body = getBody(t, ts.URL+"/")
	assertPageStatus(t, body, "status-amber", "Application status: connection pending")

	_ = pending.CloseNow()
	_ = ready.CloseNow()
	waitPageStatus(t, ts.URL+"/", "status-red", "Application status: no connections")
}

func TestFailedHTMLReadIsAmberWithLiveConnection(t *testing.T) {
	ts, h := newTestServer(t)
	dialFake(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(map[string]any) map[string]any {
			return map[string]any{"error_message": "executor read refused"}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	code, body := getBody(t, ts.URL+"/apps/myapp/workflows")
	if code != 502 || !strings.Contains(body, "executor read refused") {
		t.Fatalf("failed HTML read = %d: %s", code, body)
	}
	assertPageStatus(t, body, "status-amber", "Application status: degraded")
}

func TestHTMLBrandsKnownHubErrorsWithoutChangingJSONOrExecutorErrors(t *testing.T) {
	ts, h := newTestServer(t)
	code, body := getBody(t, ts.URL+"/apps/ghost/workflows")
	if code != 503 || !strings.Contains(body, "maestro: application unavailable") || strings.Contains(body, "conductor: application unavailable") {
		t.Fatalf("HTML internal error was not maestro-branded (%d): %s", code, body)
	}
	code, body = getBody(t, ts.URL+"/api/ghost/workflows")
	if code != 503 || !strings.Contains(body, "conductor: application unavailable") {
		t.Fatalf("JSON error identity changed (%d): %s", code, body)
	}

	dialFake(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(map[string]any) map[string]any {
			return map[string]any{"error_message": "conductor: executor connection closed"}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	code, body = getBody(t, ts.URL+"/apps/myapp/workflows")
	if code != 502 || !strings.Contains(body, "conductor: executor connection closed") || strings.Contains(body, "maestro: executor connection closed") {
		t.Fatalf("exact-string executor error was rewritten (%d): %s", code, body)
	}
}
