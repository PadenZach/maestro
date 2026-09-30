package api_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/zpaden/maestro/internal/api"
	"github.com/zpaden/maestro/internal/config"
	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/protocol"
)

func newTestServer(t *testing.T) (*httptest.Server, *hub.Hub) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(log, 2*time.Second)
	srv := api.New(config.Config{ConductorKey: "testkey"}, h, log)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, h
}

func wsURL(ts *httptest.Server, path string) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http") + path
}

// answerExecutorInfo reads the server's EXECUTOR_INFO request and replies with
// the given identity, imitating a real DBOS executor.
func answerExecutorInfo(t *testing.T, ctx context.Context, c *websocket.Conn, id, version string) {
	t.Helper()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read executor_info request: %v", err)
	}
	env, err := protocol.DecodeEnvelope(data)
	if err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Type != protocol.MsgExecutorInfo {
		t.Fatalf("expected %q, got %q", protocol.MsgExecutorInfo, env.Type)
	}
	lang := "python"
	out, _ := json.Marshal(protocol.ExecutorInfoResponse{
		Type:               protocol.MsgExecutorInfo,
		RequestID:          env.RequestID,
		ExecutorID:         id,
		ApplicationVersion: version,
		Language:           &lang,
	})
	if err := c.Write(ctx, websocket.MessageText, out); err != nil {
		t.Fatalf("write executor_info response: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestExecutorHandshakeRegistersAndDeregisters(t *testing.T) {
	ts, h := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(ctx, wsURL(ts, "/websocket/myapp/testkey"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	answerExecutorInfo(t, ctx, c, "exec-1", "v123")

	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	execs := h.Executors()
	got := execs[0]
	if got.ExecutorID != "exec-1" || got.App != "myapp" || got.Version != "v123" || got.Language != "python" {
		t.Fatalf("unexpected executor view: %+v", got)
	}

	// Closing the socket must deregister the executor.
	_ = c.Close(websocket.StatusNormalClosure, "")
	waitFor(t, func() bool { return len(h.Executors()) == 0 })
}

func TestMultipleExecutorsSameApp(t *testing.T) {
	ts, h := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i, id := range []string{"a", "b"} {
		c, _, err := websocket.Dial(ctx, wsURL(ts, "/websocket/app/testkey"), nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		answerExecutorInfo(t, ctx, c, id, "v1")
	}
	waitFor(t, func() bool { return len(h.Executors()) == 2 })
}

func TestExecutorOriginPolicy(t *testing.T) {
	ts, h := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := wsURL(ts, "/websocket/app/testkey")
	// SDK processes do not set Origin; browser requests from foreign sites do.
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("absent Origin rejected: %v", err)
	}
	answerExecutorInfo(t, ctx, c, "originless", "v1")
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	_ = c.CloseNow()
	headers := http.Header{"Origin": []string{"https://foreign.example"}}
	foreign, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: headers})
	if foreign != nil {
		_ = foreign.CloseNow()
	}
	if err == nil {
		t.Fatal("foreign Origin accepted")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign Origin status: %v, %v", resp, err)
	}
}

func TestExecutorKeyIsIgnored(t *testing.T) {
	ts, h := newTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, wsURL(ts, "/websocket/app/arbitrary-placeholder"), nil)
	if err != nil {
		t.Fatalf("gateway-authenticated executor rejected because of URL key: %v", err)
	}
	defer c.CloseNow()
	answerExecutorInfo(t, ctx, c, "gateway-executor", "v1")
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	if got := h.Executors()[0]; got.ExecutorID != "gateway-executor" || got.App != "app" {
		t.Fatalf("unexpected executor: %+v", got)
	}
}
