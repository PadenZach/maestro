package server_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/testserver"
	"github.com/coder/websocket"
)

func TestExecutorHandshakeRegistersAndDeregisters(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(ctx, testserver.WebSocketURL(ts, "/websocket/myapp/testkey"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	testserver.ReplyInfo(t, ctx, c, "exec-1", "v123")

	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	execs := h.Executors()
	got := execs[0]
	if got.ExecutorID != "exec-1" || got.App != "myapp" || got.Version != "v123" || got.Language != "python" {
		t.Fatalf("unexpected executor view: %+v", got)
	}

	// Closing the socket must deregister the executor.
	_ = c.Close(websocket.StatusNormalClosure, "")
	testserver.Wait(t, func() bool { return len(h.Executors()) == 0 })
}

func TestMultipleExecutorsSameApp(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i, id := range []string{"a", "b"} {
		c, _, err := websocket.Dial(ctx, testserver.WebSocketURL(ts, "/websocket/app/testkey"), nil)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		testserver.ReplyInfo(t, ctx, c, id, "v1")
	}
	testserver.Wait(t, func() bool { return len(h.Executors()) == 2 })
}

func TestExecutorOriginPolicy(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := testserver.WebSocketURL(ts, "/websocket/app/testkey")
	// SDK processes do not set Origin; browser requests from foreign sites do.
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("absent Origin rejected: %v", err)
	}
	testserver.ReplyInfo(t, ctx, c, "originless", "v1")
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
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
	ts, h := testserver.New(t, config.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, testserver.WebSocketURL(ts, "/websocket/app/arbitrary-placeholder"), nil)
	if err != nil {
		t.Fatalf("gateway-authenticated executor rejected because of URL key: %v", err)
	}
	defer c.CloseNow()
	testserver.ReplyInfo(t, ctx, c, "gateway-executor", "v1")
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	if got := h.Executors()[0]; got.ExecutorID != "gateway-executor" || got.App != "app" {
		t.Fatalf("unexpected executor: %+v", got)
	}
}
