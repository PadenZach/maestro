package hub

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/coder/websocket"
)

func testHub(t *testing.T, timeout time.Duration) (*Hub, *httptest.Server) {
	t.Helper()
	h := New(slog.New(slog.NewTextHandler(io.Discard, nil)), timeout)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = h.Accept(w, r, "app") }))
	t.Cleanup(ts.Close)
	return h, ts
}
func peer(t *testing.T, ts *httptest.Server, id string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	typ, req := frame(t, ctx, c)
	if typ != websocket.MessageText || req["type"] != "executor_info" {
		t.Fatalf("handshake request: %v %v", typ, req)
	}
	reply(t, ctx, c, map[string]any{"type": "executor_info", "request_id": req["request_id"], "executor_id": id, "application_version": "v1"})
	return c
}
func frame(t *testing.T, ctx context.Context, c *websocket.Conn) (websocket.MessageType, map[string]any) {
	t.Helper()
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err = json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return typ, m
}
func reply(t *testing.T, ctx context.Context, c *websocket.Conn, m map[string]any) {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}
func registered(t *testing.T, h *Hub, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.Executors()) == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("registered = %d, want %d", len(h.Executors()), n)
}

type result struct {
	data []byte
	err  error
}

func request(h *Hub, ctx context.Context, req protocol.Request) <-chan result {
	ch := make(chan result, 1)
	go func() { b, e := h.Request(ctx, "app", req); ch <- result{b, e} }()
	return ch
}
func await(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("request did not finish")
		return result{}
	}
}
func pendingCount(c *Conn) int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.pending) }
func waitPending(t *testing.T, c *Conn, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if pendingCount(c) == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pending = %d, want %d", pendingCount(c), n)
}

func TestHandshakeRejectsInvalidIdentity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
	}{
		{"error", func(m map[string]any) { m["error_message"] = "refused" }},
		{"wrong_type", func(m map[string]any) { m["type"] = "get_workflow" }},
		{"empty_id", func(m map[string]any) { m["executor_id"] = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, ts := testHub(t, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseNow()
			_, m := frame(t, ctx, c)
			resp := map[string]any{"type": "executor_info", "request_id": m["request_id"], "executor_id": "valid", "application_version": "v1"}
			tc.change(resp)
			reply(t, ctx, c, resp)
			if _, _, err = c.Read(ctx); err == nil {
				t.Fatal("invalid handshake kept socket open")
			}
			if len(h.Executors()) != 0 {
				t.Fatal("invalid identity registered")
			}
		})
	}
}
func TestConcurrentRequestReuseAndOutOfOrder(t *testing.T) {
	h, ts := testHub(t, time.Second)
	c := peer(t, ts, "one")
	registered(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := protocol.Request{"type": "get_workflow", "workflow_id": "shared"}
	a := request(h, ctx, req)
	b := request(h, ctx, req)
	_, first := frame(t, ctx, c)
	_, second := frame(t, ctx, c)
	if first["request_id"] == second["request_id"] {
		t.Fatal("duplicate request IDs")
	}
	if _, ok := req["request_id"]; ok {
		t.Fatal("caller's map mutated")
	}
	for _, m := range []map[string]any{second, first} {
		reply(t, ctx, c, map[string]any{"type": "get_workflow", "request_id": m["request_id"], "output": map[string]any{"WorkflowUUID": m["request_id"]}})
	}
	r1, r2 := await(t, a), await(t, b)
	if r1.err != nil || r2.err != nil {
		t.Fatalf("response errors: %v %v", r1.err, r2.err)
	}
	if string(r1.data) == string(r2.data) {
		t.Fatal("responses not independently correlated")
	}
	waitPending(t, h.conns("app")[0], 0)
}
func TestOutOfOrderResponses(t *testing.T) {
	h, ts := testHub(t, time.Second)
	c := peer(t, ts, "one")
	registered(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a := request(h, ctx, protocol.Request{"type": "get_workflow", "workflow_id": "a"})
	b := request(h, ctx, protocol.Request{"type": "get_workflow", "workflow_id": "b"})
	_, first := frame(t, ctx, c)
	_, second := frame(t, ctx, c)
	for _, m := range []map[string]any{second, first} {
		reply(t, ctx, c, map[string]any{"type": "get_workflow", "request_id": m["request_id"], "output": m["workflow_id"]})
	}
	for want, ch := range map[string]<-chan result{"a": a, "b": b} {
		got := await(t, ch)
		if got.err != nil {
			t.Fatal(got.err)
		}
		var resp struct {
			Output string `json:"output"`
		}
		if err := json.Unmarshal(got.data, &resp); err != nil || resp.Output != want {
			t.Fatalf("response for %s: %s %v", want, got.data, err)
		}
	}
}
func TestUnknownDuplicateAndLateResponses(t *testing.T) {
	h, ts := testHub(t, time.Second)
	c := peer(t, ts, "one")
	registered(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	a := request(h, ctx, protocol.NewRequest(protocol.MsgGetWorkflow))
	_, one := frame(t, ctx, c)
	reply(t, ctx, c, map[string]any{"type": "get_workflow", "request_id": one["request_id"], "output": 1})
	if r := await(t, a); r.err != nil {
		t.Fatal(r.err)
	}
	b := request(h, ctx, protocol.NewRequest(protocol.MsgGetWorkflow))
	_, two := frame(t, ctx, c)
	for _, id := range []any{one["request_id"], "never-requested", one["request_id"]} {
		reply(t, ctx, c, map[string]any{"type": "get_workflow", "request_id": id, "output": 0})
	}
	reply(t, ctx, c, map[string]any{"type": "get_workflow", "request_id": two["request_id"], "output": 2})
	r := await(t, b)
	if r.err != nil || !strings.Contains(string(r.data), `"output":2`) {
		t.Fatalf("wrong reply: %s %v", r.data, r.err)
	}
	waitPending(t, h.conns("app")[0], 0)
}
func TestInvalidFramesFinishPending(t *testing.T) {
	for _, tc := range []struct {
		name string
		send func(*testing.T, context.Context, *websocket.Conn, map[string]any)
	}{
		{"wrong_type", func(t *testing.T, ctx context.Context, c *websocket.Conn, m map[string]any) {
			reply(t, ctx, c, map[string]any{"type": "cancel", "request_id": m["request_id"]})
		}},
		{"malformed", func(t *testing.T, ctx context.Context, c *websocket.Conn, _ map[string]any) {
			if err := c.Write(ctx, websocket.MessageText, []byte(`{broken`)); err != nil {
				t.Fatal(err)
			}
		}},
		{"binary", func(t *testing.T, ctx context.Context, c *websocket.Conn, m map[string]any) {
			if err := c.Write(ctx, websocket.MessageBinary, []byte(`{"type":"get_workflow","request_id":"`+m["request_id"].(string)+`"}`)); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, ts := testHub(t, time.Second)
			c := peer(t, ts, "one")
			registered(t, h, 1)
			conn := h.conns("app")[0]
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r := request(h, ctx, protocol.NewRequest(protocol.MsgGetWorkflow))
			_, m := frame(t, ctx, c)
			tc.send(t, ctx, c, m)
			got := await(t, r)
			if got.err == nil {
				t.Fatalf("invalid frame completed RPC: %s", got.data)
			}
			if tc.name == "wrong_type" {
				if !strings.Contains(got.err.Error(), "response type") {
					t.Fatalf("wrong type error: %v", got.err)
				}
				// A single mismatched response fails only its RPC, not the socket.
				next := request(h, ctx, protocol.NewRequest(protocol.MsgGetWorkflow))
				_, nextReq := frame(t, ctx, c)
				reply(t, ctx, c, map[string]any{"type": "get_workflow", "request_id": nextReq["request_id"], "output": "ok"})
				if again := await(t, next); again.err != nil {
					t.Fatalf("healthy response after mismatch: %v", again.err)
				}
			} else if !errors.Is(got.err, errConnClosed) {
				t.Fatalf("uncorrelatable frame must close socket, got: %v", got.err)
			}
			waitPending(t, conn, 0)
		})
	}
}
func TestMissingEnvelopeFieldsFinishPending(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame string
	}{
		{"null", `null`},
		{"empty_object", `{}`},
		{"missing_type", `{"request_id":"$request_id"}`},
		{"missing_request_id", `{"type":"get_workflow"}`},
		{"empty_type", `{"type":"","request_id":"$request_id"}`},
		{"empty_request_id", `{"type":"get_workflow","request_id":""}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, ts := testHub(t, 300*time.Millisecond)
			c := peer(t, ts, "one")
			registered(t, h, 1)
			conn := h.conns("app")[0]
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r := request(h, ctx, protocol.NewRequest(protocol.MsgGetWorkflow))
			_, m := frame(t, ctx, c)
			data := strings.ReplaceAll(tc.frame, "$request_id", m["request_id"].(string))
			if err := c.Write(ctx, websocket.MessageText, []byte(data)); err != nil {
				t.Fatal(err)
			}
			got := await(t, r)
			if !errors.Is(got.err, errConnClosed) {
				t.Fatalf("invalid envelope %s must close socket and release pending RPC, got %s: %v", tc.name, got.data, got.err)
			}
			waitPending(t, conn, 0)
		})
	}
}

func TestStructuredErrorKeepsConnection(t *testing.T) {
	h, ts := testHub(t, time.Second)
	c := peer(t, ts, "one")
	registered(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, message := range []string{"refused", "healthy"} {
		r := request(h, ctx, protocol.NewRequest(protocol.MsgGetWorkflow))
		_, m := frame(t, ctx, c)
		resp := map[string]any{"type": m["type"], "request_id": m["request_id"]}
		if message == "refused" {
			resp["error_message"] = "private"
		} else {
			resp["output"] = message
		}
		reply(t, ctx, c, resp)
		got := await(t, r)
		if got.err != nil || !strings.Contains(string(got.data), messageIf(message)) {
			t.Fatalf("response %s: %s %v", message, got.data, got.err)
		}
	}
}
func messageIf(s string) string {
	if s == "refused" {
		return "private"
	}
	return s
}
func TestRequestCleanupOnCancelTimeoutDisconnect(t *testing.T) {
	for _, tc := range []string{"cancel", "timeout", "disconnect"} {
		t.Run(tc, func(t *testing.T) {
			h, ts := testHub(t, 40*time.Millisecond)
			c := peer(t, ts, "one")
			registered(t, h, 1)
			conn := h.conns("app")[0]
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			callCtx, stop := context.WithCancel(ctx)
			defer stop()
			r := request(h, callCtx, protocol.NewRequest(protocol.MsgGetWorkflow))
			_, m := frame(t, ctx, c)
			switch tc {
			case "cancel":
				stop()
			case "disconnect":
				_ = c.CloseNow()
			}
			got := await(t, r)
			if got.err == nil {
				t.Fatal("expected failure")
			}
			if tc == "cancel" && !errors.Is(got.err, context.Canceled) {
				t.Fatal(got.err)
			}
			if tc == "timeout" && !errors.Is(got.err, context.DeadlineExceeded) {
				t.Fatal(got.err)
			}
			if tc == "disconnect" && !errors.Is(got.err, errConnClosed) {
				t.Fatal(got.err)
			}
			waitPending(t, conn, 0)
			if tc != "disconnect" {
				reply(t, ctx, c, map[string]any{"type": "get_workflow", "request_id": m["request_id"]})
			}
		})
	}
}
func selectedPeer(t *testing.T, h *Hub) string {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, c := range h.conns("app") {
			if pendingCount(c) > 0 {
				return c.executor.ID
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no selected peer")
	return ""
}
func TestReadRetriesAfterDisconnect(t *testing.T) {
	h, ts := testHub(t, time.Second)
	peers := map[string]*websocket.Conn{"a": peer(t, ts, "a"), "b": peer(t, ts, "b")}
	registered(t, h, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r := request(h, ctx, protocol.NewRequest(protocol.MsgGetWorkflow))
	selected := selectedPeer(t, h)
	other := "a"
	if selected == "a" {
		other = "b"
	}
	_, _ = frame(t, ctx, peers[selected])
	_ = peers[selected].CloseNow()
	_, m := frame(t, ctx, peers[other])
	reply(t, ctx, peers[other], map[string]any{"type": "get_workflow", "request_id": m["request_id"], "output": "retried"})
	got := await(t, r)
	if got.err != nil || !strings.Contains(string(got.data), "retried") {
		t.Fatalf("read retry: %s %v", got.data, got.err)
	}
}
func TestMutationDisconnectIsNotRetried(t *testing.T) {
	for _, typ := range []protocol.MessageType{protocol.MsgCancel, protocol.MsgResume, protocol.MsgForkWorkflow, protocol.MessageType("future_mutation")} {
		t.Run(string(typ), func(t *testing.T) {
			h, ts := testHub(t, 200*time.Millisecond)
			peers := map[string]*websocket.Conn{"a": peer(t, ts, "a"), "b": peer(t, ts, "b")}
			registered(t, h, 2)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			r := request(h, ctx, protocol.NewRequest(typ))
			selected := selectedPeer(t, h)
			_, _ = frame(t, ctx, peers[selected])
			_ = peers[selected].CloseNow()
			got := await(t, r)
			if got.err == nil {
				t.Fatal("ambiguous mutation returned success")
			}
			for id, c := range peers {
				if id != selected {
					probe, stop := context.WithTimeout(ctx, 80*time.Millisecond)
					_, _, err := c.Read(probe)
					stop()
					if err == nil {
						t.Fatal("mutation was sent to second peer")
					}
				}
			}
		})
	}
}
