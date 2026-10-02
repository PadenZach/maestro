package testserver

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/hub"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/server"
	"github.com/coder/websocket"
)

func New(t *testing.T, cfg config.Config) (*httptest.Server, *hub.Hub) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(log, 2*time.Second)
	srv := server.New(cfg, h, log)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, h
}

func WebSocketURL(ts *httptest.Server, path string) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http") + path
}

// ReplyInfo reads the server's EXECUTOR_INFO request and replies with
// the given identity, imitating a real DBOS executor.
func ReplyInfo(t *testing.T, ctx context.Context, c *websocket.Conn, id, version string) {
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

func Wait(t *testing.T, cond func() bool) {
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

// Executor is a mock DBOS executor: it answers EXECUTOR_INFO, then replies to
// each server-initiated request from a per-type handler table. A handler that
// returns nil makes the executor drop the socket (to exercise dispatcher retry).
type Executor struct {
	Conn     *websocket.Conn
	mu       sync.Mutex
	captured map[string]map[string]any // message type -> last raw request
}

type Responder func(req map[string]any) map[string]any

func Connect(t *testing.T, ts *httptest.Server, app, key, execID string, handlers map[protocol.MessageType]Responder) *Executor {
	t.Helper()
	language, sdkVersion := "python", "3.1.0"
	return connect(t, WebSocketURL(ts, "/websocket/"+app+"/"+key), protocol.ExecutorInfoResponse{
		ExecutorID: execID, ApplicationVersion: "v1", Language: &language, DBOSVersion: &sdkVersion,
	}, handlers)
}

func connect(t *testing.T, endpoint string, info protocol.ExecutorInfoResponse, handlers map[protocol.MessageType]Responder) *Executor {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, endpoint, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read executor_info request: %v", err)
	}
	envelope, err := protocol.DecodeEnvelope(data)
	if err != nil || envelope.Type != protocol.MsgExecutorInfo {
		t.Fatalf("executor_info request: envelope=%+v err=%v", envelope, err)
	}
	info.Type, info.RequestID = protocol.MsgExecutorInfo, envelope.RequestID
	response, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write(ctx, websocket.MessageText, response); err != nil {
		t.Fatalf("write executor_info response: %v", err)
	}
	fe := &Executor{Conn: c, captured: map[string]map[string]any{}}
	done := make(chan struct{})
	go func() { defer close(done); fe.loop(handlers) }()
	t.Cleanup(func() {
		_ = c.CloseNow()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("fake executor did not stop")
		}
	})
	return fe
}

func (fe *Executor) loop(handlers map[protocol.MessageType]Responder) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, data, err := fe.Conn.Read(ctx)
		cancel()
		if err != nil {
			return
		}
		var req map[string]any
		_ = json.Unmarshal(data, &req)
		typ, _ := req["type"].(string)
		reqID, _ := req["request_id"].(string)

		fe.mu.Lock()
		fe.captured[typ] = req
		fe.mu.Unlock()

		var resp map[string]any
		if h := handlers[protocol.MessageType(typ)]; h != nil {
			resp = h(req)
		} else {
			resp = map[string]any{"error_message": "Unknown message type"}
		}
		if resp == nil { // handler asked us to disconnect
			_ = fe.Conn.Close(websocket.StatusNormalClosure, "")
			return
		}
		resp["type"] = typ
		resp["request_id"] = reqID
		out, _ := json.Marshal(resp)
		writeCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		err = fe.Conn.Write(writeCtx, websocket.MessageText, out)
		stop()
		if err != nil {
			return
		}
	}
}

// Body returns the last request body the executor saw for a message type (the
// "body" submap if present, else the whole request frame).
func (fe *Executor) Body(t *testing.T, typ protocol.MessageType) map[string]any {
	t.Helper()
	fe.mu.Lock()
	defer fe.mu.Unlock()
	req := fe.captured[string(typ)]
	if req == nil {
		t.Fatalf("executor never received %s", typ)
	}
	if b, ok := req["body"].(map[string]any); ok {
		return b
	}
	return req
}

func String(s string) *string { return &s }

func Get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func Post(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Post(url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func Workflow(id, status string) protocol.WorkflowsOutput {
	return protocol.WorkflowsOutput{
		WorkflowUUID: id,
		Status:       String(status),
		WorkflowName: String("agentic_research_workflow"),
		CreatedAt:    String("2026-05-11T00:00:00"),
		QueueName:    String("default"),
		ExecutorID:   String("exec-1"),
	}
}

func Request(t *testing.T, url, method, body string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(data)
}

// ConnectVersion advertises the given SDK language and version.
func ConnectVersion(t *testing.T, tsURL, app, execID, language, sdkVersion string, handlers map[protocol.MessageType]Responder) *Executor {
	t.Helper()
	endpoint := "ws" + strings.TrimPrefix(tsURL, "http") + "/websocket/" + url.PathEscape(app) + "/testkey"
	return connect(t, endpoint, protocol.ExecutorInfoResponse{
		ExecutorID: execID, ApplicationVersion: "fixture-v1", Language: &language, DBOSVersion: &sdkVersion,
	}, handlers)
}

// Requests snapshots the frames received so tests can assert dispatch boundaries.
func (fe *Executor) Requests() map[string]map[string]any {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	return maps.Clone(fe.captured)
}

func OpenAPI(t *testing.T) []byte {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := server.New(config.Config{}, hub.New(log, time.Second), log)
	response := httptest.NewRecorder()
	s.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("OpenAPI status = %d", response.Code)
	}
	return response.Body.Bytes()
}
