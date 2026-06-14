package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/coder/websocket"

	"github.com/zpaden/maestro/internal/protocol"
)

// fakeExec is a mock DBOS executor: it answers EXECUTOR_INFO, then replies to
// each server-initiated request from a per-type handler table. A handler that
// returns nil makes the executor drop the socket (to exercise dispatcher retry).
type fakeExec struct {
	c        *websocket.Conn
	mu       sync.Mutex
	captured map[string]map[string]any // message type -> last raw request
}

type respondFn func(req map[string]any) map[string]any

func dialFake(t *testing.T, ts *httptest.Server, app, key, execID string, handlers map[protocol.MessageType]respondFn) *fakeExec {
	t.Helper()
	ctx := context.Background()
	c, _, err := websocket.Dial(ctx, wsURL(ts, "/websocket/"+app+"/"+key), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	answerExecutorInfo(t, ctx, c, execID, "v1")
	fe := &fakeExec{c: c, captured: map[string]map[string]any{}}
	go fe.loop(handlers)
	t.Cleanup(func() { _ = c.Close(websocket.StatusNormalClosure, "") })
	return fe
}

func (fe *fakeExec) loop(handlers map[protocol.MessageType]respondFn) {
	ctx := context.Background()
	for {
		_, data, err := fe.c.Read(ctx)
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
			resp = map[string]any{} // empty (nil payload) response
		}
		if resp == nil { // handler asked us to disconnect
			_ = fe.c.Close(websocket.StatusNormalClosure, "")
			return
		}
		resp["type"] = typ
		resp["request_id"] = reqID
		out, _ := json.Marshal(resp)
		if err := fe.c.Write(ctx, websocket.MessageText, out); err != nil {
			return
		}
	}
}

// body returns the last request body the executor saw for a message type (the
// "body" submap if present, else the whole request frame).
func (fe *fakeExec) body(t *testing.T, typ protocol.MessageType) map[string]any {
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

func strp(s string) *string { return &s }

func getBody(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func postBody(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/x-www-form-urlencoded", nil)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func sampleWorkflow(id, status string) protocol.WorkflowsOutput {
	return protocol.WorkflowsOutput{
		WorkflowUUID: id,
		Status:       strp(status),
		WorkflowName: strp("agentic_research_workflow"),
		CreatedAt:    strp("2026-05-11T00:00:00"),
		QueueName:    strp("default"),
		ExecutorID:   strp("exec-1"),
	}
}
