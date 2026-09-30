package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, wsURL(ts, "/websocket/"+app+"/"+key), nil)
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
	language, sdkVersion := "python", "3.1.0"
	response, err := json.Marshal(protocol.ExecutorInfoResponse{
		Type: protocol.MsgExecutorInfo, RequestID: envelope.RequestID, ExecutorID: execID,
		ApplicationVersion: "v1", Language: &language, DBOSVersion: &sdkVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write(ctx, websocket.MessageText, response); err != nil {
		t.Fatalf("write executor_info response: %v", err)
	}
	fe := &fakeExec{c: c, captured: map[string]map[string]any{}}
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

func (fe *fakeExec) loop(handlers map[protocol.MessageType]respondFn) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, data, err := fe.c.Read(ctx)
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
			_ = fe.c.Close(websocket.StatusNormalClosure, "")
			return
		}
		resp["type"] = typ
		resp["request_id"] = reqID
		out, _ := json.Marshal(resp)
		writeCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		err = fe.c.Write(writeCtx, websocket.MessageText, out)
		stop()
		if err != nil {
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
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func postBody(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Post(url, "application/x-www-form-urlencoded", nil)
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
