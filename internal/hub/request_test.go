package hub

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/zpaden/maestro/internal/protocol"
)

func versionedPeer(t *testing.T, tsURL, language, sdkVersion, appVersion string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(tsURL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	_, req := frame(t, ctx, c)
	reply(t, ctx, c, map[string]any{"type": "executor_info", "request_id": req["request_id"], "executor_id": sdkVersion + appVersion, "application_version": appVersion, "language": language, "dbos_version": sdkVersion})
	return c
}

func TestRequestValidationRejectsMalformedFramesWithoutSending(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  any
		body any
	}{
		{"typed_empty_type", protocol.MessageType(""), nil},
		{"invalid_body", protocol.MsgListWorkflows, true},
		{"missing_type", nil, nil},
		{"invalid_type", 7, nil},
		{"empty_type", "", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, ts := testHub(t, time.Second)
			c := versionedPeer(t, ts.URL, "python", "2.24.0", "app-v1")
			registered(t, h, 1)
			req := protocol.Request{}
			if tc.typ != nil {
				req["type"] = tc.typ
			}
			if tc.body != nil {
				req["body"] = tc.body
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := h.Request(ctx, "app", req); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unsafe request accepted or timed out: %v", err)
			}
			// A valid follow-up is the next frame: rejected frames were never sent.
			next := request(h, ctx, protocol.GetWorkflowRequest("safe", false, false))
			_, wire := frame(t, ctx, c)
			if wire["type"] != string(protocol.MsgGetWorkflow) || wire["workflow_id"] != "safe" {
				t.Fatalf("rejected request reached executor: %v", wire)
			}
			reply(t, ctx, c, map[string]any{"type": "get_workflow", "request_id": wire["request_id"], "output": nil})
			if got := await(t, next); got.err != nil {
				t.Fatal(got.err)
			}
		})
	}
}

func TestReadDispatchPreservesFiltersWithoutSDKIdentity(t *testing.T) {
	h, ts := testHub(t, time.Second)
	peer := versionedPeer(t, ts.URL, "", "", "app-v1")
	registered(t, h, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, command := range []protocol.MessageType{
		protocol.MsgListWorkflows, protocol.MsgListQueuedWorkflows, protocol.MsgListQueues,
		protocol.MsgListSchedules, protocol.MsgGetSchedule, protocol.MsgGetWorkflowAggregates,
		protocol.MsgGetStepAggregates, protocol.MsgExportWorkflow,
	} {
		body := map[string]any{"attributes": map[string]any{"team": "only"}, "application_name": []string{"app"}, "schedule_name": []string{}, "has_parent": false}
		req := protocol.Request{"type": command, "body": body}
		pending := request(h, ctx, req)
		_, wire := frame(t, ctx, peer)
		want, _ := json.Marshal(body)
		got, _ := json.Marshal(wire["body"])
		if wire["type"] != string(command) || string(got) != string(want) {
			t.Fatalf("read command or filters changed: %v", wire)
		}
		reply(t, ctx, peer, map[string]any{"type": string(command), "request_id": wire["request_id"], "output": []any{}})
		if result := await(t, pending); result.err != nil {
			t.Fatal(result.err)
		}
		if req["type"] != command {
			t.Fatalf("caller request changed: %v", req)
		}
	}
}
