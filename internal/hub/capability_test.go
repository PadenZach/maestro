package hub

import (
	"context"
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

func TestCapabilityPolicyRejectsUnsupportedWithoutSending(t *testing.T) {
	for _, tc := range []struct {
		name, language, version, typ string
		body                         any
	}{
		{"old_attributes", "python", "2.24.0", "list_workflows", map[string]any{"attributes": map[string]any{"team": "only"}}},
		{"old_queued_app", "python", "2.24.0", "list_queued_workflows", map[string]any{"application_name": []string{"private"}}},
		{"old_queue_app", "python", "2.24.0", "list_queues", map[string]any{"application_name": []string{"private"}}},
		{"unknown_sdk", "python", "3.1.1", "list_workflows", map[string]any{"application_name": []string{"private"}}},
		{"semver_prerelease", "python", "3.1.0rc1", "list_workflows", map[string]any{"schedule_name": []string{"nightly"}}},
		{"wrong_language", "typescript", "3.1.0", "list_workflows", map[string]any{"attributes": map[string]any{"team": "only"}}},
		{"unknown_language", "", "3.1.0", "list_workflows", map[string]any{"attributes": map[string]any{"team": "only"}}},
		{"app_version_not_sdk", "python", "2.24.0", "list_workflows", map[string]any{"schedule_name": []string{"nightly"}}},
		{"no_restart_3", "python", "3.1.0", "restart", nil},
		{"no_rewind_2", "python", "2.31.1", "rewind_workflow", nil},
		{"unknown_mutation", "python", "3.1.1", "rewind_workflow", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, ts := testHub(t, time.Second)
			versionedPeer(t, ts.URL, tc.language, tc.version, "3.1.0")
			registered(t, h, 1)
			req := protocol.Request{"type": tc.typ}
			if tc.body != nil {
				req["body"] = tc.body
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := h.Request(ctx, "app", req)
			if err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unsafe request sent or accepted: %v", err)
			}
			waitPending(t, h.conns("app")[0], 0)
		})
	}
}

// A MessageType value serializes to the same JSON string as a plain string;
// capability checks must inspect both before choosing a peer or sending a frame.
func TestCapabilityPolicyRejectsTypedAndInvalidDiscriminatorsWithoutSending(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  any
		body any
	}{
		{"typed_workflow_filter", protocol.MsgListWorkflows, map[string]any{"attributes": map[string]any{"team": "private"}}},
		{"typed_queued_filter", protocol.MsgListQueuedWorkflows, map[string]any{"schedule_name": []string{"secret"}}},
		{"typed_queue_filter", protocol.MsgListQueues, map[string]any{"application_name": []string{"private"}}},
		{"typed_rewind", protocol.MsgRewindWorkflow, nil},
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

func TestCapabilityPolicyAllowsReviewedPairs(t *testing.T) {
	for _, tc := range []struct {
		version, typ string
		body         any
	}{
		{"2.24.0", "restart", nil},
		{"2.31.1", "restart", nil},
		{"3.1.0", "rewind_workflow", nil},
		{"2.31.1", "list_workflows", map[string]any{"attributes": map[string]any{"region": "west"}}},
		{"3.1.0", "list_queues", map[string]any{"application_name": []string{"app"}}},
	} {
		t.Run(tc.version+"/"+tc.typ, func(t *testing.T) {
			h, ts := testHub(t, time.Second)
			c := versionedPeer(t, ts.URL, "python", tc.version, "app-v1")
			registered(t, h, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req := protocol.Request{"type": tc.typ}
			if tc.body != nil {
				req["body"] = tc.body
			}
			ch := request(h, ctx, req)
			_, wire := frame(t, ctx, c)
			if wire["type"] != tc.typ {
				t.Fatalf("unexpected wire: %v", wire)
			}
			reply(t, ctx, c, map[string]any{"type": tc.typ, "request_id": wire["request_id"], "success": true})
			if got := await(t, ch); got.err != nil {
				t.Fatal(got.err)
			}
		})
	}
}

// Post-fix characterization: normalization also preserves correlation with a
// reviewed peer and leaves the caller's original request untouched.
func TestCapabilityPolicyTypedRequestSelectsReviewedPeer(t *testing.T) {
	h, ts := testHub(t, time.Second)
	old := versionedPeer(t, ts.URL, "python", "2.24.0", "app-v1")
	newer := versionedPeer(t, ts.URL, "python", "3.1.0", "app-v1")
	registered(t, h, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req := protocol.Request{"type": protocol.MsgListWorkflows, "body": map[string]any{"attributes": map[string]any{"team": "only"}}}
	first := request(h, ctx, req)
	_, wire := frame(t, ctx, newer)
	if wire["type"] != string(protocol.MsgListWorkflows) {
		t.Fatalf("request not sent to reviewed peer: %v", wire)
	}
	reply(t, ctx, newer, map[string]any{"type": "list_workflows", "request_id": wire["request_id"], "output": []any{}})
	if got := await(t, first); got.err != nil {
		t.Fatal(got.err)
	}
	if req["type"] != protocol.MsgListWorkflows {
		t.Fatalf("caller request changed: %v", req)
	}
	// A safe read is the old peer's next frame; the filtered read wasn't sent there.
	newer.CloseNow()
	registered(t, h, 1)
	next := request(h, ctx, protocol.GetWorkflowRequest("safe", false, false))
	_, wire = frame(t, ctx, old)
	if wire["type"] != "get_workflow" || wire["workflow_id"] != "safe" {
		t.Fatalf("unsupported request reached old peer: %v", wire)
	}
	reply(t, ctx, old, map[string]any{"type": "get_workflow", "request_id": wire["request_id"], "output": nil})
	if got := await(t, next); got.err != nil {
		t.Fatal(got.err)
	}
}

func TestCapabilityPolicySelectsAndRechecksPeers(t *testing.T) {
	h, ts := testHub(t, time.Second)
	old := versionedPeer(t, ts.URL, "python", "2.24.0", "app-v1")
	_ = old
	newer := versionedPeer(t, ts.URL, "python", "3.1.0", "app-v1")
	registered(t, h, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := protocol.Request{"type": "list_queued_workflows", "body": map[string]any{"attributes": map[string]any{"team": "only"}}}
	first := request(h, ctx, req)
	_, m := frame(t, ctx, newer)
	if m["type"] != "list_queued_workflows" {
		t.Fatalf("wrong peer/request: %v", m)
	}
	// A read may retry after this eligible peer drops, but not via an old peer.
	_ = newer.CloseNow()
	r := await(t, first)
	if r.err == nil {
		t.Fatalf("retry bypassed capability: %s", r.data)
	}
	registered(t, h, 1)
	second := await(t, request(h, ctx, req))
	if second.err == nil || errors.Is(second.err, context.DeadlineExceeded) {
		t.Fatalf("old peer received retry: %v", second.err)
	}
}
