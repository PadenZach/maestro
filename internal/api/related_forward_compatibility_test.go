package api_test

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

// An untested SDK identity is not evidence that an existing read cannot work.
// Exercise both HTTP surfaces: Console reads use the same hub dispatch as /api.
func TestRelatedReadsAttemptUnrecognizedSDKs(t *testing.T) {
	ts, h := localV2Server(t)
	var calls atomic.Int32
	handlers := map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
		},
	}
	for command, response := range relatedWireResponses() {
		handlers[command] = func(map[string]any) map[string]any { calls.Add(1); return response }
	}
	// No SDK identity is needed to demonstrate support for the wire commands.
	dialScheduleConsoleFake(t, ts.URL, "fixture-app", "peer", "", "", handlers)
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, root := range []string{localV2RelatedWorkflowRoot, "/api/fixture-app/workflows"} {
		for _, suffix := range []string{"events", "notifications", "streams"} {
			code, _, body := localV2Request(t, ts.URL+root+"/wf-1/"+suffix, "GET", "")
			if code != 200 || !strings.HasPrefix(strings.TrimSpace(body), "[") || strings.TrimSpace(body) == "[]" {
				t.Errorf("%s/%s: status=%d body=%s", root, suffix, code, body)
			}
		}
	}
	if calls.Load() != 6 {
		t.Errorf("related dispatches=%d, want 6", calls.Load())
	}
}

func TestRelatedReadsSurfaceActualErrorsFromUnrecognizedSDK(t *testing.T) {
	for _, response := range []struct {
		name   string
		wire   map[string]any
		detail string
	}{
		{"unsupported command", map[string]any{"error_message": "Unknown message type: get_workflow_events"}, "Unknown message type"},
		{"privacy refusal", map[string]any{"error_message": "metadata-only mode refuses events"}, "metadata-only mode refuses events"},
		{"malformed response", map[string]any{"events": nil}, "missing or null"},
	} {
		t.Run(response.name, func(t *testing.T) {
			ts, h := localV2Server(t)
			var calls atomic.Int32
			for _, id := range []string{"one", "two"} {
				dialScheduleConsoleFake(t, ts.URL, "fixture-app", id, "", "", map[protocol.MessageType]respondFn{
					protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
						return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
					},
					protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any { calls.Add(1); return response.wire },
				})
			}
			waitFor(t, func() bool { return len(h.Executors()) == 2 })
			code, _, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/wf-1/events", "GET", "")
			if code != 502 || !strings.Contains(body, response.detail) || calls.Load() != 1 {
				t.Fatalf("status=%d dispatches=%d body=%s", code, calls.Load(), body)
			}
		})
	}
}
