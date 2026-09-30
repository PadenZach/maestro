package api_test

import (
	"strings"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

func TestWorkflowDrawerReadsSelectedWorkflowAndStep(t *testing.T) {
	ts, h := newTestServer(t)
	dialFake(t, ts, "app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			id := req["workflow_id"].(string)
			if id == "missing" {
				return map[string]any{"output": nil}
			}
			wf := sampleWorkflow(id, "ERROR")
			wf.Input = strp("selected-input-" + id)
			return map[string]any{"output": wf}
		},
		protocol.MsgListSteps: func(req map[string]any) map[string]any {
			if req["load_output"] != true {
				t.Error("drawer step output was suppressed")
			}
			return map[string]any{"output": []map[string]any{
				{"function_id": 1, "function_name": "selected-step", "output": "<opaque-child-value>", "error": nil, "child_workflow_id": nil, "started_at_epoch_ms": "0", "completed_at_epoch_ms": nil},
			}}
		},
		protocol.MsgGetWorkflowEvents: func(req map[string]any) map[string]any {
			return map[string]any{"events": []map[string]any{{"key": "owner", "value": req["workflow_id"]}}}
		},
		protocol.MsgGetWorkflowNotifications: func(map[string]any) map[string]any { return map[string]any{"notifications": []any{}} },
		protocol.MsgGetWorkflowStreams: func(map[string]any) map[string]any {
			return map[string]any{"error_message": "stream inspection refused"}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, tc := range []struct {
		path string
		want []string
	}{
		{"child/inspect?step=1", []string{`data-inspected-workflow="child"`, `data-inspected-step="1"`, "selected-step", "&lt;opaque-child-value&gt;", `data-field="completedAt"`}},
		{"child/inspect", []string{`data-inspected-workflow="child"`, "selected-input-child", "No notifications recorded.", "stream inspection refused", `href="/apps/app/workflows/child"`}},
		{"child/inspect?step=42", []string{"Step 42", "unavailable"}},
		{"missing/inspect", []string{"not found"}},
		{"child/inspect?step=bad", []string{"invalid step"}},
	} {
		_, body := getBody(t, ts.URL+"/apps/app/workflows/"+tc.path)
		for _, want := range tc.want {
			if !strings.Contains(body, want) {
				t.Errorf("%s missing %q", tc.path, want)
			}
		}
	}
}
