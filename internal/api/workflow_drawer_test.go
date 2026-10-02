package api_test

import (
	"fmt"
	"strings"
	"sync/atomic"
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

func TestWorkflowDrawerUsesBoundedPageHint(t *testing.T) {
	ts, h := newTestServer(t)
	var calls atomic.Int32
	dialFake(t, ts, "app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": sampleWorkflow("root", "SUCCESS")}
		},
		protocol.MsgListSteps: func(req map[string]any) map[string]any {
			calls.Add(1)
			if req["limit"] != float64(50) || req["offset"] != float64(50) || req["load_output"] != true {
				t.Errorf("step page = %v", req)
			}
			return map[string]any{"output": []map[string]any{{"function_id": 900, "function_name": "hinted-step", "error": nil}}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	_, body := getBody(t, ts.URL+"/apps/app/workflows/root/inspect?step=900&offset=50")
	if !strings.Contains(body, "hinted-step") || calls.Load() != 1 {
		t.Fatalf("hinted drawer = %s; calls=%d", body, calls.Load())
	}
	_, body = getBody(t, ts.URL+"/apps/app/workflows/root/inspect?step=901&offset=50")
	if !strings.Contains(body, "unavailable after refresh") || calls.Load() != 2 {
		t.Fatalf("missing hinted record = %s; calls=%d", body, calls.Load())
	}
	_, body = getBody(t, ts.URL+"/apps/app/workflows/root/inspect?step=900&offset=1")
	if !strings.Contains(body, "offset must") || calls.Load() != 2 {
		t.Fatalf("invalid page hint = %s", body)
	}
}

func TestWorkflowDrawerBoundsLegacyScan(t *testing.T) {
	ts, h := newTestServer(t)
	var calls atomic.Int32
	dialFake(t, ts, "app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": sampleWorkflow("root", "SUCCESS")}
		},
		protocol.MsgListSteps: func(req map[string]any) map[string]any {
			page := calls.Add(1) - 1
			if req["limit"] != float64(50) || req["offset"] != float64(page*50) {
				t.Errorf("legacy page = %v", req)
			}
			steps := []map[string]any{}
			for i := 0; i < 50; i++ {
				steps = append(steps, map[string]any{"function_id": int(page)*50 + i, "function_name": fmt.Sprintf("step-%d", i)})
			}
			return map[string]any{"output": steps}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	_, body := getBody(t, ts.URL+"/apps/app/workflows/root/inspect?step=9999")
	if calls.Load() != 20 || !strings.Contains(body, "bounded inspection read") {
		t.Fatalf("unbounded legacy lookup: calls=%d; body=%s", calls.Load(), body)
	}
}

func TestWorkflowDrawerDoesNotInventStepsFromNullData(t *testing.T) {
	ts, h := newTestServer(t)
	dialFake(t, ts, "app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": sampleWorkflow("root", "SUCCESS")}
		},
		protocol.MsgListSteps: func(map[string]any) map[string]any { return map[string]any{"output": nil} },
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	_, body := getBody(t, ts.URL+"/apps/app/workflows/root/inspect?step=0&offset=0")
	if !strings.Contains(body, "executor step data unavailable") || strings.Contains(body, "unavailable after refresh") {
		t.Fatalf("null steps misclassified: %s", body)
	}
}
