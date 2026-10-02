package api_test

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
	"github.com/zpaden/maestro/internal/web"
)

func flowResponse(t *testing.T, target string) (int, web.Flow, string) {
	t.Helper()
	code, raw := getBody(t, target)
	var f web.Flow
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		t.Fatalf("decode flow: %v: %s", err, raw)
	}
	return code, f, raw
}

func TestWorkflowFlowBoundedPageAndEvidence(t *testing.T) {
	ts, h := newTestServer(t)
	var workflowCalls, stepCalls atomic.Int32
	dialFake(t, ts, "app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			workflowCalls.Add(1)
			if req["load_input"] != false || req["load_output"] != false {
				t.Error("metadata payload requested")
			}
			wf := sampleWorkflow("root", "SUCCESS")
			wf.Output, wf.Error = strp("opaque-workflow-output"), strp("opaque-workflow-error")
			return map[string]any{"output": wf}
		},
		protocol.MsgListSteps: func(req map[string]any) map[string]any {
			stepCalls.Add(1)
			if req["limit"] != float64(51) || req["offset"] != float64(50) || req["load_output"] != true {
				t.Errorf("unbounded or error-suppressing read: %v", req)
			}
			steps := []map[string]any{}
			for i := 0; i < 51; i++ {
				steps = append(steps, map[string]any{"function_id": 100 + i, "function_name": "DBOS.getResult", "child_workflow_id": "child", "output": "opaque-step-output", "error": "opaque-step-error"})
			}
			return map[string]any{"output": steps}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	code, f, raw := flowResponse(t, ts.URL+"/apps/app/workflows/root/flow?offset=50")
	if code != 200 || f.State != "ready" || !f.DetailsLoaded || len(f.Steps) != 50 || !f.HasMore || f.NextOffset == nil || *f.NextOffset != 100 {
		t.Fatalf("flow = %s", raw)
	}
	if !f.Steps[0].HasError || !f.Steps[0].ErrorKnown || f.Steps[0].Relationship != "return" || f.Status == nil || *f.Status != "SUCCESS" {
		t.Fatalf("recorded outcomes lost: %s", raw)
	}
	if strings.Contains(raw, "opaque-") {
		t.Fatalf("payload retained in flow: %s", raw)
	}
	if workflowCalls.Load() != 1 || stepCalls.Load() != 1 {
		t.Fatal("Flow recursively read related workflows")
	}
}

func TestWorkflowFlowReadStates(t *testing.T) {
	for _, tc := range []struct {
		name            string
		workflow, steps map[string]any
		state           string
		retains         bool
		stepCalls       int32
	}{
		{"missing", map[string]any{"output": nil}, nil, "missing", false, 0},
		{"workflow omission", map[string]any{}, nil, "error", false, 0},
		{"workflow refusal", map[string]any{"error_message": "workflow read refused"}, nil, "error", false, 0},
		{"wrong identity", map[string]any{"output": sampleWorkflow("other", "ERROR")}, nil, "error", false, 0},
		{"step refusal", map[string]any{"output": sampleWorkflow("root", "SUCCESS")}, map[string]any{"error_message": "steps refused"}, "error", true, 1},
		{"step null", map[string]any{"output": sampleWorkflow("root", "SUCCESS")}, map[string]any{"output": nil}, "error", true, 1},
		{"step omission", map[string]any{"output": sampleWorkflow("root", "SUCCESS")}, map[string]any{}, "error", true, 1},
		{"step type invalid", map[string]any{"output": sampleWorkflow("root", "SUCCESS")}, map[string]any{"output": []any{map[string]any{"function_id": "one"}}}, "error", true, 1},
		{"empty", map[string]any{"output": sampleWorkflow("root", "SUCCESS")}, map[string]any{"output": []any{}}, "ready", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := newTestServer(t)
			var calls atomic.Int32
			dialFake(t, ts, "app", "testkey", "exec", map[protocol.MessageType]respondFn{
				protocol.MsgGetWorkflow: func(map[string]any) map[string]any { return tc.workflow },
				protocol.MsgListSteps:   func(map[string]any) map[string]any { calls.Add(1); return tc.steps },
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			_, f, raw := flowResponse(t, ts.URL+"/apps/app/workflows/root/flow")
			if f.State != tc.state || (f.Status != nil) != tc.retains || f.WorkflowLoaded != tc.retains || f.Steps == nil || calls.Load() != tc.stepCalls {
				t.Fatalf("flow = %s; calls=%d", raw, calls.Load())
			}
		})
	}
	ts, _ := newTestServer(t)
	_, f, raw := flowResponse(t, ts.URL+"/apps/app/workflows/root/flow")
	if f.State != "unavailable" {
		t.Fatalf("disconnected app = %s", raw)
	}
}

func TestWorkflowFlowLimitsAndValidation(t *testing.T) {
	ts, h := newTestServer(t)
	var calls atomic.Int32
	dialFake(t, ts, "app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": sampleWorkflow("root", "PENDING")}
		},
		protocol.MsgListSteps: func(map[string]any) map[string]any {
			rows := []any{}
			for i := 0; i < 51; i++ {
				rows = append(rows, map[string]any{"function_id": i})
			}
			return map[string]any{"output": rows}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, query := range []string{"offset=-50", "offset=1", "offset=1050", "offset=50&offset=50", "unknown=yes", "offset=%zz", "details=", "details=1", "details=false&details=true"} {
		code, _ := getBody(t, ts.URL+"/apps/app/workflows/root/flow?"+query)
		if code != 400 {
			t.Errorf("%s: status=%d", query, code)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid query dispatched")
	}
	_, f, raw := flowResponse(t, ts.URL+"/apps/app/workflows/root/flow?offset=1000")
	if !f.Limited || !f.HasMore || f.NextOffset != nil {
		t.Fatalf("offset cap = %s", raw)
	}
}

func TestWorkflowFlowMetadataDiscoveryDoesNotLoadDetails(t *testing.T) {
	ts, h := newTestServer(t)
	var workflowCalls, stepCalls atomic.Int32
	dialFake(t, ts, "app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			workflowCalls.Add(1)
			if req["load_input"] != false || req["load_output"] != false {
				t.Error("workflow payload requested")
			}
			return map[string]any{"output": sampleWorkflow("root", "ERROR")}
		},
		protocol.MsgListSteps: func(req map[string]any) map[string]any {
			stepCalls.Add(1)
			if req["limit"] != float64(51) || req["offset"] != float64(0) || req["load_output"] != false {
				t.Errorf("metadata step read = %v", req)
			}
			// Even if an executor ignores the flag, a discovery response does not
			// establish that complete error evidence was requested.
			return map[string]any{"output": []any{map[string]any{"function_id": 2, "function_name": "DBOS.getResult", "child_workflow_id": "child", "output": "opaque-output", "error": "opaque-error"}}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	code, f, raw := flowResponse(t, ts.URL+"/apps/app/workflows/root/flow?details=false")
	if code != 200 || f.State != "ready" || !f.WorkflowLoaded || f.DetailsLoaded || len(f.Steps) != 1 {
		t.Fatalf("metadata flow = %s", raw)
	}
	if f.Steps[0].HasError || f.Steps[0].ErrorKnown || f.Steps[0].Relationship != "return" || f.Steps[0].ChildWorkflowID == nil || *f.Steps[0].ChildWorkflowID != "child" {
		t.Fatalf("discovery evidence = %s", raw)
	}
	if strings.Contains(raw, "opaque-") || workflowCalls.Load() != 1 || stepCalls.Load() != 1 {
		t.Fatalf("unbounded discovery or payload: %s", raw)
	}
}

func TestWorkflowFlowStatusBatch(t *testing.T) {
	ts, h := newTestServer(t)
	var calls atomic.Int32
	dialFake(t, ts, "app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(req map[string]any) map[string]any {
			calls.Add(1)
			body := req["body"].(map[string]any)
			ids := body["workflow_uuids"].([]any)
			if len(ids) != 2 || body["limit"] != float64(2) || body["load_input"] != false || body["load_output"] != false {
				t.Errorf("batch = %v", body)
			}
			return map[string]any{"output": []any{sampleWorkflow("child", "PENDING")}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	root := ts.URL + "/apps/app/workflows/root/flow-status?"
	code, raw := getBody(t, root+"workflow_id=child&workflow_id=missing")
	var response struct {
		State     string
		Workflows []web.Flow
	}
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatal(err)
	}
	if code != 200 || response.State != "ready" || len(response.Workflows) != 2 || response.Workflows[0].Status == nil || *response.Workflows[0].Status != "PENDING" || response.Workflows[1].State != "missing" {
		t.Fatalf("status batch = %s", raw)
	}
	q := url.Values{}
	for i := 0; i < 26; i++ {
		q.Add("workflow_id", fmt.Sprintf("child-%d", i))
	}
	for _, query := range []string{"", "workflow_id=", "workflow_id=x&workflow_id=x", q.Encode()} {
		code, _ := getBody(t, root+query)
		if code != 400 {
			t.Errorf("invalid status query: %d", code)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("batch dispatches=%d", calls.Load())
	}
}
