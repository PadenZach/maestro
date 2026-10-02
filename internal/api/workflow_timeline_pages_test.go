package api_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

func TestWorkflowTimelineReadsBoundedPages(t *testing.T) {
	ts, h := newTestServer(t)
	fake := dialFake(t, ts, "app", "key", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": sampleWorkflow(req["workflow_id"].(string), "SUCCESS")}
		},
		protocol.MsgListSteps: func(req map[string]any) map[string]any {
			if req["limit"] != float64(51) || req["load_output"] != true {
				t.Errorf("unbounded step read: %v", req)
			}
			offset := int(req["offset"].(float64))
			rows := []map[string]any{}
			for i := offset; i < offset+51 && i < 60; i++ {
				rows = append(rows, map[string]any{"function_id": i, "function_name": fmt.Sprintf("step-%d", i), "output": "result", "error": nil})
			}
			return map[string]any{"output": rows}
		},
		protocol.MsgGetWorkflowEvents:        func(map[string]any) map[string]any { return map[string]any{"events": []any{}} },
		protocol.MsgGetWorkflowNotifications: func(map[string]any) map[string]any { return map[string]any{"notifications": []any{}} },
		protocol.MsgGetWorkflowStreams:       func(map[string]any) map[string]any { return map[string]any{"streams": []any{}} },
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	status, body := getBody(t, ts.URL+"/apps/app/workflows/root")
	if status != 200 || !strings.Contains(body, "Load more recorded steps") || strings.Contains(body, ">step-50 ") {
		t.Fatalf("unexpected first timeline page: status%d", status)
	}
	request := fake.body(t, protocol.MsgGetWorkflow)
	if request["load_input"] != false || request["load_output"] != false {
		t.Errorf("detail header fetched blobs: %v", request)
	}
	_, body = getBody(t, ts.URL+"/apps/app/workflows/root/timeline?offset=50")
	if !strings.Contains(body, "step-50") || !strings.Contains(body, "step-59") || strings.Contains(body, "Load more recorded steps") {
		t.Fatal("second timeline page incorrect")
	}
	if !strings.Contains(body, "offset=50") {
		t.Fatal("drawer page hint missing")
	}
}

func TestWorkflowDetailRetainsFlowWhenStepsUnavailable(t *testing.T) {
	ts, h := newTestServer(t)
	dialFake(t, ts, "app", "key", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": sampleWorkflow("root", "SUCCESS")}
		},
		protocol.MsgListSteps: func(map[string]any) map[string]any { return map[string]any{"error_message": "steps refused"} },
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	status, body := getBody(t, ts.URL+"/apps/app/workflows/root")
	if status != 200 || !strings.Contains(body, "steps refused") || !strings.Contains(body, `id="flow-panel"`) {
		t.Fatal("unavailable steps hid the workflow and Flow controls")
	}
}
