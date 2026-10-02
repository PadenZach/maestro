package console_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

func TestWorkflowTimelineReadsBoundedPages(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	fake := testserver.Connect(t, ts, "app", "key", "exec", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": testserver.Workflow(req["workflow_id"].(string), "SUCCESS")}
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
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	status, body := testserver.Get(t, ts.URL+"/apps/app/workflows/root")
	if status != 200 || !strings.Contains(body, "Load more recorded steps") || strings.Contains(body, ">step-50 ") {
		t.Fatalf("unexpected first timeline page: status%d", status)
	}
	request := fake.Body(t, protocol.MsgGetWorkflow)
	if request["load_input"] != false || request["load_output"] != false {
		t.Errorf("detail header fetched blobs: %v", request)
	}
	_, body = testserver.Get(t, ts.URL+"/apps/app/workflows/root/timeline?offset=50")
	if !strings.Contains(body, "step-50") || !strings.Contains(body, "step-59") || strings.Contains(body, "Load more recorded steps") {
		t.Fatal("second timeline page incorrect")
	}
	if !strings.Contains(body, "offset=50") {
		t.Fatal("drawer page hint missing")
	}
}

func TestWorkflowDetailRetainsFlowWhenStepsUnavailable(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	testserver.Connect(t, ts, "app", "key", "exec", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": testserver.Workflow("root", "SUCCESS")}
		},
		protocol.MsgListSteps: func(map[string]any) map[string]any { return map[string]any{"error_message": "steps refused"} },
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	status, body := testserver.Get(t, ts.URL+"/apps/app/workflows/root")
	if status != 200 || !strings.Contains(body, "steps refused") || !strings.Contains(body, `id="flow-panel"`) {
		t.Fatal("unavailable steps hid the workflow and Flow controls")
	}
}
