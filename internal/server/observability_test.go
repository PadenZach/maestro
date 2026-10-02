package server_test

import (
	"strings"
	"testing"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

// staticList returns a LIST_WORKFLOWS handler that always replies with the given
// workflows (and an extra unknown field, to guard forward-compat decoding).
func staticList(wfs ...protocol.WorkflowsOutput) testserver.Responder {
	return func(req map[string]any) map[string]any {
		return map[string]any{
			"output":            wfs,
			"brand_new_field_2": "ignored-by-old-clients", // P3 additive evolution
		}
	}
}

func TestFakeUnknownCommandReportsExecutorError(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", nil)
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	code, body := testserver.Get(t, ts.URL+"/api/myapp/workflows/wf/events")
	if code != 502 || !strings.Contains(body, "Unknown message type") {
		t.Fatalf("unknown fake command masked: %d %s", code, body)
	}
}

func TestListWorkflows_JSONAndHTML(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	wf := testserver.Workflow("id_e1002bf4-01d9", "SUCCESS")
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgListWorkflows: staticList(wf),
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })

	// JSON surface.
	code, body := testserver.Get(t, ts.URL+"/api/myapp/workflows")
	if code != 200 {
		t.Fatalf("json status = %d", code)
	}
	if !strings.Contains(body, "id_e1002bf4-01d9") {
		t.Fatalf("json missing workflow id: %s", body)
	}

	// HTML console surface.
	code, html := testserver.Get(t, ts.URL+"/apps/myapp/workflows")
	if code != 200 {
		t.Fatalf("html status = %d", code)
	}
	if !strings.Contains(html, "id_e1002bf4-01d9") || !strings.Contains(html, "agentic_research_workflow") {
		t.Fatalf("html missing workflow row: %s", html)
	}
	if !strings.Contains(html, "htmx.min.js") {
		t.Fatalf("html should reference vendored htmx")
	}
}

func TestListWorkflows_FiltersOnWire(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	fe := testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgListWorkflows: staticList(),
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })

	testserver.Get(t, ts.URL+"/api/myapp/workflows?status=ERROR&name=wf&id_prefix=id_e1&offset=25")

	body := fe.Body(t, protocol.MsgListWorkflows)
	assertStrInList(t, body, "status", "ERROR")
	assertStrInList(t, body, "workflow_name", "wf")
	assertStrInList(t, body, "workflow_id_prefix", "id_e1")
	if got, ok := body["offset"].(float64); !ok || int(got) != 25 {
		t.Fatalf("offset not forwarded: %v", body["offset"])
	}
	if body["sort_desc"] != true {
		t.Fatalf("sort_desc should default true: %v", body["sort_desc"])
	}
}

func TestWorkflowDetail_Timeline(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	wf := testserver.Workflow("wf-1", "SUCCESS")
	steps := []protocol.WorkflowSteps{
		{FunctionID: 0, FunctionName: "research_topic", StartedAtEpochMS: testserver.String("1000"), CompletedAtEpochMS: testserver.String("3000")},
		{FunctionID: 1, FunctionName: "synthesise_topic", StartedAtEpochMS: testserver.String("3000"), CompletedAtEpochMS: testserver.String("3481")},
	}
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any { return map[string]any{"output": wf} },
		protocol.MsgListSteps:   func(req map[string]any) map[string]any { return map[string]any{"output": steps} },
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })

	code, html := testserver.Get(t, ts.URL+"/apps/myapp/workflows/wf-1")
	if code != 200 {
		t.Fatalf("detail status = %d: %s", code, html)
	}
	if !strings.Contains(html, "research_topic") || !strings.Contains(html, "synthesise_topic") {
		t.Fatalf("timeline missing steps: %s", html)
	}
	if !strings.Contains(html, "tl-bar") {
		t.Fatalf("timeline missing gantt bars: %s", html)
	}

	// JSON steps surface.
	code, j := testserver.Get(t, ts.URL+"/api/myapp/workflows/wf-1/steps")
	if code != 200 || !strings.Contains(j, "research_topic") {
		t.Fatalf("json steps wrong (%d): %s", code, j)
	}
}

func TestCancel_SendsCommandAndRerenders(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	wf := testserver.Workflow("wf-1", "CANCELLED")
	fe := testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any { return map[string]any{"output": wf} },
		protocol.MsgListSteps:   func(req map[string]any) map[string]any { return map[string]any{"output": []protocol.WorkflowSteps{}} },
		protocol.MsgCancel:      func(req map[string]any) map[string]any { return map[string]any{"success": true} },
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })

	code, html := testserver.Post(t, ts.URL+"/apps/myapp/workflows/wf-1/cancel")
	if code != 200 {
		t.Fatalf("cancel status = %d", code)
	}
	if !strings.Contains(html, "CANCELLED") {
		t.Fatalf("re-rendered live region should show new status: %s", html)
	}

	body := fe.Body(t, protocol.MsgCancel)
	if body["workflow_id"] != "wf-1" {
		t.Fatalf("cancel workflow_id wrong: %v", body["workflow_id"])
	}
	if body["cancel_children"] != false {
		t.Fatalf("cancel_children should be present and false: %v", body["cancel_children"])
	}
}

func TestCancel_ErrorShowsFlash(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	wf := testserver.Workflow("wf-1", "PENDING")
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any { return map[string]any{"output": wf} },
		protocol.MsgListSteps:   func(req map[string]any) map[string]any { return map[string]any{"output": []protocol.WorkflowSteps{}} },
		protocol.MsgCancel: func(req map[string]any) map[string]any {
			return map[string]any{"success": false, "error_message": "boom"}
		},
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })

	_, html := testserver.Post(t, ts.URL+"/apps/myapp/workflows/wf-1/cancel")
	if !strings.Contains(html, "Cancel failed: boom") {
		t.Fatalf("expected flash with executor error: %s", html)
	}
}

func TestQueues_JSONAndHTML(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	q := protocol.QueueOutput{Name: "default", PriorityEnabled: true, PollingIntervalSec: 0.5}
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgListQueues: func(req map[string]any) map[string]any {
			return map[string]any{"output": []protocol.QueueOutput{q}}
		},
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })

	code, j := testserver.Get(t, ts.URL+"/api/myapp/queues")
	if code != 200 || !strings.Contains(j, "default") {
		t.Fatalf("json queues wrong (%d): %s", code, j)
	}
	code, html := testserver.Get(t, ts.URL+"/apps/myapp/queues")
	if code != 200 || !strings.Contains(html, "default") {
		t.Fatalf("html queues wrong (%d): %s", code, html)
	}
}

func TestDispatcher_AppUnavailable(t *testing.T) {
	ts, _ := testserver.New(t, config.Config{})
	code, _ := testserver.Get(t, ts.URL+"/api/ghost/workflows")
	if code != 503 {
		t.Fatalf("expected 503 for unconnected app, got %d", code)
	}
}

func TestDispatcher_BrokenExecutorSurfacesError(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	// Single executor that drops the socket on the request: with no healthy
	// peer to retry, the dispatcher must surface an error (not hang).
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgListWorkflows: func(req map[string]any) map[string]any { return nil },
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })

	code, _ := testserver.Get(t, ts.URL+"/api/myapp/workflows")
	if code != 502 {
		t.Fatalf("expected 502 when the only executor drops, got %d", code)
	}
}

func TestDispatcher_RetriesHealthyPeer(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	wf := testserver.Workflow("wf-ok", "SUCCESS")
	// One broken executor, one healthy — regardless of pick order the request
	// must succeed (broken → errConnClosed → retry healthy).
	testserver.Connect(t, ts, "myapp", "testkey", "broken", map[protocol.MessageType]testserver.Responder{
		protocol.MsgListWorkflows: func(req map[string]any) map[string]any { return nil },
	})
	testserver.Connect(t, ts, "myapp", "testkey", "healthy", map[protocol.MessageType]testserver.Responder{
		protocol.MsgListWorkflows: staticList(wf),
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 2 })

	code, body := testserver.Get(t, ts.URL+"/api/myapp/workflows")
	if code != 200 || !strings.Contains(body, "wf-ok") {
		t.Fatalf("expected success via healthy peer, got %d: %s", code, body)
	}
}

func assertStrInList(t *testing.T, body map[string]any, key, want string) {
	t.Helper()
	v, ok := body[key]
	if !ok {
		t.Fatalf("body missing %q: %v", key, body)
	}
	arr, ok := v.([]any)
	if !ok {
		t.Fatalf("%q not a list: %v", key, v)
	}
	for _, e := range arr {
		if s, _ := e.(string); s == want {
			return
		}
	}
	t.Fatalf("%q = %v, want to contain %q", key, v, want)
}
