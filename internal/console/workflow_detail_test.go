package console_test

import (
	"html"
	"strings"
	"testing"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

func TestWorkflowInspectionCompleteValues(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	wf := testserver.Workflow("root", "SUCCESS")
	wf.Error = testserver.String("<failure>" + strings.Repeat("error", 100))
	wf.AuthenticatedUser = testserver.String("")
	wf.ParentWorkflowID = testserver.String("parent")
	wf.ApplicationVersion = testserver.String("postgres-gate-build-123456")
	long := "<script>" + strings.Repeat("opaque", 100)
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any { return map[string]any{"output": wf} },
		protocol.MsgListSteps: func(req map[string]any) map[string]any {
			if req["load_output"] != true {
				return map[string]any{"output": []protocol.WorkflowSteps{{FunctionID: 1, FunctionName: "step"}}}
			}
			return map[string]any{"output": []protocol.WorkflowSteps{{FunctionID: 1, FunctionName: "step", Output: testserver.String("step-only-" + long), Error: testserver.String(""), StartedAtEpochMS: testserver.String("0")}}}
		},
		protocol.MsgGetWorkflowEvents: func(req map[string]any) map[string]any { return map[string]any{"events": []protocol.EventOutput{}} },
		protocol.MsgGetWorkflowNotifications: func(req map[string]any) map[string]any {
			return map[string]any{"notifications": []protocol.NotificationOutput{}}
		},
		protocol.MsgGetWorkflowStreams: func(req map[string]any) map[string]any {
			return map[string]any{"streams": []protocol.StreamEntryOutput{{Key: "ordered", Values: []string{long, "", "last"}}}}
		},
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	for _, tc := range []struct {
		path string
		want []string
	}{
		{"", []string{`data-step-workflow="root"`, `data-step-id="1"`, `href="/apps/myapp/workflows/parent"`, "No events recorded.", "No notifications recorded.", "&lt;script&gt;" + strings.Repeat("opaque", 100)}},
		{"/inspect", []string{"&lt;failure&gt;" + strings.Repeat("error", 100), `data-field="wasForkedFrom"`, `data-field="user"`, `data-field="completedAt"`, "No events recorded.", "No notifications recorded.", "&lt;script&gt;" + strings.Repeat("opaque", 100)}},
		{"/inspect?step=1", []string{"step-only-&lt;script&gt;" + strings.Repeat("opaque", 100), `data-field="childWorkflowId"`, `data-field="completedAt"`}},
	} {
		code, body := testserver.Get(t, ts.URL+"/apps/myapp/workflows/root"+tc.path)
		if code != 200 {
			t.Fatalf("%s: status %d: %s", tc.path, code, body)
		}
		for _, want := range tc.want {
			if !strings.Contains(body, want) {
				t.Errorf("%s: inspection missing %q", tc.path, want)
			}
		}
	}
}

func TestWorkflowBlobPreservesMissingNullAndEmpty(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			id := req["workflow_id"].(string)
			wf := map[string]any{"WorkflowUUID": id}
			if id == "null" {
				wf["Output"] = nil
			}
			if id == "empty" {
				wf["Output"] = ""
			}
			return map[string]any{"output": wf}
		},
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	for _, tc := range []struct{ id, want string }{
		{"missing", "unavailable (not returned)"}, {"null", "null"}, {"empty", "&#34;&#34;"},
	} {
		_, body := testserver.Get(t, ts.URL+"/apps/myapp/workflows/"+tc.id+"/blob?kind=output")
		if !strings.Contains(body, tc.want) {
			t.Errorf("%s blob = %s, want %s", tc.id, body, tc.want)
		}
	}
}

func TestWorkflowChildBranchIdentityAndCycle(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": testserver.Workflow("child", "SUCCESS")}
		},
		protocol.MsgListSteps: func(req map[string]any) map[string]any {
			return map[string]any{"output": []protocol.WorkflowSteps{{FunctionID: 1, FunctionName: "invoke", ChildWorkflowID: testserver.String("root")}}}
		},
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	_, body := testserver.Get(t, ts.URL+"/apps/myapp/workflows/child/timeline?ancestor=root&branch=left")
	if !strings.Contains(body, "Workflow relationship cycle") {
		t.Fatalf("cycle not stopped: %s", body)
	}
	_, other := testserver.Get(t, ts.URL+"/apps/myapp/workflows/child/timeline?ancestor=other&branch=right")
	if !strings.Contains(other, `aria-expanded="false"`) || !strings.Contains(other, `data-branch-key=`) || strings.Contains(other, "Workflow relationship cycle") {
		t.Fatalf("separate branch controls absent: %s", other)
	}
}

func TestApplicationVersionExactControl(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", nil)
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	_, body := testserver.Get(t, ts.URL+"/")
	for _, want := range []string{`data-version="v1"`, `role="tooltip"`, `aria-label="Copy application version"`, `version-feedback`, `<article class="app-card"`} {
		if !strings.Contains(body, want) {
			t.Errorf("version missing %q", want)
		}
	}
}

func TestWorkflowChildReadOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing bool
		failure string
		want    string
	}{{"empty", false, "", "No steps recorded for this workflow."}, {"missing", true, "", `child workflow "child" not found`}, {"refused", false, "Private child steps unavailable", "Private child steps unavailable"}} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := testserver.New(t, config.Config{})
			testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
				protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
					if tc.missing {
						return map[string]any{"output": nil}
					}
					return map[string]any{"output": testserver.Workflow("child", "SUCCESS")}
				},
				protocol.MsgListSteps: func(req map[string]any) map[string]any {
					if tc.failure != "" {
						return map[string]any{"error_message": tc.failure}
					}
					if req["load_output"] != true {
						t.Error("child read suppressed step output")
					}
					return map[string]any{"output": []protocol.WorkflowSteps{}}
				},
			})
			testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
			_, body := testserver.Get(t, ts.URL+"/apps/myapp/workflows/child/timeline?ancestor=root&branch=left")
			if !strings.Contains(html.UnescapeString(body), tc.want) {
				t.Fatalf("%s outcome absent: %s", tc.name, body)
			}
		})
	}
}

func TestFailedChildWithoutStepsRetainsFailureStatus(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": testserver.Workflow("child", "ERROR")}
		},
		protocol.MsgListSteps: func(req map[string]any) map[string]any { return map[string]any{"output": []protocol.WorkflowSteps{}} },
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	_, body := testserver.Get(t, ts.URL+"/apps/myapp/workflows/child/timeline?ancestor=root&branch=left")
	if !strings.Contains(body, `data-child-status="ERROR"`) || !strings.Contains(body, "Child workflow status: ERROR") {
		t.Fatalf("failed empty child appears ordinary empty: %s", body)
	}
}

func TestChildTimelineUsesParentWindowAndCompactRows(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	testserver.Connect(t, ts, "myapp", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": testserver.Workflow("child", "SUCCESS")}
		},
		protocol.MsgListSteps: func(map[string]any) map[string]any {
			return map[string]any{"output": []protocol.WorkflowSteps{{FunctionID: 1, FunctionName: "child_step", ChildWorkflowID: testserver.String("grandchild"), StartedAtEpochMS: testserver.String("2000"), CompletedAtEpochMS: testserver.String("3000")}}}
		},
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	_, body := testserver.Get(t, ts.URL+"/apps/myapp/workflows/child/timeline?ancestor=root&branch=left&window_start=1000&window_end=5000")
	if !strings.Contains(body, "left:25.00%;width:25.00%") {
		t.Errorf("child must use parent [1000,5000] window: %s", body)
	}
	for _, unwanted := range []string{`class="tl-head"`, `class="child-navigation"`, "Child steps ·"} {
		if strings.Contains(body, unwanted) {
			t.Errorf("nested rows contain redundant layout %q", unwanted)
		}
	}
	for _, want := range []string{"window_start=1000", "window_end=5000", `class="tl-fold"`, `aria-label="Expand child workflow grandchild"`} {
		if !strings.Contains(body, want) {
			t.Errorf("compact child rows missing %q", want)
		}
	}
}
