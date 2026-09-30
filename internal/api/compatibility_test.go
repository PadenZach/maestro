package api_test

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

func TestSuccessFalseWithoutMessage(t *testing.T) {
	for _, command := range []protocol.MessageType{protocol.MsgCancel, protocol.MsgResume} {
		for _, response := range []struct {
			name  string
			value map[string]any
		}{
			{"false", map[string]any{"success": false}},
			{"missing", map[string]any{}},
		} {
			t.Run(string(command)+"/"+response.name, func(t *testing.T) {
				ts, h := newTestServer(t)
				var attempts atomic.Int32
				handlers := map[protocol.MessageType]respondFn{
					protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
						return map[string]any{"output": sampleWorkflow("wf-1", "PENDING")}
					},
					protocol.MsgListSteps: func(map[string]any) map[string]any { return map[string]any{"output": []protocol.WorkflowSteps{}} },
					command:               func(map[string]any) map[string]any { attempts.Add(1); return response.value },
				}
				dialFake(t, ts, "app", "testkey", "one", handlers)
				dialFake(t, ts, "app", "testkey", "two", handlers)
				waitFor(t, func() bool { return len(h.Executors()) == 2 })
				code, html := postBody(t, ts.URL+"/apps/app/workflows/wf-1/"+string(command))
				if code != 200 || !strings.Contains(html, strings.Title(string(command))+" failed:") || !strings.Contains(html, "unsuccessful") {
					t.Fatalf("failed mutation shown as success: %d %s", code, html)
				}
				if got := attempts.Load(); got != 1 {
					t.Fatalf("ambiguous mutation retried: %d", got)
				}
			})
		}
	}
}

func TestMetadataOnlyRefusalKeepsConnection(t *testing.T) {
	ts, h := newTestServer(t)
	refusal := `<private & unavailable>`
	handlers := map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": sampleWorkflow("wf-1", "SUCCESS")}
		},
		protocol.MsgListSteps: func(map[string]any) map[string]any { return map[string]any{"output": []protocol.WorkflowSteps{}} },
	}
	for _, typ := range []protocol.MessageType{protocol.MsgGetWorkflowEvents, protocol.MsgGetWorkflowNotifications, protocol.MsgGetWorkflowStreams} {
		handlers[typ] = func(map[string]any) map[string]any { return map[string]any{"error_message": refusal} }
	}
	dialFake(t, ts, "app", "testkey", "one", handlers)
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, route := range []string{"events", "notifications", "streams"} {
		code, body := getBody(t, ts.URL+"/api/app/workflows/wf-1/"+route)
		var response map[string]string
		if err := json.Unmarshal([]byte(body), &response); err != nil {
			t.Fatal(err)
		}
		if code != 502 || response["error"] != refusal {
			t.Fatalf("%s masked refusal: %d %s", route, code, body)
		}
	}
	code, html := getBody(t, ts.URL+"/apps/app/workflows/wf-1")
	if code != 200 || strings.Contains(html, refusal) || strings.Count(html, "&lt;private &amp; unavailable&gt;") != 3 {
		t.Fatalf("private panels empty, omitted or unescaped: %d %s", code, html)
	}
	if len(h.Executors()) != 1 {
		t.Fatal("refusal closed healthy socket")
	}
	code, body := getBody(t, ts.URL+"/api/app/workflows/wf-1")
	if code != 200 || !strings.Contains(body, "wf-1") {
		t.Fatalf("following read failed: %d %s", code, body)
	}
}

// A structured privacy refusal is final even if another executor could serve
// the same read: retrying would bypass the refusing executor's local policy.
func TestMetadataRefusalDoesNotRetryAnotherPeer(t *testing.T) {
	ts, h := newTestServer(t)
	var attempts atomic.Int32
	handlers := map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any {
			attempts.Add(1)
			return map[string]any{"error_message": "metadata only"}
		},
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": sampleWorkflow("wf-1", "SUCCESS")}
		},
	}
	dialFake(t, ts, "app", "testkey", "one", handlers)
	dialFake(t, ts, "app", "testkey", "two", handlers)
	waitFor(t, func() bool { return len(h.Executors()) == 2 })
	code, body := getBody(t, ts.URL+"/api/app/workflows/wf-1/events")
	if code != 502 || !strings.Contains(body, "metadata only") || attempts.Load() != 1 {
		t.Fatalf("refusal bypassed or masked: status=%d body=%s attempts=%d", code, body, attempts.Load())
	}
	code, body = getBody(t, ts.URL+"/api/app/workflows/wf-1")
	if code != 200 || !strings.Contains(body, "wf-1") || len(h.Executors()) != 2 {
		t.Fatalf("refusal broke connections: %d %s", code, body)
	}
}

func TestDataOnlyBaseResponseIsUnavailable(t *testing.T) {
	ts, h := newTestServer(t)
	dialFake(t, ts, "app", "testkey", "one", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any { return map[string]any{} },
		protocol.MsgListSteps:         func(map[string]any) map[string]any { return map[string]any{} },
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": sampleWorkflow("wf-1", "SUCCESS")}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	code, body := getBody(t, ts.URL+"/api/app/workflows/wf-1/events")
	if code != 502 || !strings.Contains(body, "unavailable") {
		t.Fatalf("data-only refusal shown as success: %d %s", code, body)
	}
	code, html := getBody(t, ts.URL+"/apps/app/workflows/wf-1/timeline")
	if !strings.Contains(html, "unavailable") || strings.Contains(html, "<div class=\"timeline\"") {
		t.Fatalf("HTMX empty successful panel: %d %s", code, html)
	}
	if len(h.Executors()) != 1 {
		t.Fatal("data-only reply closed socket")
	}
}

func TestHTMXRefusalEscapesHostileMessage(t *testing.T) {
	ts, h := newTestServer(t)
	dialFake(t, ts, "app", "testkey", "one", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": sampleWorkflow("wf-1", "SUCCESS")}
		},
		protocol.MsgListSteps: func(map[string]any) map[string]any {
			return map[string]any{"error_message": `<script>alert("x")</script>&`}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	_, body := getBody(t, ts.URL+"/apps/app/workflows/wf-1/timeline")
	if strings.Contains(body, "<script>") || !strings.Contains(body, "&lt;script&gt;") || !strings.Contains(body, "&amp;") {
		t.Fatalf("unescaped HTMX refusal: %s", body)
	}
}
