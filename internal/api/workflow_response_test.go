package api_test

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

// Released Python 3.1.0 WorkflowsOutput explicitly serializes null priority and
// updated_at. The owner-approved HTTP exception preserves these as JSON null.
func TestHTTPWorkflowPreservesSDKNulls(t *testing.T) {
	for _, field := range []string{"Priority", "UpdatedAt"} {
		t.Run(field, func(t *testing.T) {
			ts, h := testserver.New(t, config.Config{EnableAggregates: true})
			record := record()
			record[field] = nil
			testserver.Connect(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
				protocol.MsgGetWorkflow:   func(map[string]any) map[string]any { return map[string]any{"output": record} },
				protocol.MsgListWorkflows: func(map[string]any) map[string]any { return map[string]any{"output": []any{record}} },
			})
			testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
			for _, route := range []struct{ suffix, method, body string }{{"/wf-1", "GET", ""}, {"", "GET", ""}, {"/search", "POST", "{}"}} {
				code, _, raw := testserver.Request(t, ts.URL+workflowRoot+route.suffix, route.method, route.body)
				if code != 200 {
					t.Errorf("%s null %s: status=%d body=%s", route.suffix, field, code, raw)
					continue
				}
				var row map[string]any
				if route.suffix == "/wf-1" {
					if err := json.Unmarshal([]byte(raw), &row); err != nil {
						t.Fatal(err)
					}
				} else {
					var rows []map[string]any
					if err := json.Unmarshal([]byte(raw), &rows); err != nil || len(rows) != 1 {
						t.Fatalf("rows=%s err=%v", raw, err)
					}
					row = rows[0]
				}
				key := map[string]string{"Priority": "priority", "UpdatedAt": "updatedAt"}[field]
				value, present := row[key]
				if !present || value != nil {
					t.Errorf("SDK null replaced or omitted: %s", raw)
				}
				assertLocalV2Schema(t, "Workflow", row)
			}
		})
	}
}

func TestHTTPWorkflowResponseIntegrity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response map[string]any
	}{
		{"missing output", map[string]any{}},
		{"wrong identity", map[string]any{"output": func() map[string]any { r := record(); r["WorkflowUUID"] = "another-workflow"; return r }()}},
	} {
		for _, suffix := range []string{"/wf-1", "/wf-1/steps"} {
			t.Run(tc.name+suffix, func(t *testing.T) {
				ts, h := testserver.New(t, config.Config{EnableAggregates: true})
				var stepCalls atomic.Int32
				testserver.Connect(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{
					protocol.MsgGetWorkflow: func(map[string]any) map[string]any { return tc.response },
					protocol.MsgListSteps:   func(map[string]any) map[string]any { stepCalls.Add(1); return map[string]any{"output": []any{}} },
				})
				testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
				code, ct, raw := testserver.Request(t, ts.URL+workflowRoot+suffix, "GET", "")
				if code != 502 || !strings.HasPrefix(ct, "application/problem+json") {
					t.Errorf("malformed workflow response: status=%d body=%s", code, raw)
				}
				if stepCalls.Load() != 0 {
					t.Error("steps dispatched after invalid workflow identity")
				}
			})
		}
	}
}

func TestHTTPWorkflowTimestampRange(t *testing.T) {
	ts, h := testserver.New(t, config.Config{EnableAggregates: true})
	record := record()
	record["CreatedAt"] = "-62135596800001"
	testserver.Connect(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{protocol.MsgGetWorkflow: func(map[string]any) map[string]any { return map[string]any{"output": record} }})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	code, _, raw := testserver.Request(t, ts.URL+workflowRoot+"/wf-1", "GET", "")
	if code != 502 {
		t.Fatalf("timestamp below supported range: status=%d body=%s", code, raw)
	}
}

func TestHTTPWorkflowPublishedNullability(t *testing.T) {
	ts, _ := testserver.New(t, config.Config{EnableAggregates: true})
	_, _, raw := testserver.Request(t, ts.URL+"/openapi.json", "GET", "")
	var spec map[string]any
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		t.Fatal(err)
	}
	properties := spec["components"].(map[string]any)["schemas"].(map[string]any)["Workflow"].(map[string]any)["properties"].(map[string]any)
	for _, field := range []string{"priority", "updatedAt"} {
		b, _ := json.Marshal(properties[field])
		if !strings.Contains(string(b), `"null"`) {
			t.Errorf("published %s schema disallows supported SDK null: %s", field, b)
		}
	}
}
