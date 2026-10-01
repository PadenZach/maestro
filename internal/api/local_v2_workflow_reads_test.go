package api_test

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

func TestLocalHTTPV2ListWorkflowsAndExpandedSearchOptions(t *testing.T) {
	ts, h := localV2Server(t)
	var calls atomic.Int32
	fe := dialFake(t, ts, "fixture-app", "testkey", "workflow-reader", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": []any{}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	t.Run("official GET list", func(t *testing.T) {
		code, contentType, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"?status=SUCCESS&workflowName=job&limit=0&offset=0&sortDesc=false&loadInput=true&loadOutput=false", "GET", "")
		if code != 200 || !strings.HasPrefix(contentType, "application/json") || strings.TrimSpace(raw) != "[]" {
			t.Fatalf("list workflows: %d %s %s", code, contentType, raw)
		}
		wire := fe.body(t, protocol.MsgListWorkflows)
		for key, expected := range map[string]any{
			"status": []any{"SUCCESS"}, "workflow_name": []any{"job"},
			"limit": float64(0), "offset": float64(0), "sort_desc": false,
			"load_input": true, "load_output": false, "queues_only": false,
		} {
			if actual, ok := wire[key]; !ok || !jsonValuesEqual(actual, expected) {
				t.Errorf("GET wire %s = %#v; want %#v", key, actual, expected)
			}
		}
	})

	t.Run("expanded POST search", func(t *testing.T) {
		body := `{"attributes":{"tenant":"blue"},"completedAfter":"2024-01-01T00:00:00Z","completedBefore":"2025-01-01T00:00:00Z","dequeuedAfter":"2024-02-01T00:00:00Z","dequeuedBefore":"2024-12-01T00:00:00Z","executorId":["exec-1"],"forkedFrom":["source-1"],"hasParent":false,"loadInput":true,"loadOutput":false,"parentWorkflowId":["parent-1"],"scheduleName":["daily"],"wasForkedFrom":false,"workflowIdPrefix":["wf-"]}`
		code, contentType, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", body)
		if code != 200 || !strings.HasPrefix(contentType, "application/json") || strings.TrimSpace(raw) != "[]" {
			t.Fatalf("expanded search: %d %s %s", code, contentType, raw)
		}
		wire := fe.body(t, protocol.MsgListWorkflows)
		for key, expected := range map[string]any{
			"attributes":      map[string]any{"tenant": "blue"},
			"completed_after": "2024-01-01T00:00:00Z", "completed_before": "2025-01-01T00:00:00Z",
			"dequeued_after": "2024-02-01T00:00:00Z", "dequeued_before": "2024-12-01T00:00:00Z",
			"executor_id": []any{"exec-1"}, "forked_from": []any{"source-1"}, "has_parent": false,
			"load_input": true, "load_output": false, "parent_workflow_id": []any{"parent-1"},
			"schedule_name": []any{"daily"}, "was_forked_from": false, "workflow_id_prefix": []any{"wf-"},
		} {
			if actual, ok := wire[key]; !ok || !jsonValuesEqual(actual, expected) {
				t.Errorf("POST wire %s = %#v; want %#v", key, actual, expected)
			}
		}
	})
	if calls.Load() != 2 {
		t.Fatalf("dispatch calls = %d; want 2", calls.Load())
	}
}

func jsonValuesEqual(actual, expected any) bool {
	a, _ := json.Marshal(actual)
	b, _ := json.Marshal(expected)
	return string(a) == string(b)
}

func TestLocalHTTPV2WorkflowSearchPreservesOmittedNullEmptyFalseAndZero(t *testing.T) {
	ts, h := localV2Server(t)
	var calls atomic.Int32
	fe := dialFake(t, ts, "fixture-app", "testkey", "workflow-semantics", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": []any{}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	code, _, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", "")
	if code != 200 || strings.TrimSpace(raw) != "[]" {
		t.Fatalf("optional search body: %d %s", code, raw)
	}
	wire := fe.body(t, protocol.MsgListWorkflows)
	if len(wire) != 4 || wire["sort_desc"] != false || wire["load_input"] != false || wire["load_output"] != false || wire["queues_only"] != false {
		t.Fatalf("omitted defaults: %#v", wire)
	}

	nullArrays := `{"workflowIds":null,"workflowName":null,"user":null,"status":null,"appVersion":null,"executorId":null,"forkedFrom":null,"parentWorkflowId":null,"queueName":null,"scheduleName":null,"workflowIdPrefix":null}`
	code, _, raw = localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", nullArrays)
	if code != 200 || strings.TrimSpace(raw) != "[]" {
		t.Fatalf("nullable search arrays: %d %s", code, raw)
	}
	wire = fe.body(t, protocol.MsgListWorkflows)
	if len(wire) != 4 {
		t.Fatalf("null arrays must be omitted: %#v", wire)
	}

	emptyArrays := `{"workflowIds":[],"workflowName":[],"user":[],"status":[],"appVersion":[],"executorId":[],"forkedFrom":[],"parentWorkflowId":[],"queueName":[],"scheduleName":[],"workflowIdPrefix":[],"attributes":{},"sortDesc":false,"queuesOnly":false,"loadInput":false,"loadOutput":false,"wasForkedFrom":false,"hasParent":false,"limit":0,"offset":0}`
	code, _, raw = localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", emptyArrays)
	if code != 200 || strings.TrimSpace(raw) != "[]" {
		t.Fatalf("empty/false/zero search values: %d %s", code, raw)
	}
	wire = fe.body(t, protocol.MsgListWorkflows)
	for _, key := range []string{"workflow_uuids", "workflow_name", "authenticated_user", "status", "application_version", "executor_id", "forked_from", "parent_workflow_id", "queue_name", "schedule_name", "workflow_id_prefix"} {
		value, ok := wire[key].([]any)
		if !ok || len(value) != 0 {
			t.Errorf("explicit empty %s not preserved: %#v", key, wire[key])
		}
	}
	if attributes, ok := wire["attributes"].(map[string]any); !ok || len(attributes) != 0 {
		t.Errorf("empty attributes not preserved: %#v", wire["attributes"])
	}
	for _, key := range []string{"sort_desc", "queues_only", "load_input", "load_output", "was_forked_from", "has_parent"} {
		if value, ok := wire[key]; !ok || value != false {
			t.Errorf("explicit false %s not preserved: %#v", key, value)
		}
	}
	if wire["limit"] != float64(0) || wire["offset"] != float64(0) {
		t.Errorf("explicit zero pagination not preserved: %#v", wire)
	}
	if calls.Load() != 3 {
		t.Fatalf("dispatch calls = %d; want 3", calls.Load())
	}
}

func TestLocalHTTPV2WorkflowReadStrictRequestValidation(t *testing.T) {
	ts, h := localV2Server(t)
	var calls atomic.Int32
	fe := dialFake(t, ts, "fixture-app", "testkey", "workflow-strict", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": []any{}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	code, _, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"?status=SUCCESS%2CERROR&workflowName=", "GET", "")
	if code != 200 || strings.TrimSpace(raw) != "[]" {
		t.Fatalf("scalar comma/empty query values: %d %s", code, raw)
	}
	wire := fe.body(t, protocol.MsgListWorkflows)
	if !jsonValuesEqual(wire["status"], []any{"SUCCESS,ERROR"}) || !jsonValuesEqual(wire["workflow_name"], []any{""}) {
		t.Fatalf("GET scalar values were split or dropped: %#v", wire)
	}

	before := calls.Load()
	for _, query := range []string{
		"status=SUCCESS&status=ERROR", "workflowName=a&workflowName=b", "limit=1&limit=2",
		"sortDesc=False", "loadInput=1", "loadOutput=", "limit=-1", "offset=1.5",
		"queuesOnly=true", "status=%FF", "bad=%zz", "unexpected=value",
	} {
		code, contentType, body := localV2Request(t, ts.URL+localV2WorkflowRoot+"?"+query, "GET", "")
		if code != 400 || !strings.HasPrefix(contentType, "application/problem+json") {
			t.Errorf("GET query %q: %d %s %s", query, code, contentType, body)
		}
	}
	invalidBodies := []string{
		`{"$schema":"//schemas/WorkflowSearchBody.json"}`, `{"applicationName":["other"]}`,
		`{"attributes":null}`, `{"attributes":[]}`, `{"attributes":"value"}`,
		`{"completedAfter":null}`, `{"completedBefore":""}`, `{"dequeuedAfter":"not-a-date"}`, `{"dequeuedBefore":0}`,
		`{"executorId":null,"executorId":[]}`, `{"executorId":"exec"}`, `{"forkedFrom":[null]}`,
		`{"hasParent":null}`, `{"loadInput":null}`, `{"loadOutput":1}`, `{"wasForkedFrom":"false"}`,
		`{"scheduleName":{}}`, `{"workflowIdPrefix":false}`, `{"unknown":true}`, `[]`, `{ } trailing`,
		string([]byte{'{', '"', 's', 't', 'a', 't', 'u', 's', '"', ':', '"', 0xff, '"', '}'}),
	}
	for _, body := range invalidBodies {
		code, contentType, response := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", body)
		if code != 400 || !strings.HasPrefix(contentType, "application/problem+json") {
			t.Errorf("POST body %q: %d %s %s", body, code, contentType, response)
		}
	}
	for _, target := range []string{
		ts.URL + "/v2/orgs/local/apps/UPPER/workflows",
		ts.URL + "/v2/orgs/local/apps/UPPER/workflows/search",
	} {
		method, body := "GET", ""
		if strings.HasSuffix(target, "/search") {
			method, body = "POST", `{}`
		}
		code, _, _ := localV2Request(t, target, method, body)
		if code != 400 {
			t.Errorf("invalid app %s: status %d", target, code)
		}
	}
	if calls.Load() != before {
		t.Fatalf("invalid input dispatched %d calls; before=%d", calls.Load(), before)
	}
}

func TestLocalHTTPV2WorkflowReadCapabilityAndRetryBoundaries(t *testing.T) {
	expandedBody := `{"completedAfter":"2024-01-01T00:00:00Z","executorId":[],"wasForkedFrom":false}`

	t.Run("expanded filters attempt unrecognized SDK", func(t *testing.T) {
		ts, h := localV2Server(t)
		var calls atomic.Int32
		dialScheduleFake(t, ts.URL, "future", "3.1.1", map[protocol.MessageType]respondFn{
			protocol.MsgListWorkflows: func(map[string]any) map[string]any {
				calls.Add(1)
				return map[string]any{"output": []any{}}
			},
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		code, _, body := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", expandedBody)
		if code != 200 || calls.Load() != 1 || strings.TrimSpace(body) != "[]" {
			t.Fatalf("unrecognized SDK expanded filters: status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})

	t.Run("expanded filters attempt other languages", func(t *testing.T) {
		ts, h := localV2Server(t)
		var calls atomic.Int32
		dialScheduleConsoleFake(t, ts.URL, "fixture-app", "typescript", "typescript", "5.1", map[protocol.MessageType]respondFn{
			protocol.MsgListWorkflows: func(map[string]any) map[string]any {
				calls.Add(1)
				return map[string]any{"output": []any{}}
			},
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		code, _, body := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", expandedBody)
		if code != 200 || calls.Load() != 1 || strings.TrimSpace(body) != "[]" {
			t.Fatalf("other language expanded filters: status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})

	t.Run("expanded filters allow mixed peers", func(t *testing.T) {
		ts, h := localV2Server(t)
		var reviewedCalls, unknownCalls atomic.Int32
		dialScheduleFake(t, ts.URL, "unknown", "3.1.1", map[protocol.MessageType]respondFn{
			protocol.MsgListWorkflows: func(map[string]any) map[string]any {
				unknownCalls.Add(1)
				return map[string]any{"output": []any{}}
			},
		})
		dialScheduleFake(t, ts.URL, "reviewed", "2.24.0", map[protocol.MessageType]respondFn{
			protocol.MsgListWorkflows: func(map[string]any) map[string]any {
				reviewedCalls.Add(1)
				return map[string]any{"output": []any{}}
			},
		})
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, _, body := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", expandedBody)
		if code != 200 || reviewedCalls.Load()+unknownCalls.Load() != 1 || strings.TrimSpace(body) != "[]" {
			t.Fatalf("mixed expanded filters: status=%d reviewed=%d unknown=%d body=%s", code, reviewedCalls.Load(), unknownCalls.Load(), body)
		}
	})

	t.Run("recent filters attempt unrecognized SDK", func(t *testing.T) {
		ts, h := localV2Server(t)
		var calls atomic.Int32
		dialScheduleFake(t, ts.URL, "future", "3.1.1", map[protocol.MessageType]respondFn{
			protocol.MsgListWorkflows: func(map[string]any) map[string]any {
				calls.Add(1)
				return map[string]any{"output": []any{}}
			},
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		code, _, body := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", `{"attributes":{"tenant":"blue"}}`)
		if code != 200 || calls.Load() != 1 || strings.TrimSpace(body) != "[]" {
			t.Fatalf("unrecognized SDK: status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})

	t.Run("reviewed legacy fields retain Python 2.24 compatibility", func(t *testing.T) {
		ts, h := localV2Server(t)
		var calls atomic.Int32
		dialScheduleFake(t, ts.URL, "legacy", "2.24.0", map[protocol.MessageType]respondFn{
			protocol.MsgListWorkflows: func(map[string]any) map[string]any {
				calls.Add(1)
				return map[string]any{"output": []any{}}
			},
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		body := `{"completedAfter":"2024-01-01T00:00:00Z","dequeuedBefore":"2025-01-01T00:00:00Z","executorId":["exec"],"forkedFrom":["source"],"hasParent":false,"parentWorkflowId":["parent"],"wasForkedFrom":false,"workflowIdPrefix":["wf-"]}`
		code, _, response := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", body)
		if code != 200 || calls.Load() != 1 || strings.TrimSpace(response) != "[]" {
			t.Fatalf("reviewed legacy SDK: status=%d calls=%d body=%s", code, calls.Load(), response)
		}
	})

	t.Run("executor refusal is final", func(t *testing.T) {
		ts, h := localV2Server(t)
		var calls atomic.Int32
		for _, id := range []string{"one", "two"} {
			dialScheduleFake(t, ts.URL, id, "3.1.0", map[protocol.MessageType]respondFn{
				protocol.MsgListWorkflows: func(map[string]any) map[string]any {
					calls.Add(1)
					return map[string]any{"error_message": "metadata-only refusal"}
				},
			})
		}
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, _, body := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", `{"attributes":{"tenant":"blue"}}`)
		if code != 502 || calls.Load() != 1 || !strings.Contains(body, "metadata-only refusal") {
			t.Fatalf("refusal bypass: status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})

	t.Run("pure read disconnect retries another peer", func(t *testing.T) {
		ts, h := localV2Server(t)
		var calls atomic.Int32
		handlers := map[protocol.MessageType]respondFn{
			protocol.MsgListWorkflows: func(map[string]any) map[string]any {
				if calls.Add(1) == 1 {
					return nil
				}
				return map[string]any{"output": []any{}}
			},
		}
		for _, id := range []string{"one", "two"} {
			dialScheduleConsoleFake(t, ts.URL, "fixture-app", id, "", "", handlers)
		}
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, _, body := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", `{"scheduleName":["daily"]}`)
		if code != 200 || calls.Load() != 2 || strings.TrimSpace(body) != "[]" {
			t.Fatalf("disconnect retry: status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})
}

func TestLocalHTTPV2ListWorkflowsUnavailableAndNullOutput(t *testing.T) {
	ts, _ := localV2Server(t)
	code, _, _ := localV2Request(t, ts.URL+localV2WorkflowRoot, "GET", "")
	if code != 503 {
		t.Fatalf("unavailable GET list status=%d", code)
	}

	ts, h := localV2Server(t)
	dialFake(t, ts, "fixture-app", "testkey", "null-list", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(map[string]any) map[string]any { return map[string]any{"output": nil} },
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, request := range []struct {
		method, suffix, body string
	}{{"GET", "", ""}, {"POST", "/search", `{}`}} {
		code, contentType, body := localV2Request(t, ts.URL+localV2WorkflowRoot+request.suffix, request.method, request.body)
		if code != 502 || !strings.HasPrefix(contentType, "application/problem+json") || !strings.Contains(body, "null") {
			t.Errorf("null output %s: %d %s %s", request.method, code, contentType, body)
		}
	}
}
