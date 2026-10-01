package api_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zpaden/maestro/internal/api"
	"github.com/zpaden/maestro/internal/config"
	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/protocol"
	"log/slog"
	"net/http/httptest"
)

const localV2WorkflowRoot = "/v2/orgs/local/apps/fixture-app/workflows"

func localV2Server(t *testing.T) (*httptest.Server, *hub.Hub) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(log, 2*time.Second)
	srv := api.New(config.Config{ConductorKey: "testkey", ListenAddr: "127.0.0.1:0"}, h, log)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, h
}

func localV2Request(t *testing.T, url, method, body string) (int, string, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(data)
}

func localV2Record() map[string]any {
	return map[string]any{"WorkflowUUID": "wf-1", "Status": "SUCCESS", "WorkflowName": "job", "CreatedAt": "1720000000123", "UpdatedAt": "1720000000456", "Priority": "0", "WorkflowTimeoutMS": "0", "WorkflowDeadlineEpochMS": nil, "Attributes": "{\"secret\":true}", "WasForkedFrom": false}
}

func localV2Fixture(t *testing.T) (*httptest.Server, *fakeExec) {
	t.Helper()
	ts, h := localV2Server(t)
	rows := make([]map[string]any, 30)
	for i := range rows {
		rows[i] = localV2Record()
		rows[i]["WorkflowUUID"] = fmt.Sprintf("wf-%d", i+1)
	}
	fe := dialFake(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(req map[string]any) map[string]any {
			b := req["body"].(map[string]any)
			n := len(rows)
			if l, ok := b["limit"].(float64); ok && int(l) < n {
				n = int(l)
			}
			return map[string]any{"output": rows[:n]}
		},
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			if req["workflow_id"] == "missing" {
				return map[string]any{"output": nil}
			}
			return map[string]any{"output": localV2Record()}
		},
		protocol.MsgListSteps: func(req map[string]any) map[string]any {
			return map[string]any{"output": []any{map[string]any{"function_id": 0, "function_name": "step-one", "started_at_epoch_ms": "1720000000123", "completed_at_epoch_ms": nil, "output": nil, "error": nil, "child_workflow_id": nil}}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	return ts, fe
}

// Official OpenAPI: a schema oracle loaded from the official snapshot, never from our HTTP DTOs.
func assertLocalV2Schema(t *testing.T, schema string, value map[string]any) {
	t.Helper()
	b, err := os.ReadFile("../../docs/reference/conductor-openapi-2026-09-25.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Components struct {
			Schemas map[string]struct {
				Required   []string `json:"required"`
				Properties map[string]struct {
					Type   json.RawMessage `json:"type"`
					Format string          `json:"format"`
				} `json:"properties"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(b, &spec); err != nil {
		t.Fatal(err)
	}
	s := spec.Components.Schemas[schema]
	if len(s.Required) == 0 {
		t.Fatalf("schema %s absent", schema)
	}
	for _, field := range s.Required {
		if _, ok := value[field]; !ok {
			t.Errorf("%s missing %s", schema, field)
		}
	}
	for key, v := range value {
		p, ok := s.Properties[key]
		if !ok {
			t.Errorf("%s unknown %s", schema, key)
			continue
		}
		var typ any
		_ = json.Unmarshal(p.Type, &typ)
		allowed := []string{}
		switch x := typ.(type) {
		case string:
			allowed = append(allowed, x)
		case []any:
			for _, i := range x {
				allowed = append(allowed, i.(string))
			}
		}
		actual := "string"
		switch v.(type) {
		case nil:
			actual = "null"
		case bool:
			actual = "boolean"
		case float64:
			actual = "integer"
		case map[string]any:
			actual = "object"
		}
		valid := false
		for _, a := range allowed {
			if a == actual || a == "number" && actual == "integer" {
				valid = true
			}
		}
		if !valid {
			t.Errorf("%s.%s: expected %v got %v", schema, key, allowed, v)
		}
		if p.Format == "date-time" && v != nil {
			if _, err := time.Parse(time.RFC3339Nano, v.(string)); err != nil {
				t.Errorf("%s.%s: %v", schema, key, err)
			}
		}
	}
}

func TestLocalHTTPV2WorkflowSchemaAndWire(t *testing.T) {
	ts, fe := localV2Fixture(t)
	code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", `{"limit":2,"offset":0,"sortDesc":false,"queuesOnly":false,"status":["SUCCESS"],"startTime":"2024-07-03T00:00:00Z"}`)
	if code != 200 || !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("search: %d %s %s", code, ct, raw)
	}
	var list []map[string]any
	if err := json.Unmarshal([]byte(raw), &list); err != nil || len(list) != 2 {
		t.Fatalf("search rows: %s %v", raw, err)
	}
	if list[0]["workflowId"] != "wf-1" || list[1]["workflowId"] != "wf-2" {
		t.Fatalf("ids %v", list)
	}
	assertLocalV2Schema(t, "Workflow", list[0])
	if list[0]["createdAt"] != "2024-07-03T09:46:40.123Z" || list[0]["updatedAt"] != "2024-07-03T09:46:40.456Z" {
		t.Fatalf("createdAt: %v", list[0])
	}
	b := fe.body(t, protocol.MsgListWorkflows)
	for k, v := range map[string]any{"limit": float64(2), "offset": float64(0), "sort_desc": false, "queues_only": false, "start_time": "2024-07-03T00:00:00Z"} {
		if b[k] != v {
			t.Errorf("wire %s = %v; want %v", k, b[k], v)
		}
	}
	code, _, raw = localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", `{"workflowIds":["wf-1"],"user":["alice"],"status":["SUCCESS"],"workflowName":["job"],"appVersion":["v1"],"queueName":["q"],"endTime":"2024-07-04T00:00:00Z","sortDesc":true,"queuesOnly":true,"limit":0}`)
	if code != 200 || strings.TrimSpace(raw) != "[]" {
		t.Fatalf("all-filter/explicit zero: %d %s", code, raw)
	}
	wire := fe.body(t, protocol.MsgListWorkflows)
	for httpField, wireField := range map[string]string{"workflowIds": "workflow_uuids", "user": "authenticated_user", "status": "status", "workflowName": "workflow_name", "appVersion": "application_version", "queueName": "queue_name"} {
		v, ok := wire[wireField].([]any)
		if !ok || len(v) != 1 {
			t.Fatalf("%s -> %s: %v", httpField, wireField, wire)
		}
	}
	if wire["end_time"] != "2024-07-04T00:00:00Z" || wire["sort_desc"] != true || wire["queues_only"] != true || wire["limit"] != float64(0) {
		t.Fatalf("filtered wire: %v", wire)
	}
	code, _, raw = localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", `{"status":null}`)
	if code != 200 {
		t.Fatalf("nullable array: %d %s", code, raw)
	}
	if _, ok := fe.body(t, protocol.MsgListWorkflows)["status"]; ok {
		t.Fatal("null status applied as filter")
	}
	code, _, raw = localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", `{}`)
	if code != 200 {
		t.Fatalf("no limit: %d %s", code, raw)
	}
	if err := json.Unmarshal([]byte(raw), &list); err != nil || len(list) != 30 || list[29]["workflowId"] != "wf-30" {
		t.Fatalf("omitted limit >25: %s %v", raw, err)
	}
	if _, ok := fe.body(t, protocol.MsgListWorkflows)["limit"]; ok {
		t.Fatal("hidden limit")
	}
	code, _, raw = localV2Request(t, ts.URL+localV2WorkflowRoot+"/wf-1", "GET", "")
	var wf map[string]any
	_ = json.Unmarshal([]byte(raw), &wf)
	if code != 200 {
		t.Fatalf("get: %d %s", code, raw)
	}
	assertLocalV2Schema(t, "Workflow", wf)
	if wf["priority"] != float64(0) || wf["timeoutMs"] != float64(0) || wf["deadline"] != nil || wf["attributes"] != "{\"secret\":true}" {
		t.Fatalf("wrong values: %v", wf)
	}
	code, _, raw = localV2Request(t, ts.URL+localV2WorkflowRoot+"/wf-1/steps?limit=0&offset=1", "GET", "")
	var steps []map[string]any
	_ = json.Unmarshal([]byte(raw), &steps)
	if code != 200 || len(steps) != 1 {
		t.Fatalf("steps: %d %s", code, raw)
	}
	assertLocalV2Schema(t, "Step", steps[0])
	if stepWire := fe.body(t, protocol.MsgListSteps); stepWire["limit"] != float64(0) || stepWire["offset"] != float64(1) {
		t.Fatalf("step pagination wire: %v", stepWire)
	}
	if steps[0]["stepId"] != float64(0) || steps[0]["stepName"] != "step-one" || steps[0]["startedAt"] != "2024-07-03T09:46:40.123Z" || steps[0]["completedAt"] != nil {
		t.Fatalf("step: %v", steps[0])
	}
}

func TestLocalHTTPV2StrictErrors(t *testing.T) {
	ts, _ := localV2Fixture(t)
	// attributes, workflowIdPrefix, and explicit empty arrays are now supported by
	// the complete pinned WorkflowSearchBody implementation; the remaining cases
	// are still malformed or outside that schema.
	for _, body := range []string{`{"limit":-1}`, `{"limit":1.5}`, `{"limit":"2"}`, `{"limit":null}`, `{"status":42}`, `{"status":[null]}`, `{"startTime":"garbage"}`, `{"applicationName":["other"]}`, `{"unexpected":1}`, `{"limit":2,"limit":100}`, `{"limit":9223372036854775808}`, `{"sortDesc":null}`, `{"startTime":null}`} {
		code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"/search", "POST", body)
		if code != 400 || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, "detail") {
			t.Errorf("body %s -> %d %s %s", body, code, ct, raw)
		}
	}
	for _, route := range []string{"/missing", "/missing/steps"} {
		code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+route, "GET", "")
		if code != 404 || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, "detail") {
			t.Errorf("%s: %d %s %s", route, code, ct, raw)
		}
	}
	code, _, _ := localV2Request(t, ts.URL+"/v2/orgs/other/apps/fixture-app/workflows/wf-1", "GET", "")
	if code != 404 {
		t.Fatalf("other org: %d", code)
	}
	code, _, _ = localV2Request(t, ts.URL+localV2WorkflowRoot+"/search?bad=%zz", "POST", `{}`)
	if code != 400 {
		t.Fatalf("malformed search query ignored: %d", code)
	}
	code, _, _ = localV2Request(t, ts.URL+localV2WorkflowRoot+"/wf-1/steps?limit=2&bad=%zz", "GET", "")
	if code != 400 {
		t.Fatalf("malformed steps query ignored: %d", code)
	}
	code, _, _ = localV2Request(t, ts.URL+localV2WorkflowRoot+"/wf-1/steps?limit=bad", "GET", "")
	if code != 400 {
		t.Fatalf("bad steps query: %d", code)
	}
}

// The HTTP schema requires these fields: required executor collections and scalars must not be fabricated by
// Go's JSON zero values. Empty arrays and explicit false/zero remain valid.
func TestLocalHTTPV2NullExecutorCollections(t *testing.T) {
	for _, tc := range []struct {
		name, route, method, body string
		command                   protocol.MessageType
	}{
		{"search-null", "/search", "POST", `{}`, protocol.MsgListWorkflows},
		{"steps-null", "/wf-1/steps", "GET", "", protocol.MsgListSteps},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := localV2Server(t)
			dialFake(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]respondFn{
				protocol.MsgGetWorkflow: func(map[string]any) map[string]any { return map[string]any{"output": localV2Record()} },
				tc.command:              func(map[string]any) map[string]any { return map[string]any{"output": nil} },
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+tc.route, tc.method, tc.body)
			if code != 502 || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, "detail") {
				t.Fatalf("null executor %s output: %d %s %s", tc.command, code, ct, raw)
			}
		})
	}
}

func TestLocalHTTPV2ExplicitEmptyExecutorCollections(t *testing.T) {
	ts, h := localV2Server(t)
	dialFake(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow:   func(map[string]any) map[string]any { return map[string]any{"output": localV2Record()} },
		protocol.MsgListWorkflows: func(map[string]any) map[string]any { return map[string]any{"output": []any{}} },
		protocol.MsgListSteps:     func(map[string]any) map[string]any { return map[string]any{"output": []any{}} },
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, tc := range []struct{ route, method, body string }{{"/search", "POST", `{}`}, {"/wf-1/steps", "GET", ""}} {
		code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+tc.route, tc.method, tc.body)
		if code != 200 || !strings.HasPrefix(ct, "application/json") || strings.TrimSpace(raw) != "[]" {
			t.Fatalf("explicit empty %s: %d %s %s", tc.route, code, ct, raw)
		}
	}
}

func TestLocalHTTPV2MissingExecutorRequiredScalars(t *testing.T) {
	for _, tc := range []struct {
		name, field, route string
		value              any
		missing            bool
	}{
		{"missing-step-id", "function_id", "/wf-1/steps", nil, true},
		{"null-step-id", "function_id", "/wf-1/steps", nil, false},
		{"missing-was-forked", "WasForkedFrom", "/wf-1", nil, true},
		{"null-was-forked", "WasForkedFrom", "/wf-1", nil, false},
		{"list-missing-was-forked", "WasForkedFrom", "/search", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := localV2Server(t)
			record := localV2Record()
			step := map[string]any{"function_id": 0, "function_name": "step-one"}
			if tc.route == "/wf-1/steps" {
				if tc.missing {
					delete(step, tc.field)
				} else {
					step[tc.field] = tc.value
				}
			} else if tc.missing {
				delete(record, tc.field)
			} else {
				record[tc.field] = tc.value
			}
			dialFake(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]respondFn{
				protocol.MsgGetWorkflow:   func(map[string]any) map[string]any { return map[string]any{"output": record} },
				protocol.MsgListWorkflows: func(map[string]any) map[string]any { return map[string]any{"output": []any{record}} },
				protocol.MsgListSteps:     func(map[string]any) map[string]any { return map[string]any{"output": []any{step}} },
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			method, body := "GET", ""
			if tc.route == "/search" {
				method, body = "POST", `{}`
			}
			code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+tc.route, method, body)
			if code != 502 || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, "detail") {
				t.Fatalf("%s source %s: %d %s %s", tc.name, tc.field, code, ct, raw)
			}
		})
	}
}

func TestLocalHTTPV2PresenceChecksDoNotChangeLegacyJSON(t *testing.T) {
	ts, h := localV2Server(t)
	record := localV2Record()
	delete(record, "WasForkedFrom")
	dialFake(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any { return map[string]any{"output": record} },
		protocol.MsgListSteps: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{map[string]any{"function_name": "step-one"}}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	code, _, raw := localV2Request(t, ts.URL+"/api/fixture-app/workflows/wf-1", "GET", "")
	var workflow map[string]any
	if code != 200 || json.Unmarshal([]byte(raw), &workflow) != nil || workflow["WasForkedFrom"] != false {
		t.Fatalf("legacy workflow JSON changed: %d %s", code, raw)
	}
	code, _, raw = localV2Request(t, ts.URL+"/api/fixture-app/workflows/wf-1/steps", "GET", "")
	var steps []map[string]any
	if code != 200 || json.Unmarshal([]byte(raw), &steps) != nil || len(steps) != 1 || steps[0]["function_id"] != float64(0) {
		t.Fatalf("legacy steps JSON changed: %d %s", code, raw)
	}
}

func TestLocalHTTPV2InvalidStepID(t *testing.T) {
	ts, h := localV2Server(t)
	dialFake(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any { return map[string]any{"output": localV2Record()} },
		protocol.MsgListSteps: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{map[string]any{"function_id": 2147483648, "function_name": "overflow"}}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"/wf-1/steps", "GET", "")
	if code != 502 || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, "stepId") {
		t.Fatalf("overflow step: %d %s %s", code, ct, raw)
	}
}

func TestLocalHTTPV2InvalidExecutorValues(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		value any
	}{
		{"bad-created", "CreatedAt", "not-an-epoch"}, {"overflow-priority", "Priority", "2147483648"},
		{"bad-timeout", "WorkflowTimeoutMS", "not-a-number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := localV2Server(t)
			record := localV2Record()
			record[tc.field] = tc.value
			dialFake(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]respondFn{
				protocol.MsgGetWorkflow: func(map[string]any) map[string]any { return map[string]any{"output": record} },
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"/wf-1", "GET", "")
			if code != 502 || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, "detail") {
				t.Fatalf("invalid %s: %d %s %s", tc.field, code, ct, raw)
			}
		})
	}
}

func TestLocalHTTPV2PrivacyRefusalIsNotRetried(t *testing.T) {
	ts, h := localV2Server(t)
	var attempts atomic.Int32
	refusal := map[protocol.MessageType]respondFn{protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
		attempts.Add(1)
		return map[string]any{"error_message": "metadata-only refusal"}
	}}
	dialFake(t, ts, "fixture-app", "testkey", "one", refusal)
	dialFake(t, ts, "fixture-app", "testkey", "two", refusal)
	waitFor(t, func() bool { return len(h.Executors()) == 2 })
	code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"/wf-1", "GET", "")
	if code != 502 || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, "metadata-only refusal") || attempts.Load() != 1 {
		t.Fatalf("refusal bypass: %d %s %s attempts=%d", code, ct, raw, attempts.Load())
	}
}

func TestLocalHTTPV2RejectsNonLoopbackRemote(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(log, time.Second)
	srv := api.New(config.Config{ConductorKey: "testkey"}, h, log)
	r := httptest.NewRequest("GET", localV2WorkflowRoot+"/wf-1", nil)
	r.RemoteAddr = "198.51.100.20:4321"
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, r)
	if w.Code != 403 || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/problem+json") {
		t.Fatalf("non-loopback remote: %d %s", w.Code, w.Body.String())
	}
}

func TestLocalHTTPV2Unavailable(t *testing.T) {
	ts, _ := localV2Server(t)
	code, ct, raw := localV2Request(t, ts.URL+localV2WorkflowRoot+"/wf-1", "GET", "")
	if code != 503 || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, "detail") {
		t.Fatalf("unavailable: %d %s %s", code, ct, raw)
	}
}
