package api_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zpaden/maestro/internal/protocol"
)

// These assertions characterize the local JSON contract, which deliberately
// differs from the strict official HTTP adapter (docs/CONTRACTS.md).
func jsonAPIRequest(t *testing.T, target string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Local JSON has always responded in JSON irrespective of Accept.
	req.Header.Set("Accept", "text/html")
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got := response.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("%s: content type %q", target, got)
	}
	if !strings.HasSuffix(string(body), "\n") {
		t.Fatalf("%s: missing JSON encoder newline: %s", target, body)
	}
	return response.StatusCode, string(body)
}

func TestJSONAPIHealthAndRegistry(t *testing.T) {
	ts, h := newTestServer(t)
	for path, want := range map[string]string{
		"/healthz": "{\"status\":true}\n", "/api/executors": "[]\n", "/api/apps": "[]\n",
	} {
		code, body := jsonAPIRequest(t, ts.URL+path)
		if code != 200 || body != want {
			t.Fatalf("%s: %d %s, want %s", path, code, body, want)
		}
	}
	dialFake(t, ts, "z-app", "testkey", "z-1", nil)
	dialFake(t, ts, "a-app", "testkey", "a-1", nil)
	dialFake(t, ts, "a-app", "testkey", "a-2", nil)
	waitFor(t, func() bool { return len(h.Executors()) == 3 })
	code, body := jsonAPIRequest(t, ts.URL+"/api/apps")
	if code != 200 || body != "[{\"name\":\"a-app\",\"executors\":2},{\"name\":\"z-app\",\"executors\":1}]\n" {
		t.Fatalf("sorted applications: %d %s", code, body)
	}
	code, body = jsonAPIRequest(t, ts.URL+"/api/executors")
	var executors []map[string]any
	if code != 200 || json.Unmarshal([]byte(body), &executors) != nil || len(executors) != 3 {
		t.Fatalf("executor snapshot: %d %s", code, body)
	}
	for _, executor := range executors {
		for _, key := range []string{"executor_id", "app", "application_version", "language", "dbos_version", "connected_at"} {
			if _, ok := executor[key]; !ok {
				t.Errorf("executor missing %s: %v", key, executor)
			}
		}
		if _, ok := executor["hostname"]; ok {
			t.Errorf("empty optional hostname emitted: %v", executor)
		}
		if _, ok := executor["$schema"]; ok {
			t.Errorf("executor JSON gained $schema: %v", executor)
		}
	}
}

func TestJSONAPINullEmptyAndMissingPayloads(t *testing.T) {
	ts, h := newTestServer(t)
	var mode atomic.Int32
	read := func(key string, list bool) respondFn {
		return func(map[string]any) map[string]any {
			switch mode.Load() {
			case 0:
				return map[string]any{key: nil}
			case 1:
				if list {
					return map[string]any{key: []any{}}
				}
				return map[string]any{key: nil}
			default:
				return map[string]any{}
			}
		}
	}
	dialFake(t, ts, "app", "testkey", "one", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: read("output", true), protocol.MsgGetWorkflow: read("output", false),
		protocol.MsgListSteps: read("output", true), protocol.MsgListQueues: read("output", true),
		protocol.MsgGetQueue: read("output", false), protocol.MsgGetWorkflowEvents: read("events", true),
		protocol.MsgGetWorkflowNotifications: read("notifications", true), protocol.MsgGetWorkflowStreams: read("streams", true),
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, tc := range []struct {
		path string
		list bool
	}{
		{"workflows", true}, {"workflows/wf", false}, {"workflows/wf/steps", true},
		{"workflows/wf/events", true}, {"workflows/wf/notifications", true}, {"workflows/wf/streams", true},
		{"queues", true}, {"queues/jobs", false},
	} {
		t.Run(tc.path, func(t *testing.T) {
			mode.Store(0)
			code, body := jsonAPIRequest(t, ts.URL+"/api/app/"+tc.path)
			if code != 200 || body != "null\n" {
				t.Fatalf("null is a successful local result: %d %s", code, body)
			}
			mode.Store(1)
			code, body = jsonAPIRequest(t, ts.URL+"/api/app/"+tc.path)
			want := "null\n"
			if tc.list {
				want = "[]\n"
			}
			if code != 200 || body != want {
				t.Fatalf("explicit empty array changed: %d %s", code, body)
			}
			mode.Store(2)
			code, body = jsonAPIRequest(t, ts.URL+"/api/app/"+tc.path)
			if code != 502 || body != "{\"error\":\"executor response data unavailable\"}\n" {
				t.Fatalf("missing payload became success: %d %s", code, body)
			}
		})
	}
}

func TestJSONAPIErrorAndOpaqueRepresentations(t *testing.T) {
	ts, h := newTestServer(t)
	code, body := jsonAPIRequest(t, ts.URL+"/api/ghost/workflows")
	if code != 503 || body != "{\"error\":\"conductor: application unavailable\"}\n" {
		t.Fatalf("unavailable app: %d %s", code, body)
	}
	const opaque = "<private>&\"opaque\"\n雪"
	dialFake(t, ts, "app", "testkey", "one", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": map[string]any{"WorkflowUUID": "wf", "Input": opaque}}
		},
		protocol.MsgListSteps: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{map[string]any{"function_name": "step", "output": opaque}}}
		},
		protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any {
			return map[string]any{"error_message": opaque}
		},
		protocol.MsgGetWorkflowNotifications: func(map[string]any) map[string]any {
			return map[string]any{"notifications": []any{map[string]any{"message": opaque, "created_at_epoch_ms": 0, "consumed": false, "topic": nil}}}
		},
		protocol.MsgGetWorkflowStreams: func(map[string]any) map[string]any {
			return map[string]any{"streams": []any{map[string]any{"key": "stream", "values": []string{opaque, ""}}}}
		},
		protocol.MsgGetQueue: func(map[string]any) map[string]any {
			return map[string]any{"output": map[string]any{"name": "jobs", "polling_interval_sec": 0}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, path := range []string{"workflows/wf", "workflows/wf/steps", "workflows/wf/notifications", "workflows/wf/streams"} {
		code, body := jsonAPIRequest(t, ts.URL+"/api/app/"+path)
		if code != 200 || !strings.Contains(body, `\u003cprivate\u003e\u0026\"opaque\"\n雪`) || strings.Contains(body, "$schema") {
			t.Fatalf("opaque JSON encoding changed: %d %s", code, body)
		}
	}
	code, body = jsonAPIRequest(t, ts.URL+"/api/app/workflows/wf")
	var workflow map[string]any
	if json.Unmarshal([]byte(body), &workflow) != nil || workflow["WasForkedFrom"] != false || workflow["Status"] != nil || len(workflow) != 30 {
		t.Fatalf("tolerant workflow zero/null fields changed: %d %s", code, body)
	}
	code, body = jsonAPIRequest(t, ts.URL+"/api/app/workflows/wf/steps")
	var steps []map[string]any
	if json.Unmarshal([]byte(body), &steps) != nil || steps[0]["function_id"] != float64(0) || steps[0]["error"] != nil || len(steps[0]) != 7 {
		t.Fatalf("tolerant step zero/null fields changed: %d %s", code, body)
	}
	code, body = jsonAPIRequest(t, ts.URL+"/api/app/queues/jobs")
	var queue map[string]any
	if code != 200 || json.Unmarshal([]byte(body), &queue) != nil || len(queue) != 13 || queue["priority_enabled"] != false || queue["polling_interval_sec"] != float64(0) || queue["concurrency"] != nil {
		t.Fatalf("tolerant queue zero/null fields changed: %d %s", code, body)
	}
	code, body = jsonAPIRequest(t, ts.URL+"/api/app/workflows/wf/events")
	var failure map[string]string
	if code != 502 || json.Unmarshal([]byte(body), &failure) != nil || len(failure) != 1 || failure["error"] != opaque || !strings.Contains(body, `\u003cprivate\u003e`) {
		t.Fatalf("executor error shape/escaping changed: %d %s", code, body)
	}
}

func TestJSONAPIWorkflowFiltersAndPagination(t *testing.T) {
	ts, h := newTestServer(t)
	workflows := make([]protocol.WorkflowsOutput, 26)
	for i := range workflows {
		workflows[i] = sampleWorkflow(fmt.Sprintf("wf-%02d", i), "SUCCESS")
	}
	fe := dialFake(t, ts, "app", "testkey", "one", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: staticList(workflows...),
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, tc := range []struct {
		query  string
		name   string
		offset int
	}{
		{"status=UNKNOWN&name=%20exact%20&id_prefix=id&queue=jobs&offset=25&extra=ignored", " exact ", 25},
		{"name=first&name=second&offset=-4", "first", -4},
		{"name&offset=bad", "", 0},
		{"name=bad;value&name=next&offset=", "next", 0},
		{"name=%zz&name=next&offset=0", "next", 0},
		{"offset=9999999999999999999999999", "", int(^uint(0) >> 1)},
	} {
		t.Run(tc.query, func(t *testing.T) {
			code, body := jsonAPIRequest(t, ts.URL+"/api/app/workflows?"+tc.query)
			var rows []map[string]any
			if code != 200 || json.Unmarshal([]byte(body), &rows) != nil || len(rows) != 25 || rows[24]["WorkflowUUID"] != "wf-24" {
				t.Fatalf("local list bounds changed: %d %s", code, body)
			}
			wire := fe.body(t, protocol.MsgListWorkflows)
			if wire["limit"] != float64(26) || wire["offset"] != float64(tc.offset) || wire["sort_desc"] != true || wire["load_input"] != false || wire["load_output"] != false {
				t.Fatalf("pagination/blob flags changed: %v", wire)
			}
			if tc.name == "" {
				if _, ok := wire["workflow_name"]; ok {
					t.Fatalf("empty filter dispatched: %v", wire)
				}
			} else if !reflect.DeepEqual(wire["workflow_name"], []any{tc.name}) {
				t.Fatalf("exact-name query changed: %v", wire)
			}
			if strings.HasPrefix(tc.query, "status=") {
				for key, want := range map[string]string{"status": "UNKNOWN", "workflow_id_prefix": "id", "queue_name": "jobs"} {
					if !reflect.DeepEqual(wire[key], []any{want}) {
						t.Fatalf("filter %s changed: %v", key, wire)
					}
				}
			}
		})
	}
}

func TestJSONAPIDecodeErrorsAndReadFlags(t *testing.T) {
	ts, h := newTestServer(t)
	fe := dialFake(t, ts, "app", "testkey", "one", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			return map[string]any{"output": map[string]any{"WorkflowUUID": "wf"}}
		},
		protocol.MsgListSteps: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{}}
		},
		protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any {
			return map[string]any{"events": "invalid array"}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	code, body := jsonAPIRequest(t, ts.URL+"/api/app/workflows/wf%20id")
	if code != 200 {
		t.Fatalf("workflow read: %d %s", code, body)
	}
	wire := fe.body(t, protocol.MsgGetWorkflow)
	if wire["workflow_id"] != "wf id" || wire["load_input"] != true || wire["load_output"] != true {
		t.Fatalf("detail request flags/path changed: %v", wire)
	}
	code, body = jsonAPIRequest(t, ts.URL+"/api/app/workflows/wf%20id/steps")
	if code != 200 || body != "[]\n" {
		t.Fatalf("steps read: %d %s", code, body)
	}
	wire = fe.body(t, protocol.MsgListSteps)
	if wire["workflow_id"] != "wf id" || wire["load_output"] != true || wire["limit"] != nil || wire["offset"] != nil {
		t.Fatalf("step read flags/path changed: %v", wire)
	}
	code, body = jsonAPIRequest(t, ts.URL+"/api/app/workflows/wf/events")
	var failure map[string]string
	if code != 502 || json.Unmarshal([]byte(body), &failure) != nil || len(failure) != 1 || !strings.HasPrefix(failure["error"], "decode response: ") {
		t.Fatalf("decode failure changed: %d %s", code, body)
	}
}

func TestJSONAPIHTTPMethods(t *testing.T) {
	ts, _ := newTestServer(t)
	for _, path := range []string{"/healthz", "/api/executors", "/api/apps"} {
		response, err := http.Head(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || len(body) != 0 || response.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("HEAD %s changed: %d %s err=%v", path, response.StatusCode, body, err)
		}
		response, err = http.Post(ts.URL+path, "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 405 || response.Header.Get("Allow") != "GET, HEAD" {
			t.Fatalf("POST %s changed: %d Allow=%q", path, response.StatusCode, response.Header.Get("Allow"))
		}
	}
}
