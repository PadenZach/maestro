package api_test

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

func TestConsoleWorkflowDrilldownFiltersSurviveSearchPaginationAndReload(t *testing.T) {
	ts, h := newTestServer(t)
	const start = "2026-10-01T00:00:00.000Z"
	const end = "2026-10-01T00:59:59.999Z"
	statuses := []string{"ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED", "FUTURE_STATUS"}
	var mu sync.Mutex
	var bodies []map[string]any
	dialFake(t, ts, "filtered", "testkey", "executor", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(req map[string]any) map[string]any {
			b := req["body"].(map[string]any)
			mu.Lock()
			bodies = append(bodies, b)
			mu.Unlock()
			offset := int(b["offset"].(float64))
			limit := int(b["limit"].(float64))
			rows := []map[string]any{}
			for i := offset; i < min(offset+limit, 31); i++ {
				rows = append(rows, map[string]any{"WorkflowUUID": fmt.Sprintf("filtered-run-%02d", i), "WorkflowName": "gate_workflow", "Status": statuses[i%len(statuses)], "CreatedAt": start})
			}
			return map[string]any{"output": rows}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, name := range []string{"", "GATE"} {
		q := url.Values{"status": statuses, "start_time": {start}, "end_time": {end}, "queue": {"orders"}, "id_prefix": {"filtered-"}, "children": {"true"}}
		if name != "" {
			q.Set("name", name)
		}
		base := "/apps/filtered/workflows"
		status, body := getBody(t, ts.URL+base+"?"+q.Encode())
		if status != http.StatusOK || !strings.Contains(body, `value="FUTURE_STATUS" checked`) || !strings.Contains(body, `value="`+start+`"`) || !strings.Contains(body, `type="datetime-local" step="0.001"`) {
			t.Fatalf("drilldown controls do not retain URL filters: %d %s", status, body)
		}
		next := searchPagerURL(t, body, "Next")
		nextURL, _ := url.Parse(next)
		assertWorkflowFilterQuery(t, nextURL.Query(), q, "25")
		resp, err := http.Get(ts.URL + next)
		if err != nil {
			t.Fatal(err)
		}
		secondBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		second := string(secondBytes)
		if !strings.Contains(second, "filtered-run-25") || !strings.Contains(second, "26–31") {
			t.Fatalf("second page: %s", second)
		}
		pushed, err := url.Parse(resp.Header.Get("HX-Push-Url"))
		if err != nil || pushed.Path != base {
			t.Fatalf("rows history must refer to full page: %q", resp.Header.Get("HX-Push-Url"))
		}
		assertWorkflowFilterQuery(t, pushed.Query(), q, "25")
		_, reloaded := getBody(t, ts.URL+pushed.String())
		if !strings.Contains(reloaded, "<html") || !strings.Contains(reloaded, "filtered-run-25") || strings.Contains(reloaded, "filtered-run-00") {
			t.Fatalf("full-page reload lost offset: %s", reloaded)
		}
		prev := searchPagerURL(t, second, "Prev")
		prevURL, _ := url.Parse(prev)
		assertWorkflowFilterQuery(t, prevURL.Query(), q, "0")
		_, previous := getBody(t, ts.URL+prev)
		if !strings.Contains(previous, "filtered-run-00") {
			t.Fatalf("back page lost filters: %s", previous)
		}
		if !strings.Contains(second, `hx-get="`+strings.ReplaceAll(next, "&", "&amp;")+`" hx-target="#wf-rows">Refresh`) {
			t.Fatalf("refresh lost current offset or filters: %s", second)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for _, b := range bodies {
		if !reflect.DeepEqual(b["status"], []any{"ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED", "FUTURE_STATUS"}) || b["start_time"] != start || b["end_time"] != end || !reflect.DeepEqual(b["queue_name"], []any{"orders"}) || !reflect.DeepEqual(b["workflow_id_prefix"], []any{"filtered-"}) {
			t.Errorf("executor read lost cohort filters: %#v", b)
		}
		if _, ok := b["workflow_name"]; ok {
			t.Errorf("Console name matching must be local substring matching: %#v", b)
		}
		if b["load_input"] != false || b["load_output"] != false {
			t.Errorf("list read loaded blobs: %#v", b)
		}
	}
}

func assertWorkflowFilterQuery(t *testing.T, got, want url.Values, offset string) {
	t.Helper()
	for key, values := range want {
		if !reflect.DeepEqual(got[key], values) {
			t.Errorf("%s = %#v, want %#v", key, got[key], values)
		}
	}
	if got.Get("offset") != offset {
		t.Errorf("offset = %s, want %s", got.Get("offset"), offset)
	}
}

func TestConsoleWorkflowDefaultsToRootsWithoutChangingJSONAPI(t *testing.T) {
	ts, h := newTestServer(t)
	var mu sync.Mutex
	var bodies []map[string]any
	dialFake(t, ts, "family", "testkey", "executor", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(req map[string]any) map[string]any {
			b := req["body"].(map[string]any)
			mu.Lock()
			bodies = append(bodies, b)
			mu.Unlock()
			rows := []map[string]any{{"WorkflowUUID": "child-run", "WorkflowName": "family_job", "Status": "SUCCESS", "ParentWorkflowID": "parent-run"}}
			for i := 0; i < 26; i++ {
				rows = append(rows, map[string]any{"WorkflowUUID": fmt.Sprintf("parent-run-%02d", i), "WorkflowName": "family_job", "Status": "SUCCESS", "ParentWorkflowID": nil})
			}
			if parent, exists := b["has_parent"]; exists && parent == false {
				rows = rows[1:]
			}
			offset := int(b["offset"].(float64))
			limit := int(b["limit"].(float64))
			return map[string]any{"output": rows[offset:min(offset+limit, len(rows))]}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, query := range []string{"", "?name=FAMILY", "?children=false"} {
		_, body := getBody(t, ts.URL+"/apps/family/workflows"+query)
		if strings.Contains(body, "child-run") || !strings.Contains(body, "parent-run-00") {
			t.Fatalf("default list should show roots: %s", body)
		}
		next, _ := url.Parse(searchPagerURL(t, body, "Next"))
		if next.Query().Get("children") != "false" {
			t.Errorf("root-only pagination lost scope: %s", next)
		}
	}
	mu.Lock()
	for _, body := range bodies {
		if body["has_parent"] != false {
			t.Errorf("default Console read must request has_parent=false: %#v", body)
		}
	}
	mu.Unlock()
	_, all := getBody(t, ts.URL+"/apps/family/workflows?children=true&name=family")
	if !strings.Contains(all, "child-run") || !strings.Contains(all, `name="children" value="true" checked`) {
		t.Fatalf("child toggle did not include children: %s", all)
	}
	next, _ := url.Parse(searchPagerURL(t, all, "Next"))
	if next.Query().Get("children") != "true" {
		t.Errorf("all-executions pagination lost scope: %s", next)
	}
	_, legacy := getBody(t, ts.URL+"/api/family/workflows")
	if !strings.Contains(legacy, "child-run") {
		t.Fatalf("legacy JSON scope changed: %s", legacy)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, body := range bodies[len(bodies)-2:] {
		if _, present := body["has_parent"]; present {
			t.Errorf("all-executions read must omit has_parent: %#v", body)
		}
	}
}

func TestConsoleWorkflowInvalidFiltersNeverReadExecutor(t *testing.T) {
	ts, h := newTestServer(t)
	var mu sync.Mutex
	reads := 0
	dialFake(t, ts, "filtered", "testkey", "executor", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(req map[string]any) map[string]any {
			mu.Lock()
			reads++
			mu.Unlock()
			return map[string]any{"output": []any{}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, query := range []string{
		"start_time=not-a-time",
		"end_time=2026-10-01T00%3A00%3A00",
		"start_time=2026-10-02T00%3A00%3A00.000Z&end_time=2026-10-01T00%3A00%3A00.000Z",
		"start_time=&start_time=2026-10-01T00%3A00%3A00.000Z",
		"end_time=2026-10-01T00%3A00%3A00.000Z&end_time=2026-10-01T00%3A00%3A00.000Z",
		"start_time=2026-10-01T00%3A00%3A00.0001Z",
		"start_time=2026-10-01T00%3A00%3A00-05%3A00",
		"status_is_null=true",
		"offset=-1",
		"children=unsupported",
		"children=true&children=false",
	} {
		for _, path := range []string{"/apps/filtered/workflows", "/apps/filtered/workflows/rows"} {
			status, body := getBody(t, ts.URL+path+"?"+query)
			if status != http.StatusBadRequest || strings.Contains(body, "No workflows match") {
				t.Errorf("bad query %s: %d %s", query, status, body)
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if reads != 0 {
		t.Errorf("invalid input issued %d executor reads", reads)
	}
}
