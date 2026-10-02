package console_test

import (
	"fmt"
	"html"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

// The fake follows the released Python 3.1.0 exact-name list handler, including
// server-side AND filters and offset/limit. Console substring matching must not
// depend on a nonexistent fuzzy wire option.
func TestConsoleWorkflowNameSearchPagesCandidatesBeforeMatches(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	var mu sync.Mutex
	var bodies []map[string]any
	failLater := false
	candidates := make([]map[string]any, 0)
	for i := 0; i < 52; i++ {
		candidates = append(candidates, map[string]any{"WorkflowUUID": fmt.Sprintf("run-billing-%02d", i), "WorkflowName": "billing", "Status": "SUCCESS", "QueueName": "orders"})
	}
	for i := 0; i < 31; i++ {
		candidates = append(candidates, map[string]any{"WorkflowUUID": fmt.Sprintf("run-gate-%02d", i), "WorkflowName": "gate_workflow", "Status": "SUCCESS", "QueueName": "orders"})
	}
	candidates = append(candidates, map[string]any{"WorkflowUUID": "other-gate", "WorkflowName": "gate_workflow", "Status": "ERROR", "QueueName": "different"})
	testserver.Connect(t, ts, "search-app", "testkey", "executor", map[protocol.MessageType]testserver.Responder{protocol.MsgListWorkflows: func(req map[string]any) map[string]any {
		b := req["body"].(map[string]any)
		mu.Lock()
		bodies = append(bodies, b)
		fail := failLater
		mu.Unlock()
		offset := int(b["offset"].(float64))
		limit := int(b["limit"].(float64))
		if fail && offset >= 26 {
			return map[string]any{"error_message": "later candidate page refused"}
		}
		var filtered []map[string]any
		for _, wf := range candidates {
			ok := true
			for key, field := range map[string]string{"workflow_name": "WorkflowName", "status": "Status", "queue_name": "QueueName"} {
				if v, exists := b[key]; exists && !reflect.DeepEqual(v, []any{wf[field]}) {
					ok = false
				}
			}
			if v, exists := b["workflow_id_prefix"]; exists && !strings.HasPrefix(wf["WorkflowUUID"].(string), v.([]any)[0].(string)) {
				ok = false
			}
			if ok {
				filtered = append(filtered, wf)
			}
		}
		if offset > len(filtered) {
			offset = len(filtered)
		}
		end := offset + limit
		if end > len(filtered) {
			end = len(filtered)
		}
		return map[string]any{"output": filtered[offset:end]}
	}})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	base := ts.URL + "/apps/search-app/workflows"
	for _, query := range []string{"gate", "work", "GATE", "  gate  "} {
		_, body := testserver.Get(t, base+"?name="+url.QueryEscape(query)+"&status=SUCCESS&queue=orders&id_prefix=run-")
		if !strings.Contains(body, "run-gate-00") || !strings.Contains(body, "run-gate-24") || strings.Contains(body, "run-billing") || strings.Contains(body, "other-gate") {
			t.Fatalf("query %q must display first 25 matching runs after multiple empty candidate pages: %s", query, body)
		}
		next := searchPagerURL(t, body, "Next")
		_, second := testserver.Get(t, ts.URL+next)
		if !strings.Contains(second, "run-gate-25") || !strings.Contains(second, "run-gate-30") || !strings.Contains(second, "26–31") || strings.Contains(second, "run-gate-24") {
			t.Fatalf("second matching page incorrect: %s", second)
		}
		prev := searchPagerURL(t, second, "Prev")
		_, first := testserver.Get(t, ts.URL+prev)
		if !strings.Contains(first, "run-gate-00") {
			t.Fatalf("previous page lost filters: %s", first)
		}
		for _, link := range []string{next, prev} {
			u, _ := url.Parse(link)
			for k, v := range map[string]string{"name": strings.TrimSpace(query), "status": "SUCCESS", "queue": "orders", "id_prefix": "run-"} {
				if u.Query().Get(k) != v {
					t.Fatalf("pager %q lost %s", link, k)
				}
			}
		}
	}
	for _, query := range []string{"", "   "} {
		_, body := testserver.Get(t, base+"?name="+url.QueryEscape(query)+"&status=SUCCESS&queue=orders&id_prefix=run-")
		if !strings.Contains(body, "run-billing-00") {
			t.Fatalf("blank query must restore first non-name-filtered page: %s", body)
		}
	}
	_, empty := testserver.Get(t, base+"/rows?name=missing")
	if !strings.Contains(empty, "No workflows match these filters.") {
		t.Fatalf("empty success missing: %s", empty)
	}
	_, literal := testserver.Get(t, base+"/rows?name=gate.*")
	if !strings.Contains(literal, "No workflows match these filters.") {
		t.Fatalf("query must be a literal substring: %s", literal)
	}
	_, jsonBody := testserver.Get(t, ts.URL+"/api/search-app/workflows?name=gate")
	if strings.Contains(jsonBody, "run-gate") {
		t.Fatalf("JSON exact-name semantics changed: %s", jsonBody)
	}
	_, jsonExact := testserver.Get(t, ts.URL+"/api/search-app/workflows?name=gate_workflow")
	if !strings.Contains(jsonExact, "run-gate-00") {
		t.Fatalf("JSON exact name failed: %s", jsonExact)
	}
	mu.Lock()
	for _, b := range bodies {
		if b["load_input"] != false || b["load_output"] != false {
			t.Errorf("candidate list loaded blobs: %#v", b)
		}
		if b["limit"].(float64) > 26 {
			t.Errorf("unbounded candidate read: %#v", b)
		}
	}
	failLater = true
	mu.Unlock()
	status, failed := testserver.Get(t, base+"?name=gate")
	if status != http.StatusBadGateway || !strings.Contains(failed, "later candidate page refused") || strings.Contains(failed, "No workflows match") {
		t.Fatalf("later failure became empty success: %d %s", status, failed)
	}
	_, failed = testserver.Get(t, base+"/rows?name=gate")
	if !strings.Contains(failed, "later candidate page refused") || strings.Contains(failed, "No workflows match") {
		t.Fatalf("HTMX later failure became empty success: %s", failed)
	}
}

func searchPagerURL(t *testing.T, body, label string) string {
	t.Helper()
	re := regexp.MustCompile(`<button hx-get="([^"]+)" hx-target="#wf-rows">[^<]*` + label)
	m := re.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("missing %s page button: %s", label, body)
	}
	return html.UnescapeString(m[1])
}
