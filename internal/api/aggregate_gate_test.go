package api_test

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

func TestAdvancedAggregatesDisabledByDefault(t *testing.T) {
	ts, h := newTestServer(t)
	var queries atomic.Int32
	read := func(map[string]any) map[string]any {
		queries.Add(1)
		return map[string]any{"output": []any{}}
	}
	dialFake(t, ts, "fixture-app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflowAggregates: read,
		protocol.MsgGetStepAggregates:     read,
		protocol.MsgListWorkflows:         func(map[string]any) map[string]any { return map[string]any{"output": []any{}} },
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, path := range []string{"/apps/fixture-app/aggregates/workflows", "/apps/fixture-app/aggregates/steps"} {
		code, body := getBody(t, ts.URL+path)
		if code != http.StatusNotFound || !strings.Contains(body, "disabled") {
			t.Fatalf("disabled Console query %s: %d", path, code)
		}
	}
	for _, path := range []string{localV2WorkflowAggregatesPath, localV2StepAggregatesPath} {
		code, _, body := localV2Request(t, ts.URL+path, http.MethodPost, `{"groupByStatus":true,"selectCount":true}`)
		if code != http.StatusNotFound || !strings.Contains(body, "disabled") {
			t.Fatalf("disabled API query %s: %d", path, code)
		}
	}
	if queries.Load() != 0 {
		t.Fatalf("disabled advanced queries dispatched %d reads", queries.Load())
	}
	for _, path := range []string{"/apps/fixture-app", "/apps/fixture-app/workflows"} {
		code, body := getBody(t, ts.URL+path)
		if code != http.StatusOK || strings.Contains(body, `href="/apps/fixture-app/aggregates/`) {
			t.Fatalf("disabled aggregate navigation remains on %s", path)
		}
	}
	for _, panel := range []string{"activity", "workload"} {
		code, body := getBody(t, ts.URL+"/apps/fixture-app/overview/"+panel)
		if code != http.StatusOK || !strings.Contains(body, `data-loaded="true"`) {
			t.Fatalf("fixed overview query %s failed: %d %s", panel, code, body)
		}
	}
	if queries.Load() != 2 {
		t.Fatalf("fixed overview queries dispatched %d reads, want 2", queries.Load())
	}
}

func TestAdvancedAggregatesEnabled(t *testing.T) {
	ts, h := localV2Server(t)
	var queries atomic.Int32
	read := func(map[string]any) map[string]any {
		queries.Add(1)
		return map[string]any{"output": []any{}}
	}
	dialFake(t, ts, "fixture-app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflowAggregates: read,
		protocol.MsgGetStepAggregates:     read,
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, kind := range []string{"workflows", "steps"} {
		code, _ := getBody(t, ts.URL+"/apps/fixture-app/aggregates/"+kind)
		if code != http.StatusOK {
			t.Fatalf("enabled Console %s status %d", kind, code)
		}
		code, _, _ = localV2Request(t, ts.URL+"/v2/orgs/local/apps/fixture-app/"+kind+"/aggregates", http.MethodPost, `{"groupByStatus":true,"selectCount":true}`)
		if code != http.StatusOK {
			t.Fatalf("enabled API %s status %d", kind, code)
		}
	}
	if queries.Load() != 4 {
		t.Fatalf("enabled queries dispatched %d reads, want 4", queries.Load())
	}
}
