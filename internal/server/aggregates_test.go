package server_test

import (
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

func TestAdvancedAggregatesDisabledByDefault(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	var queries atomic.Int32
	read := func(map[string]any) map[string]any {
		queries.Add(1)
		return map[string]any{"output": []any{}}
	}
	testserver.Connect(t, ts, "fixture-app", "testkey", "exec", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflowAggregates: read,
		protocol.MsgGetStepAggregates:     read,
		protocol.MsgListWorkflows:         func(map[string]any) map[string]any { return map[string]any{"output": []any{}} },
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	for _, path := range []string{"/apps/fixture-app/aggregates/workflows", "/apps/fixture-app/aggregates/steps"} {
		code, body := testserver.Get(t, ts.URL+path)
		if code != http.StatusNotFound || !strings.Contains(body, "disabled") {
			t.Fatalf("disabled Console query %s: %d", path, code)
		}
	}
	for _, path := range []string{"/v2/orgs/local/apps/fixture-app/workflows/aggregates", "/v2/orgs/local/apps/fixture-app/steps/aggregates"} {
		code, _, body := testserver.Request(t, ts.URL+path, http.MethodPost, `{"groupByStatus":true,"selectCount":true}`)
		if code != http.StatusNotFound || !strings.Contains(body, "disabled") {
			t.Fatalf("disabled API query %s: %d", path, code)
		}
	}
	if queries.Load() != 0 {
		t.Fatalf("disabled advanced queries dispatched %d reads", queries.Load())
	}
	for _, path := range []string{"/apps/fixture-app", "/apps/fixture-app/workflows"} {
		code, body := testserver.Get(t, ts.URL+path)
		if code != http.StatusOK || strings.Contains(body, `href="/apps/fixture-app/aggregates/`) {
			t.Fatalf("disabled aggregate navigation remains on %s", path)
		}
	}
	for _, panel := range []string{"activity", "workload"} {
		code, body := testserver.Get(t, ts.URL+"/apps/fixture-app/overview/"+panel)
		if code != http.StatusOK || !strings.Contains(body, `data-loaded="true"`) {
			t.Fatalf("fixed overview query %s failed: %d %s", panel, code, body)
		}
	}
	if queries.Load() != 2 {
		t.Fatalf("fixed overview queries dispatched %d reads, want 2", queries.Load())
	}
}

func TestAdvancedAggregatesEnabled(t *testing.T) {
	ts, h := testserver.New(t, config.Config{EnableAggregates: true})
	var queries atomic.Int32
	read := func(map[string]any) map[string]any {
		queries.Add(1)
		return map[string]any{"output": []any{}}
	}
	testserver.Connect(t, ts, "fixture-app", "testkey", "exec", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflowAggregates: read,
		protocol.MsgGetStepAggregates:     read,
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	for _, kind := range []string{"workflows", "steps"} {
		code, _ := testserver.Get(t, ts.URL+"/apps/fixture-app/aggregates/"+kind)
		if code != http.StatusOK {
			t.Fatalf("enabled Console %s status %d", kind, code)
		}
		code, _, _ = testserver.Request(t, ts.URL+"/v2/orgs/local/apps/fixture-app/"+kind+"/aggregates", http.MethodPost, `{"groupByStatus":true,"selectCount":true}`)
		if code != http.StatusOK {
			t.Fatalf("enabled API %s status %d", kind, code)
		}
	}
	if queries.Load() != 4 {
		t.Fatalf("enabled queries dispatched %d reads, want 4", queries.Load())
	}
}
