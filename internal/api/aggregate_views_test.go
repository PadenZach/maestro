package api_test

import (
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

// CONTRACTS aggregate reads and pinned WorkflowAggregatesBody / StepAggregatesBody:
// the Console exposes these reads independently of the optional HTTP adapter.
func TestConsoleAggregatesNavigationFieldsAndReads(t *testing.T) {
	ts, h := localV2Server(t)
	fe := dialFake(t, ts, "fixture-app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflowAggregates: func(map[string]any) map[string]any {
			r := inspectionWorkflowAggregateWire()
			r["group"] = map[string]any{"status": "<script>unsafe</script>", "queue_name": nil}
			return map[string]any{"output": []any{r}}
		},
		protocol.MsgGetStepAggregates: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{inspectionStepAggregateWire()}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	code, _, html := localV2Request(t, ts.URL+"/apps/fixture-app", "GET", "")
	if code != 200 || !strings.Contains(html, `href="/apps/fixture-app/aggregates/workflows"`) {
		t.Errorf("application missing aggregate navigation: %d", code)
	}
	for _, tc := range []struct {
		kind    string
		command protocol.MessageType
		fields  []string
		columns []string
	}{
		{"workflows", protocol.MsgGetWorkflowAggregates, []string{"groupByStatus", "groupByWorkflowName", "groupByQueueName", "groupByExecutorId", "groupByAppVersion", "groupByApplicationName", "selectCount", "selectMinCreatedAt", "selectMaxQueueWaitMs", "selectMaxTotalLatencyMs", "timeBucketSizeMs", "status", "startTime", "endTime", "completedAfter", "completedBefore", "dequeuedAfter", "dequeuedBefore", "workflowName", "appVersion", "executorId", "queueName", "workflowIdPrefix", "workflowIds", "forkedFrom", "parentWorkflowId", "user", "scheduleName", "wasForkedFrom", "hasParent", "attributes"}, []string{"Count", "Earliest created", "Max queue wait (ms)", "Max total latency (ms)"}},
		{"steps", protocol.MsgGetStepAggregates, []string{"groupByFunctionName", "groupByStatus", "selectCount", "selectMaxDurationMs", "timeBucketSizeMs", "status", "stepName", "workflowIdPrefix", "completedAfter", "completedBefore"}, []string{"Count", "Max duration (ms)"}},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			code, _, html := localV2Request(t, ts.URL+"/apps/fixture-app/aggregates/"+tc.kind, "GET", "")
			if code != 200 {
				t.Fatalf("aggregate page status=%d body=%s", code, html)
			}
			for _, field := range tc.fields {
				if !strings.Contains(html, `name="`+field+`"`) {
					t.Errorf("missing aggregate control %s", field)
				}
			}
			for _, column := range tc.columns {
				if !strings.Contains(html, column) {
					t.Errorf("missing result column %s", column)
				}
			}
			if !strings.Contains(html, `<td>0</td>`) || !strings.Contains(html, `null`) {
				t.Error("result lost zero/null")
			}
			if strings.Contains(html, `<script>unsafe</script>`) {
				t.Error("aggregate group executed as HTML")
			}
			if tc.kind == "workflows" && !strings.Contains(html, `&lt;script&gt;unsafe&lt;/script&gt;`) {
				t.Error("group text lost")
			}
			body := fe.body(t, tc.command)
			if body["group_by_status"] != true || body["select_count"] != true {
				t.Errorf("default grouping/count missing: %#v", body)
			}
		})
	}
}

func TestConsoleAggregateFiltersPreserveValues(t *testing.T) {
	ts, h := localV2Server(t)
	fe := dialFake(t, ts, "fixture-app", "testkey", "exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflowAggregates: func(map[string]any) map[string]any { return map[string]any{"output": []any{}} },
		protocol.MsgGetStepAggregates:     func(map[string]any) map[string]any { return map[string]any{"output": []any{}} },
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	query := url.Values{"groupByStatus": {"true"}, "selectCount": {"false"}, "timeBucketSizeMs": {"0"}, "executorId": {"executor-a\nexecutor-b"}, "dequeuedAfter": {"2024-01-01T00:00:00Z"}, "hasParent": {"false"}, "attributes": {`{"tenant":"a","value":9007199254740993}`}}
	code, _, html := localV2Request(t, ts.URL+"/apps/fixture-app/aggregates/workflows?"+query.Encode(), "GET", "")
	if code != 200 || !strings.Contains(html, "No matching aggregates") {
		t.Fatalf("empty aggregate results: %d %s", code, html)
	}
	body := fe.body(t, protocol.MsgGetWorkflowAggregates)
	for name, want := range map[string]any{"group_by_status": true, "select_count": false, "time_bucket_size_ms": float64(0), "executor_id": []any{"executor-a", "executor-b"}, "dequeued_after": "2024-01-01T00:00:00Z", "has_parent": false} {
		if !reflect.DeepEqual(body[name], want) {
			t.Errorf("wire %s=%#v want=%#v", name, body[name], want)
		}
	}
	query = url.Values{"groupByFunctionName": {"true"}, "stepName": {"one\ntwo"}, "selectMaxDurationMs": {"true"}, "completedBefore": {"2025-01-01T00:00:00Z"}}
	code, _, _ = localV2Request(t, ts.URL+"/apps/fixture-app/aggregates/steps?"+query.Encode(), "GET", "")
	if code != 200 {
		t.Fatalf("step filters status=%d", code)
	}
	step := fe.body(t, protocol.MsgGetStepAggregates)
	if !reflect.DeepEqual(step["function_name"], []any{"one", "two"}) || step["select_max_duration_ms"] != true {
		t.Errorf("step filters=%#v", step)
	}
}

func TestConsoleAggregateErrors(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		response    map[string]any
		want        int
		detail      string
		calls       int32
	}{
		{"invalid boolean", "?groupByStatus=maybe", nil, 400, "groupByStatus", 0},
		{"duplicate", "?status=a&status=b", nil, 400, "duplicate", 0},
		{"unknown", "?ignored=true", nil, 400, "unsupported", 0},
		{"refusal", "", map[string]any{"error_message": "aggregate read refused"}, 502, "aggregate read refused", 1},
		{"null", "", map[string]any{"output": nil}, 502, "missing or null", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := localV2Server(t)
			var calls atomic.Int32
			dialFake(t, ts, "fixture-app", "testkey", "exec", map[protocol.MessageType]respondFn{protocol.MsgGetWorkflowAggregates: func(map[string]any) map[string]any { calls.Add(1); return tc.response }})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			code, _, html := localV2Request(t, ts.URL+"/apps/fixture-app/aggregates/workflows"+tc.query, "GET", "")
			if code != tc.want || !strings.Contains(html, tc.detail) || calls.Load() != tc.calls {
				t.Fatalf("aggregate error status=%d calls=%d body=%s", code, calls.Load(), html)
			}
		})
	}
}
