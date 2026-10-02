package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

const (
	workflowAggregatesPath = "/v2/orgs/local/apps/fixture-app/workflows/aggregates"
	stepAggregatesPath     = "/v2/orgs/local/apps/fixture-app/steps/aggregates"
	exportPath             = "/v2/orgs/local/apps/fixture-app/workflows/wf-1/export"
)

func inspectionExistingWorkflow(id any) map[string]any {
	// Export existence is intentionally independent of the stricter official
	// Workflow mapper; these are sufficient released-SDK fields for this read.
	return map[string]any{"WorkflowUUID": id, "Status": "SUCCESS", "UpdatedAt": nil, "Priority": nil}
}

func inspectionFixture(t *testing.T) (*testserver.Executor, string, *atomic.Int32) {
	t.Helper()
	ts, h := testserver.New(t, config.Config{EnableAggregates: true})
	var calls atomic.Int32
	fe := dialScheduleFake(t, ts.URL, "inspection-exec", "3.1.0", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflowAggregates: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": []any{testserver.WorkflowAggregate()}}
		},
		protocol.MsgGetStepAggregates: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": []any{testserver.StepAggregate()}}
		},
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			calls.Add(1)
			if req["workflow_id"] == "missing" {
				return map[string]any{"output": nil}
			}
			return map[string]any{"output": inspectionExistingWorkflow(req["workflow_id"])}
		},
		protocol.MsgExportWorkflow: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"serialized_workflow": "opaque<字>\\u0000/base64=="}
		},
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	return fe, ts.URL, &calls
}

func TestAPIInspectionSchemaAndWire(t *testing.T) {
	fe, base, _ := inspectionFixture(t)
	workflowBody := `{
		"groupByStatus":true,"groupByWorkflowName":false,"groupByQueueName":true,
		"groupByExecutorId":false,"groupByAppVersion":true,"groupByApplicationName":false,
		"selectCount":true,"selectMinCreatedAt":true,"selectMaxQueueWaitMs":false,
		"selectMaxTotalLatencyMs":true,"timeBucketSizeMs":0,
		"status":[],"startTime":"1970-01-01T00:00:00Z","endTime":"2026-09-30T01:02:03.456+02:00",
		"completedAfter":"2024-01-01T00:00:00Z","completedBefore":"2024-12-31T23:59:59Z",
		"dequeuedAfter":"2025-01-01T00:00:00Z","dequeuedBefore":"2025-12-31T23:59:59Z",
		"workflowName":[""],"appVersion":["v1"],"executorId":["exec"],"queueName":["queue"],
		"workflowIdPrefix":["wf-"],"workflowIds":["wf-1"],"forkedFrom":["source"],
		"parentWorkflowId":["parent"],"user":["alice"],"scheduleName":["nightly"],
		"wasForkedFrom":false,"hasParent":false,
		"attributes":{"zero":0,"false":false,"null":null,"nested":{"value":9007199254740993}}
	}`
	code, ct, raw := testserver.Request(t, base+workflowAggregatesPath, http.MethodPost, workflowBody)
	if code != http.StatusOK || !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("workflow aggregates: status=%d content-type=%s body=%s", code, ct, raw)
	}
	var workflows []map[string]any
	if err := json.Unmarshal([]byte(raw), &workflows); err != nil || len(workflows) != 1 {
		t.Fatalf("workflow aggregate response=%s err=%v", raw, err)
	}
	assertLocalV2Schema(t, "WorkflowAggregate", workflows[0])
	wantWorkflow := map[string]any{
		"group": map[string]any{"status": "SUCCESS", "queue_name": nil},
		"count": float64(0), "minCreatedAt": "1970-01-01T00:00:00.000Z",
		"maxQueueWaitMs": float64(0), "maxTotalLatencyMs": float64(0),
	}
	if !reflect.DeepEqual(workflows[0], wantWorkflow) {
		t.Fatalf("workflow aggregate=%#v want=%#v", workflows[0], wantWorkflow)
	}
	workflowWire := fe.Body(t, protocol.MsgGetWorkflowAggregates)
	wantWorkflowWire := map[string]any{
		"group_by_status": true, "group_by_name": false, "group_by_queue_name": true,
		"group_by_executor_id": false, "group_by_application_version": true,
		"group_by_application_name": false, "select_count": true,
		"select_min_created_at": true, "select_max_queue_wait_ms": false,
		"select_max_total_latency_ms": true, "time_bucket_size_ms": float64(0),
		"status": []any{}, "start_time": "1970-01-01T00:00:00Z",
		"end_time":        "2026-09-30T01:02:03.456+02:00",
		"completed_after": "2024-01-01T00:00:00Z", "completed_before": "2024-12-31T23:59:59Z",
		"dequeued_after": "2025-01-01T00:00:00Z", "dequeued_before": "2025-12-31T23:59:59Z",
		"name": []any{""}, "app_version": []any{"v1"}, "executor_id": []any{"exec"},
		"queue_name": []any{"queue"}, "workflow_id_prefix": []any{"wf-"},
		"workflow_ids": []any{"wf-1"}, "forked_from": []any{"source"},
		"parent_workflow_id": []any{"parent"}, "user": []any{"alice"},
		"schedule_name": []any{"nightly"}, "was_forked_from": false, "has_parent": false,
		"attributes": map[string]any{"zero": float64(0), "false": false, "null": nil, "nested": map[string]any{"value": float64(9007199254740992)}},
	}
	if !reflect.DeepEqual(workflowWire, wantWorkflowWire) {
		t.Fatalf("workflow aggregate wire=%#v want=%#v", workflowWire, wantWorkflowWire)
	}

	stepBody := `{
		"groupByFunctionName":true,"groupByStatus":false,"selectCount":false,
		"selectMaxDurationMs":true,"timeBucketSizeMs":1,"status":[],"stepName":[""],
		"workflowIdPrefix":["wf-"],"completedAfter":"2024-01-01T00:00:00Z",
		"completedBefore":"2024-12-31T23:59:59.999Z"
	}`
	code, ct, raw = testserver.Request(t, base+stepAggregatesPath, http.MethodPost, stepBody)
	if code != http.StatusOK || !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("step aggregates: status=%d content-type=%s body=%s", code, ct, raw)
	}
	var steps []map[string]any
	if err := json.Unmarshal([]byte(raw), &steps); err != nil || len(steps) != 1 {
		t.Fatalf("step aggregate response=%s err=%v", raw, err)
	}
	assertLocalV2Schema(t, "StepAggregate", steps[0])
	wantStep := map[string]any{
		"group": map[string]any{"function_name": "gate_step", "status": nil},
		"count": float64(0), "maxDurationMs": float64(0),
	}
	if !reflect.DeepEqual(steps[0], wantStep) {
		t.Fatalf("step aggregate=%#v want=%#v", steps[0], wantStep)
	}
	wantStepWire := map[string]any{
		"group_by_function_name": true, "group_by_status": false, "select_count": false,
		"select_max_duration_ms": true, "time_bucket_size_ms": float64(1),
		"status": []any{}, "function_name": []any{""}, "workflow_id_prefix": []any{"wf-"},
		"completed_after": "2024-01-01T00:00:00Z", "completed_before": "2024-12-31T23:59:59.999Z",
	}
	if got := fe.Body(t, protocol.MsgGetStepAggregates); !reflect.DeepEqual(got, wantStepWire) {
		t.Fatalf("step aggregate wire=%#v want=%#v", got, wantStepWire)
	}

	code, ct, raw = testserver.Request(t, base+exportPath, http.MethodGet, "")
	if code != http.StatusOK || !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("export: status=%d content-type=%s body=%s", code, ct, raw)
	}
	var exported map[string]any
	if err := json.Unmarshal([]byte(raw), &exported); err != nil {
		t.Fatal(err)
	}
	assertLocalV2Schema(t, "ExportWorkflowOutputBody", exported)
	if !reflect.DeepEqual(exported, map[string]any{"serializedWorkflow": "opaque<字>\\u0000/base64=="}) {
		t.Fatalf("opaque export changed: %#v", exported)
	}
	exportWire := fe.Body(t, protocol.MsgExportWorkflow)
	if len(exportWire) != 4 || exportWire["type"] != "export_workflow" || exportWire["workflow_id"] != "wf-1" || exportWire["export_children"] != false {
		t.Fatalf("omitted exportChildren wire=%#v", exportWire)
	}
	code, _, raw = testserver.Request(t, base+exportPath+"?exportChildren=true", http.MethodGet, "")
	if code != http.StatusOK || fe.Body(t, protocol.MsgExportWorkflow)["export_children"] != true {
		t.Fatalf("explicit exportChildren=true: status=%d body=%s wire=%#v", code, raw, fe.Body(t, protocol.MsgExportWorkflow))
	}
	code, _, raw = testserver.Request(t, base+exportPath+"?exportChildren=false", http.MethodGet, "")
	if code != http.StatusOK || fe.Body(t, protocol.MsgExportWorkflow)["export_children"] != false {
		t.Fatalf("explicit exportChildren=false: status=%d body=%s wire=%#v", code, raw, fe.Body(t, protocol.MsgExportWorkflow))
	}
	existence := fe.Body(t, protocol.MsgGetWorkflow)
	if len(existence) != 5 || existence["workflow_id"] != "wf-1" || existence["load_input"] != false || existence["load_output"] != false {
		t.Fatalf("export existence wire=%#v", existence)
	}
}

func TestAPIInspectionOmittedNullEmptyFalseAndZero(t *testing.T) {
	fe, base, _ := inspectionFixture(t)
	for _, tc := range []struct {
		name, path, body string
		command          protocol.MessageType
		want             map[string]any
	}{
		{"workflow-omitted", workflowAggregatesPath, `{}`, protocol.MsgGetWorkflowAggregates, map[string]any{}},
		{"workflow-nullable-arrays", workflowAggregatesPath, `{"status":null,"workflowName":null}`, protocol.MsgGetWorkflowAggregates, map[string]any{}},
		{"workflow-empty-false-zero", workflowAggregatesPath, `{"status":[],"groupByStatus":false,"timeBucketSizeMs":0,"attributes":{}}`, protocol.MsgGetWorkflowAggregates, map[string]any{"status": []any{}, "group_by_status": false, "time_bucket_size_ms": float64(0), "attributes": map[string]any{}}},
		{"step-omitted", stepAggregatesPath, `{}`, protocol.MsgGetStepAggregates, map[string]any{}},
		{"step-empty-false-zero", stepAggregatesPath, `{"status":[],"stepName":[],"groupByStatus":false,"timeBucketSizeMs":0}`, protocol.MsgGetStepAggregates, map[string]any{"status": []any{}, "function_name": []any{}, "group_by_status": false, "time_bucket_size_ms": float64(0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, _, raw := testserver.Request(t, base+tc.path, http.MethodPost, tc.body)
			if code != http.StatusOK {
				t.Fatalf("status=%d body=%s", code, raw)
			}
			if got := fe.Body(t, tc.command); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("wire=%#v want=%#v", got, tc.want)
			}
		})
	}
	// requestBody itself is optional in the pinned operation.
	code, _, raw := testserver.Request(t, base+workflowAggregatesPath, http.MethodPost, "")
	if code != http.StatusOK || len(fe.Body(t, protocol.MsgGetWorkflowAggregates)) != 0 {
		t.Fatalf("omitted request body: status=%d body=%s", code, raw)
	}
}

func TestAPIInspectionStrictRequestValidation(t *testing.T) {
	_, base, calls := inspectionFixture(t)
	workflowInvalid := []string{
		`null`, `[]`, `{"unknown":1}`, `{"$schema":"example"}`,
		`{"groupByStatus":true,"groupByStatus":false}`, `{"groupByStatus":null}`,
		`{"selectCount":0}`, `{"timeBucketSizeMs":null}`, `{"timeBucketSizeMs":1.5}`,
		`{"timeBucketSizeMs":9223372036854775808}`, `{"status":42}`, `{"status":[null]}`,
		`{"startTime":null}`, `{"startTime":""}`, `{"startTime":"2024-01-01"}`,
		`{"attributes":null}`, `{"attributes":[]}`, `{"attributes":"opaque"}`,
		`{"status":[]} trailing`, `{`,
	}
	stepInvalid := []string{
		`null`, `[]`, `{"unknown":1}`, `{"status":null}`, `{"stepName":null}`,
		`{"workflowIdPrefix":null}`, `{"status":[1]}`, `{"completedAfter":"not-a-date"}`,
		`{"groupByFunctionName":"true"}`, `{"timeBucketSizeMs":1e3}`,
		`{"selectCount":true,"selectCount":false}`,
	}
	before := calls.Load()
	for _, group := range []struct {
		path   string
		bodies []string
	}{{workflowAggregatesPath, workflowInvalid}, {stepAggregatesPath, stepInvalid}} {
		for _, body := range group.bodies {
			code, ct, raw := testserver.Request(t, base+group.path, http.MethodPost, body)
			if code != http.StatusBadRequest || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, `"detail"`) {
				t.Errorf("path=%s body=%s: status=%d content-type=%s response=%s", group.path, body, code, ct, raw)
			}
		}
	}
	for _, path := range []string{
		workflowAggregatesPath + "?limit=1",
		workflowAggregatesPath + "?bad=%zz",
		stepAggregatesPath + "?status=SUCCESS&status=ERROR",
		stepAggregatesPath + "?%FF=x",
		strings.Replace(workflowAggregatesPath, "fixture-app", "ab", 1),
		strings.Replace(stepAggregatesPath, "fixture-app", "Fixture-App", 1),
	} {
		code, _, _ := testserver.Request(t, base+path, http.MethodPost, `{}`)
		if code != http.StatusBadRequest {
			t.Errorf("invalid path/query %q status=%d", path, code)
		}
	}
	for _, suffix := range []string{
		"?exportChildren=", "?exportChildren=TRUE", "?exportChildren=0", "?exportChildren=null",
		"?exportChildren=true&exportChildren=false", "?unknown=false", "?bad=%zz", "?%FF=true",
	} {
		code, ct, raw := testserver.Request(t, base+exportPath+suffix, http.MethodGet, "")
		if code != http.StatusBadRequest || !strings.HasPrefix(ct, "application/problem+json") {
			t.Errorf("invalid export query %q: status=%d content-type=%s body=%s", suffix, code, ct, raw)
		}
	}
	for _, path := range []string{
		strings.Replace(exportPath, "fixture-app", "ab", 1),
		strings.Replace(exportPath, "fixture-app", "Fixture-App", 1),
		"/v2/orgs/local/apps/fixture-app/workflows/%FF/export",
	} {
		code, _, _ := testserver.Request(t, base+path, http.MethodGet, "")
		if code != http.StatusBadRequest {
			t.Errorf("invalid export path %q status=%d", path, code)
		}
	}
	if calls.Load() != before {
		t.Fatalf("invalid requests reached executor: before=%d after=%d", before, calls.Load())
	}

	request, err := http.NewRequest(http.MethodPost, base+workflowAggregatesPath, strings.NewReader(string([]byte{'{', '"', 's', 't', 'a', 't', 'u', 's', '"', ':', '[', '"', 0xff, '"', ']', '}'})))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || calls.Load() != before {
		t.Fatalf("invalid UTF-8 body status=%d calls=%d", response.StatusCode, calls.Load())
	}
}

func TestAPIExportRejectsEveryMixedQueryParameterBeforeDispatch(t *testing.T) {
	_, base, calls := inspectionFixture(t)
	queries := []string{
		"?exportChildren=true&unknown=1",
		"?unknown=1&exportChildren=true",
		"?exportChildren=false&unknown=1",
		"?unknown=1&exportChildren=false",
		"?exportChildren=true&%FF=x",
		"?%FF=x&exportChildren=true",
		"?exportChildren=false&%FF=x",
		"?%FF=x&exportChildren=false",
	}
	var wrongStatus int
	firstFailure := ""
	for range 32 {
		for _, query := range queries {
			code, contentType, body := testserver.Request(t, base+exportPath+query, http.MethodGet, "")
			if code != http.StatusBadRequest || !strings.HasPrefix(contentType, "application/problem+json") {
				wrongStatus++
				if firstFailure == "" {
					firstFailure = fmt.Sprintf("query=%q status=%d content-type=%s body=%s", query, code, contentType, body)
				}
			}
		}
	}
	if wrongStatus != 0 {
		t.Errorf("mixed export queries bypassed validation %d times; first: %s", wrongStatus, firstFailure)
	}
	if calls.Load() != 0 {
		t.Fatalf("mixed invalid export queries reached executor %d times", calls.Load())
	}
}

func TestAPIInspectionResponseBoundaries(t *testing.T) {
	aggregateCases := []struct {
		name    string
		command protocol.MessageType
		path    string
		payload map[string]any
		status  int
		want    string
	}{
		{"workflow-empty", protocol.MsgGetWorkflowAggregates, workflowAggregatesPath, map[string]any{"output": []any{}}, 200, "[]"},
		{"workflow-null-output", protocol.MsgGetWorkflowAggregates, workflowAggregatesPath, map[string]any{"output": nil}, 502, ""},
		{"workflow-missing-output", protocol.MsgGetWorkflowAggregates, workflowAggregatesPath, map[string]any{}, 502, ""},
		{"workflow-null-group", protocol.MsgGetWorkflowAggregates, workflowAggregatesPath, map[string]any{"output": []any{map[string]any{"group": nil, "count": nil, "min_created_at": nil, "max_queue_wait_ms": nil, "max_total_latency_ms": nil}}}, 502, ""},
		{"workflow-missing-nullable-wire-key", protocol.MsgGetWorkflowAggregates, workflowAggregatesPath, map[string]any{"output": []any{map[string]any{"group": map[string]any{}, "count": nil, "min_created_at": nil, "max_queue_wait_ms": nil}}}, 502, ""},
		{"workflow-invalid-group-value", protocol.MsgGetWorkflowAggregates, workflowAggregatesPath, map[string]any{"output": []any{map[string]any{"group": map[string]any{"status": 1}, "count": nil, "min_created_at": nil, "max_queue_wait_ms": nil, "max_total_latency_ms": nil}}}, 502, ""},
		{"workflow-invalid-count", protocol.MsgGetWorkflowAggregates, workflowAggregatesPath, map[string]any{"output": []any{map[string]any{"group": map[string]any{}, "count": 1.5, "min_created_at": nil, "max_queue_wait_ms": nil, "max_total_latency_ms": nil}}}, 502, ""},
		{"workflow-invalid-time", protocol.MsgGetWorkflowAggregates, workflowAggregatesPath, map[string]any{"output": []any{map[string]any{"group": map[string]any{}, "count": nil, "min_created_at": int64(253402300800000), "max_queue_wait_ms": nil, "max_total_latency_ms": nil}}}, 502, ""},
		{"step-empty", protocol.MsgGetStepAggregates, stepAggregatesPath, map[string]any{"output": []any{}}, 200, "[]"},
		{"step-null-output", protocol.MsgGetStepAggregates, stepAggregatesPath, map[string]any{"output": nil}, 502, ""},
		{"step-missing-key", protocol.MsgGetStepAggregates, stepAggregatesPath, map[string]any{"output": []any{map[string]any{"group": map[string]any{}, "count": nil}}}, 502, ""},
	}
	for _, tc := range aggregateCases {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := testserver.New(t, config.Config{EnableAggregates: true})
			dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]testserver.Responder{
				tc.command: func(map[string]any) map[string]any { return tc.payload },
			})
			testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
			code, ct, raw := testserver.Request(t, ts.URL+tc.path, http.MethodPost, `{}`)
			if code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", code, tc.status, raw)
			}
			if tc.want != "" && strings.TrimSpace(raw) != tc.want {
				t.Fatalf("body=%s want=%s", raw, tc.want)
			}
			if code >= 400 && !strings.HasPrefix(ct, "application/problem+json") {
				t.Fatalf("content-type=%s body=%s", ct, raw)
			}
		})
	}

	// Wire nulls for unselected measures are omitted from the nonnullable,
	// optional HTTP properties rather than changed into zero or JSON null.
	ts, h := testserver.New(t, config.Config{EnableAggregates: true})
	dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflowAggregates: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{map[string]any{"group": map[string]any{}, "count": nil, "min_created_at": nil, "max_queue_wait_ms": nil, "max_total_latency_ms": nil}}}
		},
	})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	code, _, raw := testserver.Request(t, ts.URL+workflowAggregatesPath, http.MethodPost, `{}`)
	var rows []map[string]any
	if err := json.Unmarshal([]byte(raw), &rows); err != nil || code != 200 || len(rows) != 1 || !reflect.DeepEqual(rows[0], map[string]any{"group": map[string]any{}}) {
		t.Fatalf("nullable measures: status=%d body=%s rows=%#v err=%v", code, raw, rows, err)
	}
}

func TestAPIExportMissingMalformedAndRefusal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		existence  map[string]any
		export     map[string]any
		status     int
		exportCall int32
	}{
		{"missing-workflow", map[string]any{"output": nil}, map[string]any{"serialized_workflow": "should-not-run"}, 404, 0},
		{"missing-existence-output", map[string]any{}, map[string]any{"serialized_workflow": "should-not-run"}, 502, 0},
		{"malformed-existence", map[string]any{"output": map[string]any{"Status": "SUCCESS"}}, map[string]any{"serialized_workflow": "should-not-run"}, 502, 0},
		{"missing-export-value", map[string]any{"output": inspectionExistingWorkflow("wf-1")}, map[string]any{}, 502, 1},
		{"null-export-value", map[string]any{"output": inspectionExistingWorkflow("wf-1")}, map[string]any{"serialized_workflow": nil}, 502, 1},
		{"executor-refusal", map[string]any{"output": inspectionExistingWorkflow("wf-1")}, map[string]any{"error_message": "metadata-only mode refuses export"}, 502, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := testserver.New(t, config.Config{EnableAggregates: true})
			var exportCalls atomic.Int32
			dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]testserver.Responder{
				protocol.MsgGetWorkflow:    func(map[string]any) map[string]any { return tc.existence },
				protocol.MsgExportWorkflow: func(map[string]any) map[string]any { exportCalls.Add(1); return tc.export },
			})
			testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
			code, ct, raw := testserver.Request(t, ts.URL+exportPath, http.MethodGet, "")
			if code != tc.status || !strings.HasPrefix(ct, "application/problem+json") || exportCalls.Load() != tc.exportCall {
				t.Fatalf("status=%d content-type=%s calls=%d body=%s", code, ct, exportCalls.Load(), raw)
			}
			if tc.name == "executor-refusal" && !strings.Contains(raw, "metadata-only mode refuses export") {
				t.Fatalf("refusal detail lost: %s", raw)
			}
		})
	}

	t.Run("empty opaque export string", func(t *testing.T) {
		ts, h := testserver.New(t, config.Config{EnableAggregates: true})
		dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]testserver.Responder{
			protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
				return map[string]any{"output": inspectionExistingWorkflow(req["workflow_id"])}
			},
			protocol.MsgExportWorkflow: func(map[string]any) map[string]any {
				return map[string]any{"serialized_workflow": ""}
			},
		})
		testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
		code, _, raw := testserver.Request(t, ts.URL+exportPath, http.MethodGet, "")
		if code != 200 || strings.TrimSpace(raw) != `{"serializedWorkflow":""}` {
			t.Fatalf("status=%d body=%s", code, raw)
		}
	})
}

func TestAPIInspectionCapabilitiesRetryAndUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name, path, method, body string
		command                  protocol.MessageType
	}{
		{"workflow", workflowAggregatesPath, http.MethodPost, `{}`, protocol.MsgGetWorkflowAggregates},
		{"step", stepAggregatesPath, http.MethodPost, `{}`, protocol.MsgGetStepAggregates},
		{"export", exportPath, http.MethodGet, "", protocol.MsgExportWorkflow},
	} {
		t.Run("attempt/"+tc.name, func(t *testing.T) {
			ts, h := testserver.New(t, config.Config{EnableAggregates: true})
			var calls atomic.Int32
			handlers := map[protocol.MessageType]testserver.Responder{
				protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
					return map[string]any{"output": inspectionExistingWorkflow(req["workflow_id"])}
				},
				tc.command: func(map[string]any) map[string]any {
					calls.Add(1)
					return map[string]any{"output": []any{}, "serialized_workflow": "opaque"}
				},
			}
			dialScheduleFake(t, ts.URL, "old", "2.31.1", handlers)
			testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
			code, _, raw := testserver.Request(t, ts.URL+tc.path, tc.method, tc.body)
			if code != 200 || calls.Load() != 1 {
				t.Fatalf("status=%d calls=%d body=%s", code, calls.Load(), raw)
			}
		})
	}

	t.Run("mixed peers can serve reads", func(t *testing.T) {
		for _, tc := range []struct {
			name, path, method, body string
			command                  protocol.MessageType
			response                 map[string]any
		}{
			{"workflow", workflowAggregatesPath, http.MethodPost, `{}`, protocol.MsgGetWorkflowAggregates, map[string]any{"output": []any{testserver.WorkflowAggregate()}}},
			{"step", stepAggregatesPath, http.MethodPost, `{}`, protocol.MsgGetStepAggregates, map[string]any{"output": []any{testserver.StepAggregate()}}},
			{"export", exportPath, http.MethodGet, "", protocol.MsgExportWorkflow, map[string]any{"serialized_workflow": "reviewed"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ts, h := testserver.New(t, config.Config{EnableAggregates: true})
				var reviewedCalls, unknownCalls atomic.Int32
				existence := func(req map[string]any) map[string]any {
					return map[string]any{"output": inspectionExistingWorkflow(req["workflow_id"])}
				}
				dialScheduleFake(t, ts.URL, "reviewed", "3.1.0", map[protocol.MessageType]testserver.Responder{
					protocol.MsgGetWorkflow: existence,
					tc.command: func(map[string]any) map[string]any {
						reviewedCalls.Add(1)
						return tc.response
					},
				})
				dialScheduleFake(t, ts.URL, "unknown", "3.1.1", map[protocol.MessageType]testserver.Responder{
					protocol.MsgGetWorkflow: existence,
					tc.command: func(map[string]any) map[string]any {
						unknownCalls.Add(1)
						return tc.response
					},
				})
				testserver.Wait(t, func() bool { return len(h.Executors()) == 2 })
				code, _, raw := testserver.Request(t, ts.URL+tc.path, tc.method, tc.body)
				if code != 200 || reviewedCalls.Load()+unknownCalls.Load() != 1 {
					t.Fatalf("status=%d reviewed=%d unknown=%d body=%s", code, reviewedCalls.Load(), unknownCalls.Load(), raw)
				}
			})
		}
	})

	t.Run("export metadata refusal is final", func(t *testing.T) {
		ts, h := testserver.New(t, config.Config{EnableAggregates: true})
		var exportCalls atomic.Int32
		for _, id := range []string{"one", "two"} {
			dialScheduleFake(t, ts.URL, id, "3.1.0", map[protocol.MessageType]testserver.Responder{
				protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
					return map[string]any{"output": inspectionExistingWorkflow(req["workflow_id"])}
				},
				protocol.MsgExportWorkflow: func(map[string]any) map[string]any {
					exportCalls.Add(1)
					return map[string]any{"error_message": "export_workflow is not allowed in conductor metadata-only mode"}
				},
			})
		}
		testserver.Wait(t, func() bool { return len(h.Executors()) == 2 })
		code, _, raw := testserver.Request(t, ts.URL+exportPath, http.MethodGet, "")
		if code != 502 || exportCalls.Load() != 1 || !strings.Contains(raw, "metadata-only mode") {
			t.Fatalf("status=%d calls=%d body=%s", code, exportCalls.Load(), raw)
		}
	})

	t.Run("aggregate disconnect retries another peer", func(t *testing.T) {
		ts, h := testserver.New(t, config.Config{EnableAggregates: true})
		var calls atomic.Int32
		handler := func(map[string]any) map[string]any {
			if calls.Add(1) == 1 {
				return nil
			}
			return map[string]any{"output": []any{}}
		}
		for _, id := range []string{"one", "two"} {
			dialScheduleFake(t, ts.URL, id, "3.1.0", map[protocol.MessageType]testserver.Responder{protocol.MsgGetWorkflowAggregates: handler})
		}
		testserver.Wait(t, func() bool { return len(h.Executors()) == 2 })
		code, _, raw := testserver.Request(t, ts.URL+workflowAggregatesPath, http.MethodPost, `{}`)
		if code != 200 || calls.Load() != 2 || strings.TrimSpace(raw) != "[]" {
			t.Fatalf("status=%d calls=%d body=%s", code, calls.Load(), raw)
		}
	})

	t.Run("executor error is final", func(t *testing.T) {
		ts, h := testserver.New(t, config.Config{EnableAggregates: true})
		var calls atomic.Int32
		for _, id := range []string{"one", "two"} {
			dialScheduleFake(t, ts.URL, id, "3.1.0", map[protocol.MessageType]testserver.Responder{
				protocol.MsgGetStepAggregates: func(map[string]any) map[string]any {
					calls.Add(1)
					return map[string]any{"error_message": "aggregate refused", "output": []any{}}
				},
			})
		}
		testserver.Wait(t, func() bool { return len(h.Executors()) == 2 })
		code, _, raw := testserver.Request(t, ts.URL+stepAggregatesPath, http.MethodPost, `{}`)
		if code != 502 || calls.Load() != 1 || !strings.Contains(raw, "aggregate refused") {
			t.Fatalf("status=%d calls=%d body=%s", code, calls.Load(), raw)
		}
	})

	unavailable, _ := testserver.New(t, config.Config{EnableAggregates: true})
	for _, tc := range []struct{ path, method, body string }{
		{workflowAggregatesPath, http.MethodPost, `{}`},
		{stepAggregatesPath, http.MethodPost, `{}`},
		{exportPath, http.MethodGet, ""},
	} {
		code, _, _ := testserver.Request(t, unavailable.URL+tc.path, tc.method, tc.body)
		if code != 503 {
			t.Errorf("unavailable %s status=%d", tc.path, code)
		}
	}
}

func TestAPIInspectionProblemResponsesHavePinnedShape(t *testing.T) {
	_, base, _ := inspectionFixture(t)
	code, ct, raw := testserver.Request(t, base+workflowAggregatesPath, http.MethodPost, `{"unknown":true}`)
	if code != 400 || !strings.HasPrefix(ct, "application/problem+json") {
		t.Fatalf("status=%d content-type=%s body=%s", code, ct, raw)
	}
	var problem map[string]any
	if err := json.Unmarshal([]byte(raw), &problem); err != nil {
		t.Fatal(err)
	}
	wantFields := map[string]bool{"type": true, "title": true, "status": true, "detail": true}
	if len(problem) != len(wantFields) {
		t.Fatalf("problem fields=%#v", problem)
	}
	for field := range wantFields {
		if _, ok := problem[field]; !ok {
			t.Fatalf("problem missing %s: %#v", field, problem)
		}
	}
}
