package api_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/zpaden/maestro/internal/protocol"
)

const localV2ScheduleRoot = "/v2/orgs/local/apps/fixture-app/schedules"

// dialScheduleFake is the existing fake executor with the reviewed SDK version
// included in its handshake. Schedule reads deliberately fail closed without it.
func dialScheduleFake(t *testing.T, tsURL, execID, sdkVersion string, handlers map[protocol.MessageType]respondFn) *fakeExec {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(tsURL, "http")+"/websocket/fixture-app/testkey", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read executor_info request: %v", err)
	}
	env, err := protocol.DecodeEnvelope(data)
	if err != nil || env.Type != protocol.MsgExecutorInfo {
		t.Fatalf("executor_info request: envelope=%+v err=%v", env, err)
	}
	language := "python"
	out, err := json.Marshal(protocol.ExecutorInfoResponse{
		Type: protocol.MsgExecutorInfo, RequestID: env.RequestID, ExecutorID: execID,
		ApplicationVersion: "fixture-v1", Language: &language, DBOSVersion: &sdkVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write(ctx, websocket.MessageText, out); err != nil {
		t.Fatalf("write executor_info response: %v", err)
	}
	fe := &fakeExec{c: c, captured: map[string]map[string]any{}}
	done := make(chan struct{})
	go func() { defer close(done); fe.loop(handlers) }()
	t.Cleanup(func() {
		_ = c.CloseNow()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("schedule fake executor did not stop")
		}
	})
	return fe
}

// Independent values from the pinned Schedule schema and the released Python
// 3.1.0 ScheduleOutput definition and list/get handlers.
func scheduleWireRecord() map[string]any {
	return map[string]any{
		"schedule_id": "schedule-id-1", "schedule_name": "nightly", "workflow_name": "job",
		"workflow_class_name": nil, "schedule": "0 0 1 1 *", "status": "ACTIVE",
		"context": "opaque-sdk-context", "last_fired_at": "2026-03-01T01:02:03.456-05:00",
		"automatic_backfill": false, "cron_timezone": nil, "queue_name": "jobs", "application_name": nil,
	}
}

func TestLocalHTTPV2ScheduleSchemaWireAndFilters(t *testing.T) {
	ts, h := localV2Server(t, true)
	var listCalls atomic.Int32
	fe := dialScheduleFake(t, ts.URL, "schedule-exec", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgListSchedules: func(req map[string]any) map[string]any {
			if req["type"] != "list_schedules" {
				t.Errorf("list type is not lowercase: %v", req["type"])
			}
			call := listCalls.Add(1)
			body, _ := req["body"].(map[string]any)
			switch call {
			case 1:
				for name, want := range map[string]any{
					"status": []any{"ACTIVE"}, "workflow_name": []any{"job"},
					"schedule_name_prefix": []any{"nightly"}, "load_context": false,
				} {
					if !reflect.DeepEqual(body[name], want) {
						t.Errorf("list wire %s=%#v want %#v", name, body[name], want)
					}
				}
				if _, ok := body["application_name"]; ok {
					t.Error("HTTP route invented an SDK-only application_name filter")
				}
			case 2:
				if len(body) != 0 {
					t.Errorf("omitted filters changed SDK defaults: %#v", body)
				}
			case 3:
				for _, name := range []string{"status", "workflow_name", "schedule_name_prefix"} {
					if !reflect.DeepEqual(body[name], []any{""}) {
						t.Errorf("empty %s not preserved: %#v", name, body[name])
					}
				}
			}
			return map[string]any{"output": []any{scheduleWireRecord()}}
		},
		protocol.MsgGetSchedule: func(req map[string]any) map[string]any {
			if req["type"] != "get_schedule" || req["schedule_name"] != "nightly" || req["load_context"] != true {
				t.Errorf("get wire/defaults: %#v", req)
			}
			return map[string]any{"output": scheduleWireRecord()}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	code, ct, raw := localV2Request(t, ts.URL+localV2ScheduleRoot+"?status=ACTIVE&workflowName=job&scheduleNamePrefix=nightly&loadContext=false", "GET", "")
	if code != 200 || !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("filtered list: status=%d content-type=%s body=%s", code, ct, raw)
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(raw), &rows); err != nil || len(rows) != 1 {
		t.Fatalf("schedule list: %s err=%v", raw, err)
	}
	assertLocalV2Schema(t, "Schedule", rows[0])
	expected := map[string]any{
		"scheduleId": "schedule-id-1", "scheduleName": "nightly", "workflowName": "job",
		"workflowClass": nil, "cronExpression": "0 0 1 1 *", "status": "ACTIVE",
		"context": "opaque-sdk-context", "lastFiredAt": "2026-03-01T01:02:03.456-05:00",
		"automaticBackfill": false, "cronTimezone": nil, "applicationName": nil,
	}
	if !reflect.DeepEqual(rows[0], expected) {
		t.Fatalf("schedule fields: got %#v want %#v", rows[0], expected)
	}

	code, _, raw = localV2Request(t, ts.URL+localV2ScheduleRoot+"/nightly", "GET", "")
	var record map[string]any
	if err := json.Unmarshal([]byte(raw), &record); err != nil || code != 200 {
		t.Fatalf("schedule get: status=%d body=%s err=%v", code, raw, err)
	}
	assertLocalV2Schema(t, "Schedule", record)
	if !reflect.DeepEqual(record, expected) {
		t.Fatalf("get fields: got %#v want %#v", record, expected)
	}

	code, _, raw = localV2Request(t, ts.URL+localV2ScheduleRoot, "GET", "")
	if code != 200 {
		t.Fatalf("default list: status=%d body=%s", code, raw)
	}
	code, _, raw = localV2Request(t, ts.URL+localV2ScheduleRoot+"?status=&workflowName=&scheduleNamePrefix=", "GET", "")
	if code != 200 {
		t.Fatalf("empty filters: status=%d body=%s", code, raw)
	}
	if listCalls.Load() != 3 {
		t.Fatalf("list calls=%d", listCalls.Load())
	}
	_ = fe
}

func TestLocalHTTPV2ScheduleRequiredFieldsNullsAndTypes(t *testing.T) {
	request := func(t *testing.T, row map[string]any) (int, string, map[string]any) {
		t.Helper()
		ts, h := localV2Server(t, true)
		dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]respondFn{
			protocol.MsgGetSchedule: func(map[string]any) map[string]any { return map[string]any{"output": row} },
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		code, _, raw := localV2Request(t, ts.URL+localV2ScheduleRoot+"/nightly", "GET", "")
		var record map[string]any
		if code == 200 {
			if err := json.Unmarshal([]byte(raw), &record); err != nil {
				t.Fatal(err)
			}
		}
		return code, raw, record
	}

	allFields := []string{
		"schedule_id", "schedule_name", "workflow_name", "workflow_class_name", "schedule", "status",
		"context", "last_fired_at", "automatic_backfill", "cron_timezone", "queue_name", "application_name",
	}
	for _, field := range allFields {
		t.Run("missing/"+field, func(t *testing.T) {
			row := scheduleWireRecord()
			delete(row, field)
			code, raw, _ := request(t, row)
			if code != 502 {
				t.Fatalf("missing required SDK key %s: status=%d body=%s", field, code, raw)
			}
		})
	}

	for _, field := range []string{"schedule_id", "schedule_name", "workflow_name", "schedule", "status", "automatic_backfill"} {
		t.Run("nonnull-null/"+field, func(t *testing.T) {
			row := scheduleWireRecord()
			row[field] = nil
			code, raw, _ := request(t, row)
			if code != 502 {
				t.Fatalf("null nonnullable SDK key %s: status=%d body=%s", field, code, raw)
			}
		})
	}

	for _, field := range []struct{ wire, http string }{
		{"workflow_class_name", "workflowClass"}, {"context", "context"}, {"last_fired_at", "lastFiredAt"},
		{"cron_timezone", "cronTimezone"}, {"queue_name", ""}, {"application_name", "applicationName"},
	} {
		t.Run("nullable-null/"+field.wire, func(t *testing.T) {
			row := scheduleWireRecord()
			row[field.wire] = nil
			code, raw, record := request(t, row)
			if code != 200 {
				t.Fatalf("explicit null %s: status=%d body=%s", field.wire, code, raw)
			}
			if field.http != "" {
				value, present := record[field.http]
				if !present || value != nil {
					t.Fatalf("explicit null %s fabricated: %#v", field.http, record)
				}
			}
		})
	}

	for _, tc := range []struct {
		field string
		value any
	}{
		{"schedule_id", false}, {"schedule_name", false}, {"workflow_name", false},
		{"workflow_class_name", false}, {"schedule", false}, {"status", false},
		{"context", false}, {"last_fired_at", false}, {"automatic_backfill", "false"},
		{"cron_timezone", false}, {"queue_name", false}, {"application_name", false},
	} {
		t.Run("invalid-type/"+tc.field, func(t *testing.T) {
			row := scheduleWireRecord()
			row[tc.field] = tc.value
			code, raw, _ := request(t, row)
			if code != 502 {
				t.Fatalf("invalid type %s: status=%d body=%s", tc.field, code, raw)
			}
		})
	}

	t.Run("invalid-date", func(t *testing.T) {
		row := scheduleWireRecord()
		row["last_fired_at"] = "not-a-date"
		code, raw, _ := request(t, row)
		if code != 502 {
			t.Fatalf("invalid date: status=%d body=%s", code, raw)
		}
	})
	t.Run("empty-values-and-false", func(t *testing.T) {
		row := scheduleWireRecord()
		for _, field := range []string{"schedule_id", "schedule_name", "workflow_name", "workflow_class_name", "schedule", "status", "context", "cron_timezone", "queue_name", "application_name"} {
			row[field] = ""
		}
		row["last_fired_at"] = nil
		row["automatic_backfill"] = false
		code, raw, record := request(t, row)
		if code != 200 || record["context"] != "" || record["automaticBackfill"] != false {
			t.Fatalf("empty/false values not preserved: status=%d body=%s", code, raw)
		}
	})
}

func TestLocalHTTPV2ScheduleResponseErrorsAndRetry(t *testing.T) {
	for _, tc := range []struct {
		name, suffix string
		command      protocol.MessageType
		response     map[string]any
		status       int
	}{
		{"empty-list", "", protocol.MsgListSchedules, map[string]any{"output": []any{}}, 200},
		{"null-list", "", protocol.MsgListSchedules, map[string]any{"output": nil}, 502},
		{"missing-list-output", "", protocol.MsgListSchedules, map[string]any{}, 502},
		{"missing-schedule", "/missing", protocol.MsgGetSchedule, map[string]any{"output": nil}, 404},
		{"missing-get-output", "/nightly", protocol.MsgGetSchedule, map[string]any{}, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := localV2Server(t, true)
			dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]respondFn{
				tc.command: func(map[string]any) map[string]any { return tc.response },
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			code, ct, raw := localV2Request(t, ts.URL+localV2ScheduleRoot+tc.suffix, "GET", "")
			if code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", code, tc.status, raw)
			}
			if code >= 400 && !strings.HasPrefix(ct, "application/problem+json") {
				t.Fatalf("not problem JSON: %s", ct)
			}
			if code == 200 && strings.TrimSpace(raw) != "[]" {
				t.Fatalf("empty list encoded as %s", raw)
			}
		})
	}

	t.Run("SDK refusal is final", func(t *testing.T) {
		ts, h := localV2Server(t, true)
		var calls atomic.Int32
		refuse := func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"error_message": "SDK schedule read refused"}
		}
		for _, id := range []string{"one", "two"} {
			dialScheduleFake(t, ts.URL, id, "3.1.0", map[protocol.MessageType]respondFn{protocol.MsgGetSchedule: refuse})
		}
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, _, raw := localV2Request(t, ts.URL+localV2ScheduleRoot+"/nightly", "GET", "")
		if code != 502 || !strings.Contains(raw, "SDK schedule read refused") || calls.Load() != 1 {
			t.Fatalf("refusal bypass: status=%d calls=%d body=%s", code, calls.Load(), raw)
		}
	})

	t.Run("disconnect retries pure read", func(t *testing.T) {
		ts, h := localV2Server(t, true)
		var calls atomic.Int32
		handle := func(map[string]any) map[string]any {
			if calls.Add(1) == 1 {
				return nil
			}
			return map[string]any{"output": scheduleWireRecord()}
		}
		for _, id := range []string{"one", "two"} {
			dialScheduleFake(t, ts.URL, id, "3.1.0", map[protocol.MessageType]respondFn{protocol.MsgGetSchedule: handle})
		}
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, _, raw := localV2Request(t, ts.URL+localV2ScheduleRoot+"/nightly", "GET", "")
		if code != 200 || calls.Load() != 2 {
			t.Fatalf("disconnect retry: status=%d calls=%d body=%s", code, calls.Load(), raw)
		}
	})

	t.Run("disconnect without alternate", func(t *testing.T) {
		ts, h := localV2Server(t, true)
		dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]respondFn{
			protocol.MsgGetSchedule: func(map[string]any) map[string]any { return nil },
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		code, _, raw := localV2Request(t, ts.URL+localV2ScheduleRoot+"/nightly", "GET", "")
		if code != 502 || !strings.Contains(raw, "connection closed") {
			t.Fatalf("disconnect error: status=%d body=%s", code, raw)
		}
	})
}

func TestLocalHTTPV2ScheduleRequestValidationAndCapability(t *testing.T) {
	ts, h := localV2Server(t, true)
	var calls atomic.Int32
	fe := dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgListSchedules: func(map[string]any) map[string]any { calls.Add(1); return map[string]any{"output": []any{}} },
		protocol.MsgGetSchedule: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": scheduleWireRecord()}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	for _, suffix := range []string{
		"?applicationName=fixture-app", "?unknown=1", "?status=ACTIVE&status=PAUSED", "?loadContext=true&loadContext=false",
		"?loadContext=", "?loadContext=0", "?loadContext=TRUE", "?loadContext=null", "?bad=%zz", "/nightly?loadContext=false",
		"?status=%FF", "?workflowName=%FF", "?scheduleNamePrefix=%FF", "?%FF=value",
	} {
		code, ct, raw := localV2Request(t, ts.URL+localV2ScheduleRoot+suffix, "GET", "")
		if code != 400 || !strings.HasPrefix(ct, "application/problem+json") || !strings.Contains(raw, "detail") {
			t.Errorf("invalid query %q: status=%d content-type=%s body=%s", suffix, code, ct, raw)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid request reached executor %d times", calls.Load())
	}

	code, _, raw := localV2Request(t, ts.URL+localV2ScheduleRoot+"?status=%E6%B4%BB%E5%8A%A8&workflowName=%E5%B7%A5%E4%BD%9C%E6%B5%81&scheduleNamePrefix=%E5%A4%9C%E9%97%B4", "GET", "")
	if code != 200 {
		t.Fatalf("valid Unicode query rejected: status=%d body=%s", code, raw)
	}
	wire := fe.body(t, protocol.MsgListSchedules)
	for name, want := range map[string]any{
		"status": []any{"活动"}, "workflow_name": []any{"工作流"}, "schedule_name_prefix": []any{"夜间"},
	} {
		if !reflect.DeepEqual(wire[name], want) {
			t.Fatalf("valid Unicode %s changed: got %#v want %#v", name, wire[name], want)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("valid Unicode request dispatch count=%d", calls.Load())
	}

	code, _, _ = localV2Request(t, ts.URL+strings.Replace(localV2ScheduleRoot, "/local/", "/other/", 1), "GET", "")
	if code != 404 {
		t.Fatalf("unknown org: %d", code)
	}
	code, _, _ = localV2Request(t, ts.URL+strings.Replace(localV2ScheduleRoot, "fixture-app", "absent-app", 1), "GET", "")
	if code != 503 {
		t.Fatalf("absent executor: %d", code)
	}
	disabled, _ := localV2Server(t, false)
	code, _, _ = localV2Request(t, disabled.URL+localV2ScheduleRoot, "GET", "")
	if code != 404 {
		t.Fatalf("default-off adapter: %d", code)
	}

	unsupported, unsupportedHub := localV2Server(t, true)
	var unsupportedCalls atomic.Int32
	dialScheduleFake(t, unsupported.URL, "old", "2.31.1", map[protocol.MessageType]respondFn{
		protocol.MsgListSchedules: func(map[string]any) map[string]any { unsupportedCalls.Add(1); return map[string]any{"output": []any{}} },
	})
	waitFor(t, func() bool { return len(unsupportedHub.Executors()) == 1 })
	code, ct, raw := localV2Request(t, unsupported.URL+localV2ScheduleRoot, "GET", "")
	if code != 502 || !strings.HasPrefix(ct, "application/problem+json") || unsupportedCalls.Load() != 0 {
		t.Fatalf("unsupported SDK did not fail closed: status=%d calls=%d body=%s", code, unsupportedCalls.Load(), raw)
	}
}
