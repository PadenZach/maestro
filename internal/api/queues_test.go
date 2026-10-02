package api_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

const queueRoot = "/v2/orgs/local/apps/fixture-app/queues"

// Independent values from the pinned Queue schema and Python 3.1.0
// QueueOutput / LIST_QUEUES / GET_QUEUE definitions and handlers.
func queueWireRecord() map[string]any {
	return map[string]any{"name": "jobs", "concurrency": 3, "worker_concurrency": nil,
		"rate_limit_max": 5, "rate_limit_period_sec": 1.5, "priority_enabled": false,
		"partition_queue": false, "polling_interval_sec": 0.25, "application_name": "fixture-app",
		"partition_concurrency": 2, "partition_worker_concurrency": 1,
		"partition_rate_limit_max": 4, "partition_rate_limit_period_sec": 2.5}
}

func TestAPIQueueSchemaAndWire(t *testing.T) {
	ts, h := testserver.New(t, config.Config{EnableAggregates: true})
	var calls atomic.Int32
	fe := testserver.Connect(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]testserver.Responder{
		protocol.MsgListQueues: func(req map[string]any) map[string]any {
			calls.Add(1)
			if _, ok := req["body"]; ok {
				t.Error("queue list invented a filter")
			}
			return map[string]any{"output": []any{queueWireRecord()}}
		},
		protocol.MsgGetQueue: func(req map[string]any) map[string]any {
			calls.Add(1)
			if req["name"] != "jobs" {
				t.Errorf("queue name not forwarded: %v", req["name"])
			}
			return map[string]any{"output": queueWireRecord()}
		},
	})
	_ = fe
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	expected := map[string]any{"name": "jobs", "concurrency": float64(3), "workerConcurrency": nil,
		"rateLimitMax": float64(5), "rateLimitPeriodSecs": 1.5, "priorityEnabled": false,
		"partitionQueue": false, "pollingIntervalSecs": 0.25, "applicationName": "fixture-app",
		"partitionConcurrency": float64(2), "partitionWorkerConcurrency": float64(1),
		"partitionRateLimitMax": float64(4), "partitionRateLimitPeriodSecs": 2.5}
	for _, suffix := range []string{"", "/jobs"} {
		code, ct, raw := testserver.Request(t, ts.URL+queueRoot+suffix, "GET", "")
		if code != 200 || !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("queue read %q: status=%d body=%s", suffix, code, raw)
		}
		var record map[string]any
		if suffix == "" {
			var rows []map[string]any
			if err := json.Unmarshal([]byte(raw), &rows); err != nil || len(rows) != 1 {
				t.Fatalf("list: %s %v", raw, err)
			}
			record = rows[0]
		} else if err := json.Unmarshal([]byte(raw), &record); err != nil {
			t.Fatal(err)
		}
		assertLocalV2Schema(t, "Queue", record)
		if !reflect.DeepEqual(record, expected) {
			t.Fatalf("queue fields: got %#v want %#v", record, expected)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("unexpected read count %d", calls.Load())
	}
}

func TestAPIQueueErrorsAndNulls(t *testing.T) {
	for _, tc := range []struct {
		name, suffix string
		output       map[string]any
		status       int
	}{
		{"empty list", "", map[string]any{"output": []any{}}, 200},
		{"null list", "", map[string]any{"output": nil}, 502},
		{"missing list", "", map[string]any{}, 502},
		{"missing queue", "/jobs", map[string]any{"output": nil}, 404},
		{"missing payload", "/jobs", map[string]any{}, 502},
		{"refusal", "/jobs", map[string]any{"error_message": "metadata only"}, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := testserver.New(t, config.Config{EnableAggregates: true})
			var calls atomic.Int32
			handler := func(map[string]any) map[string]any { calls.Add(1); return tc.output }
			for _, id := range []string{"one", "two"} {
				testserver.Connect(t, ts, "fixture-app", "testkey", id, map[protocol.MessageType]testserver.Responder{protocol.MsgListQueues: handler, protocol.MsgGetQueue: handler})
			}
			testserver.Wait(t, func() bool { return len(h.Executors()) == 2 })
			code, ct, raw := testserver.Request(t, ts.URL+queueRoot+tc.suffix, "GET", "")
			if code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", code, tc.status, raw)
			}
			if code >= 400 && !strings.HasPrefix(ct, "application/problem+json") {
				t.Fatalf("not problem JSON: %s", ct)
			}
			if code == 200 && strings.TrimSpace(raw) != "[]" {
				t.Fatalf("not empty array: %s", raw)
			}
			if calls.Load() != 1 {
				t.Fatalf("refusal/read retried: %d", calls.Load())
			}
		})
	}
	for _, field := range []string{"name", "priority_enabled", "partition_queue", "polling_interval_sec"} {
		for _, missing := range []bool{false, true} {
			t.Run(field+map[bool]string{true: "/missing", false: "/null"}[missing], func(t *testing.T) {
				row := queueWireRecord()
				if missing {
					delete(row, field)
				} else {
					row[field] = nil
				}
				ts, h := testserver.New(t, config.Config{EnableAggregates: true})
				testserver.Connect(t, ts, "fixture-app", "testkey", "one", map[protocol.MessageType]testserver.Responder{protocol.MsgGetQueue: func(map[string]any) map[string]any { return map[string]any{"output": row} }})
				testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
				code, _, raw := testserver.Request(t, ts.URL+queueRoot+"/jobs", "GET", "")
				if code != 502 {
					t.Fatalf("fabricated %s: %d %s", field, code, raw)
				}
			})
		}
	}
	for _, field := range []string{"concurrency", "worker_concurrency", "rate_limit_max", "partition_concurrency", "partition_worker_concurrency", "partition_rate_limit_max"} {
		t.Run(field+"/overflow", func(t *testing.T) {
			row := queueWireRecord()
			row[field] = int64(2147483648)
			ts, h := testserver.New(t, config.Config{EnableAggregates: true})
			testserver.Connect(t, ts, "fixture-app", "testkey", "one", map[protocol.MessageType]testserver.Responder{protocol.MsgGetQueue: func(map[string]any) map[string]any { return map[string]any{"output": row} }})
			testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
			code, _, raw := testserver.Request(t, ts.URL+queueRoot+"/jobs", "GET", "")
			if code != 502 {
				t.Fatalf("int32 overflow: %d %s", code, raw)
			}
		})
	}
}

func TestAPIQueueNullablePresenceAndZero(t *testing.T) {
	fields := []struct {
		wire, http string
		numeric    bool
	}{
		{"concurrency", "concurrency", true},
		{"worker_concurrency", "workerConcurrency", true},
		{"rate_limit_max", "rateLimitMax", true},
		{"rate_limit_period_sec", "rateLimitPeriodSecs", true},
		{"application_name", "applicationName", false},
		{"partition_concurrency", "partitionConcurrency", true},
		{"partition_worker_concurrency", "partitionWorkerConcurrency", true},
		{"partition_rate_limit_max", "partitionRateLimitMax", true},
		{"partition_rate_limit_period_sec", "partitionRateLimitPeriodSecs", true},
	}
	request := func(t *testing.T, row map[string]any) (int, string, map[string]any) {
		t.Helper()
		ts, h := testserver.New(t, config.Config{EnableAggregates: true})
		testserver.Connect(t, ts, "fixture-app", "testkey", "one", map[protocol.MessageType]testserver.Responder{
			protocol.MsgGetQueue: func(map[string]any) map[string]any { return map[string]any{"output": row} },
		})
		testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
		code, _, raw := testserver.Request(t, ts.URL+queueRoot+"/jobs", "GET", "")
		var record map[string]any
		if code == 200 {
			if err := json.Unmarshal([]byte(raw), &record); err != nil {
				t.Fatal(err)
			}
		}
		return code, raw, record
	}
	for _, field := range fields {
		t.Run(field.wire+"/missing", func(t *testing.T) {
			row := queueWireRecord()
			delete(row, field.wire)
			code, raw, _ := request(t, row)
			if code != 502 {
				t.Fatalf("missing required nullable %s: %d %s", field.wire, code, raw)
			}
		})
		t.Run(field.wire+"/null", func(t *testing.T) {
			row := queueWireRecord()
			row[field.wire] = nil
			code, raw, record := request(t, row)
			if code != 200 {
				t.Fatalf("explicit null %s: %d %s", field.wire, code, raw)
			}
			value, present := record[field.http]
			if !present || value != nil {
				t.Fatalf("explicit null %s fabricated: %#v", field.http, record)
			}
		})
		if field.numeric {
			t.Run(field.wire+"/zero", func(t *testing.T) {
				row := queueWireRecord()
				row[field.wire] = 0
				code, raw, record := request(t, row)
				if code != 200 || record[field.http] != float64(0) {
					t.Fatalf("explicit zero %s: %d %s", field.wire, code, raw)
				}
			})
		}
	}
	t.Run("polling_interval_sec/zero", func(t *testing.T) {
		row := queueWireRecord()
		row["polling_interval_sec"] = 0
		code, raw, record := request(t, row)
		if code != 200 || record["pollingIntervalSecs"] != float64(0) {
			t.Fatalf("explicit zero polling_interval_sec: %d %s", code, raw)
		}
	})
}

func TestAPIQueueRequestValidation(t *testing.T) {
	ts, h := testserver.New(t, config.Config{EnableAggregates: true})
	var calls atomic.Int32
	testserver.Connect(t, ts, "fixture-app", "testkey", "one", map[protocol.MessageType]testserver.Responder{protocol.MsgListQueues: func(map[string]any) map[string]any { calls.Add(1); return map[string]any{"output": []any{}} }, protocol.MsgGetQueue: func(map[string]any) map[string]any { calls.Add(1); return map[string]any{"output": queueWireRecord()} }})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	for _, suffix := range []string{"?limit=1", "?applicationName=other", "?bad=%zz", "/jobs?limit=1"} {
		code, _, raw := testserver.Request(t, ts.URL+queueRoot+suffix, "GET", "")
		if code != 400 {
			t.Errorf("query %s: %d %s", suffix, code, raw)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request reached executor")
	}
	code, _, _ := testserver.Request(t, ts.URL+strings.Replace(queueRoot, "/local/", "/other/", 1), "GET", "")
	if code != 404 {
		t.Fatalf("unknown org: %d", code)
	}
	code, _, _ = testserver.Request(t, ts.URL+strings.Replace(queueRoot, "fixture-app", "absent-app", 1), "GET", "")
	if code != 503 {
		t.Fatalf("absent executor: %d", code)
	}
	unavailable, _ := testserver.New(t, config.Config{EnableAggregates: true})
	code, _, _ = testserver.Request(t, unavailable.URL+queueRoot, "GET", "")
	if code != 503 {
		t.Fatalf("unavailable adapter: %d", code)
	}
}
