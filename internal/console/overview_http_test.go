package console_test

import (
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

func TestApplicationOverviewReadsSingleExecutorAndScheduleScale(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	var aggregateCalls, scheduleCalls, recentCalls atomic.Int32
	responders := map[protocol.MessageType]testserver.Responder{
		protocol.MsgGetWorkflowAggregates: func(req map[string]any) map[string]any {
			aggregateCalls.Add(1)
			body := req["body"].(map[string]any)
			if body["start_time"] == nil {
				return map[string]any{"output": []any{map[string]any{"group": map[string]any{"status": "PENDING", "queue_name": "jobs"}, "count": 13}, map[string]any{"group": map[string]any{"status": "ENQUEUED", "queue_name": "jobs"}, "count": 4}}}
			}
			start, _ := time.Parse(time.RFC3339Nano, body["start_time"].(string))
			bucket := int64(body["time_bucket_size_ms"].(float64))
			return map[string]any{"output": []any{map[string]any{"group": map[string]any{"status": "ERROR", "time_bucket": strconv.FormatInt(start.UnixMilli()/bucket*bucket, 10)}, "count": 7}}}
		},
		protocol.MsgListWorkflows: func(map[string]any) map[string]any {
			recentCalls.Add(1)
			return map[string]any{"output": []any{map[string]any{"WorkflowUUID": "recent-run", "Status": "SUCCESS", "WorkflowName": "checkout", "CreatedAt": "2026-10-01T00:00:00Z"}}}
		},
		protocol.MsgListSchedules: func(map[string]any) map[string]any {
			scheduleCalls.Add(1)
			records := make([]any, 3000)
			for i := range records {
				records[i] = map[string]any{"schedule_id": fmt.Sprint(i), "schedule_name": fmt.Sprint(i), "status": "ACTIVE"}
			}
			return map[string]any{"output": records}
		},
	}
	first := testserver.Connect(t, ts, "fixture-app", "testkey", "one", responders)
	second := testserver.Connect(t, ts, "fixture-app", "testkey", "two", responders)
	testserver.Wait(t, func() bool { return len(h.Executors()) == 2 })
	end := time.Date(2026, 10, 1, 12, 34, 0, 0, time.UTC)
	q := url.Values{"range": {"24h"}, "start_time": {end.Add(-24 * time.Hour).Format("2006-01-02T15:04:05.000Z")}, "end_time": {end.Format("2006-01-02T15:04:05.000Z")}}
	for _, panel := range []string{"activity", "workload", "recent", "schedules"} {
		for i := 0; i < 2; i++ {
			code, body := testserver.Get(t, ts.URL+"/apps/fixture-app/overview/"+panel+"?"+q.Encode())
			if code != 200 || !strings.Contains(body, `data-loaded="true"`) || strings.Contains(body, "template error") {
				t.Fatal(panel, code, body)
			}
			if panel == "schedules" && !strings.Contains(body, "<strong>3000</strong>") {
				t.Fatal("schedule count wrong")
			}
			if panel == "recent" && !strings.Contains(body, "recent-run") {
				t.Fatal("recent link missing")
			}
		}
	}
	if aggregateCalls.Load() != 2 || scheduleCalls.Load() != 1 || recentCalls.Load() != 1 {
		t.Fatal("duplicate database-wide reads", aggregateCalls.Load(), scheduleCalls.Load(), recentCalls.Load())
	}
	var aggregate, recent, schedules map[string]any
	for _, fake := range []*testserver.Executor{first, second} {
		for _, req := range fake.Requests() {
			switch req["type"] {
			case string(protocol.MsgGetWorkflowAggregates):
				if body := req["body"].(map[string]any); body["start_time"] == nil {
					aggregate = body
				}
			case string(protocol.MsgListWorkflows):
				recent = req["body"].(map[string]any)
			case string(protocol.MsgListSchedules):
				schedules = req["body"].(map[string]any)
			}
		}
	}
	if aggregate["start_time"] != nil || !reflect.DeepEqual(aggregate["status"], []any{"PENDING", "ENQUEUED", "DELAYED"}) || aggregate["group_by_queue_name"] != true {
		t.Fatal("workload query", aggregate)
	}
	if recent["limit"] != float64(10) || recent["load_input"] != false || recent["load_output"] != false || recent["start_time"] != q.Get("start_time") || recent["end_time"] != q.Get("end_time") {
		t.Fatal("recent query", recent)
	}
	if schedules["load_context"] != false || !reflect.DeepEqual(schedules["status"], []any{"ACTIVE"}) {
		t.Fatal("schedule query", schedules)
	}
	for _, body := range []map[string]any{aggregate, recent, schedules} {
		if !reflect.DeepEqual(body["application_name"], []any{"fixture-app"}) {
			t.Fatal("app scope missing", body)
		}
	}
}

func TestApplicationOverviewIndependentErrorsAndAppAvailability(t *testing.T) {
	ts, h := testserver.New(t, config.Config{})
	var calls atomic.Int32
	testserver.Connect(t, ts, "fixture-app", "testkey", "one", map[protocol.MessageType]testserver.Responder{protocol.MsgGetWorkflowAggregates: func(map[string]any) map[string]any {
		calls.Add(1)
		return map[string]any{"error_message": "aggregate unsupported"}
	}, protocol.MsgListWorkflows: func(map[string]any) map[string]any {
		return map[string]any{"output": []any{map[string]any{"WorkflowUUID": "still-visible"}}}
	}})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	code, body := testserver.Get(t, ts.URL+"/apps/fixture-app/overview/activity")
	if code != 200 || !strings.Contains(body, "aggregate unsupported") || strings.Contains(body, `data-loaded="true"`) {
		t.Fatal(code, body)
	}
	code, body = testserver.Get(t, ts.URL+"/apps/fixture-app/overview/recent")
	if code != 200 || !strings.Contains(body, "still-visible") {
		t.Fatal(code, body)
	}
	code, body = testserver.Get(t, ts.URL+"/apps/other-app/overview/activity")
	if code != 200 || !strings.Contains(body, "unavailable") || calls.Load() != 1 {
		t.Fatal("other app made unavailable app ready", code, body, calls.Load())
	}
}
