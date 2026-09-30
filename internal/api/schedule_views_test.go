package api_test

import (
	"context"
	"encoding/json"
	"html"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/zpaden/maestro/internal/protocol"
)

// scheduleConsoleWireRecord uses the reviewed Python 3.1.0 ScheduleOutput wire
// keys. The Console field inventory is checked independently against OpenAPI.
func scheduleConsoleWireRecord(name string) map[string]any {
	return map[string]any{
		"schedule_id":         "schedule-id-1",
		"schedule_name":       name,
		"workflow_name":       "job",
		"workflow_class_name": "Jobs",
		"schedule":            "0 0 1 1 *",
		"status":              "ACTIVE",
		"context":             `opaque <script>alert("context")</script> & data`,
		"last_fired_at":       "2026-03-01T01:02:03.456-05:00",
		"automatic_backfill":  false,
		"cron_timezone":       "America/New_York",
		"queue_name":          "jobs",
		"application_name":    "fixture-app",
	}
}

func pinnedScheduleFields(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile("../../docs/reference/conductor-openapi-2026-09-25.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Components struct {
			Schemas map[string]struct {
				Required []string `json:"required"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(b, &spec); err != nil {
		t.Fatal(err)
	}
	fields := spec.Components.Schemas["Schedule"].Required
	if len(fields) != 11 {
		t.Fatalf("pinned Schedule required fields = %d, want 11: %v", len(fields), fields)
	}
	return fields
}

func scheduleFieldText(t *testing.T, body, field string) string {
	t.Helper()
	re := regexp.MustCompile(`(?s)<td[^>]*data-field="` + regexp.QuoteMeta(field) + `"[^>]*>\s*([^<]*?)\s*</td>`)
	match := re.FindStringSubmatch(body)
	if match == nil {
		t.Fatalf("schedule detail missing field %q: %s", field, body)
	}
	return strings.TrimSpace(html.UnescapeString(match[1]))
}

func extractScheduleHrefs(t *testing.T, body string) []string {
	t.Helper()
	re := regexp.MustCompile(`<a class="link schedule-detail-link" href="([^"]+)"`)
	matches := re.FindAllStringSubmatch(body, -1)
	hrefs := make([]string, 0, len(matches))
	for _, match := range matches {
		hrefs = append(hrefs, html.UnescapeString(match[1]))
	}
	return hrefs
}

// dialScheduleConsoleFake advertises the exact reviewed schedule capability.
func dialScheduleConsoleFake(t *testing.T, tsURL, app, execID, language, sdkVersion string, handlers map[protocol.MessageType]respondFn) *fakeExec {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	endpoint := "ws" + strings.TrimPrefix(tsURL, "http") + "/websocket/" + url.PathEscape(app) + "/testkey"
	c, _, err := websocket.Dial(ctx, endpoint, nil)
	if err != nil {
		t.Fatalf("dial schedule executor: %v", err)
	}
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read executor_info request: %v", err)
	}
	envelope, err := protocol.DecodeEnvelope(data)
	if err != nil || envelope.Type != protocol.MsgExecutorInfo {
		t.Fatalf("executor_info request: envelope=%+v err=%v", envelope, err)
	}
	out, err := json.Marshal(protocol.ExecutorInfoResponse{
		Type: protocol.MsgExecutorInfo, RequestID: envelope.RequestID, ExecutorID: execID,
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

func TestScheduleConsoleDetailSchemaAndReadOnlyWire(t *testing.T) {
	ts, h := newTestServer(t)
	var calls atomic.Int32
	fe := dialScheduleConsoleFake(t, ts.URL, "fixture-app", "schedule-exec", "python", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgGetSchedule: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": scheduleConsoleWireRecord("nightly")}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	code, body := getBody(t, ts.URL+"/apps/fixture-app/schedule?name=nightly")
	if code != http.StatusOK {
		t.Fatalf("schedule detail status = %d, want 200: %s", code, body)
	}
	for _, want := range []string{
		`<div class="brand">maestro</div>`,
		`class="status-dot status-green"`,
		`<a href="/">Home</a>`,
		`<a href="/apps/fixture-app">fixture-app</a>`,
		`<a href="/apps/fixture-app/schedules">Schedules</a>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("schedule detail missing %q: %s", want, body)
		}
	}

	want := map[string]string{
		"scheduleId":        "schedule-id-1",
		"scheduleName":      "nightly",
		"workflowName":      "job",
		"workflowClass":     "Jobs",
		"cronExpression":    "0 0 1 1 *",
		"status":            "ACTIVE",
		"context":           `opaque <script>alert("context")</script> & data`,
		"lastFiredAt":       "2026-03-01T01:02:03.456-05:00",
		"automaticBackfill": "No",
		"cronTimezone":      "America/New_York",
		"applicationName":   "fixture-app",
	}
	for _, field := range pinnedScheduleFields(t) {
		if got := scheduleFieldText(t, body, field); got != want[field] {
			t.Errorf("field %s = %q, want %q", field, got, want[field])
		}
	}
	if got := scheduleFieldText(t, body, "queueName"); got != "jobs" {
		t.Errorf("SDK queueName = %q, want jobs", got)
	}
	if got := strings.Count(body, `data-field="`); got != 12 {
		t.Errorf("rendered schedule field count = %d, want 11 official fields plus queueName", got)
	}
	if strings.Contains(body, `<script>alert("context")</script>`) || !strings.Contains(body, `&lt;script&gt;alert(&#34;context&#34;)&lt;/script&gt; &amp; data`) {
		t.Fatalf("opaque context was not safely escaped: %s", body)
	}

	wire := fe.body(t, protocol.MsgGetSchedule)
	if wire["type"] != "get_schedule" || wire["schedule_name"] != "nightly" || wire["load_context"] != true {
		t.Fatalf("unexpected schedule detail wire request: %#v", wire)
	}
	if len(wire) != 4 {
		t.Fatalf("GET_SCHEDULE request grew unexpected fields: %#v", wire)
	}

	postCode, _ := postBody(t, ts.URL+"/apps/fixture-app/schedule?name=nightly")
	if postCode != http.StatusMethodNotAllowed {
		t.Fatalf("schedule detail POST status = %d, want 405", postCode)
	}
	if calls.Load() != 1 {
		t.Fatalf("non-GET dispatched to executor; calls = %d", calls.Load())
	}
}

func TestScheduleConsoleDetailPreservesNilEmptyAndFalse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
		want   string
	}{
		{
			name: "nil",
			mutate: func(row map[string]any) {
				for _, field := range []string{"workflow_class_name", "context", "last_fired_at", "cron_timezone", "queue_name", "application_name"} {
					row[field] = nil
				}
			},
			want: "Unavailable",
		},
		{
			name: "empty",
			mutate: func(row map[string]any) {
				for _, field := range []string{"workflow_class_name", "context", "cron_timezone", "queue_name", "application_name"} {
					row[field] = ""
				}
				row["last_fired_at"] = nil
			},
			want: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := scheduleConsoleWireRecord("nightly")
			tc.mutate(row)
			ts, h := newTestServer(t)
			dialScheduleConsoleFake(t, ts.URL, "fixture-app", "schedule-exec", "python", "3.1.0", map[protocol.MessageType]respondFn{
				protocol.MsgGetSchedule: func(map[string]any) map[string]any { return map[string]any{"output": row} },
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })

			code, body := getBody(t, ts.URL+"/apps/fixture-app/schedule?name=nightly")
			if code != http.StatusOK {
				t.Fatalf("schedule detail status = %d: %s", code, body)
			}
			for _, field := range []string{"workflowClass", "context", "cronTimezone", "queueName", "applicationName"} {
				if got := scheduleFieldText(t, body, field); got != tc.want {
					t.Errorf("%s %s = %q, want %q", tc.name, field, got, tc.want)
				}
			}
			if got := scheduleFieldText(t, body, "lastFiredAt"); got != "Unavailable" {
				t.Errorf("nil lastFiredAt = %q, want Unavailable", got)
			}
			if got := scheduleFieldText(t, body, "automaticBackfill"); got != "No" {
				t.Errorf("explicit false automaticBackfill = %q, want No", got)
			}
		})
	}
}

func TestScheduleConsoleListFiltersDefaultsAndEmptyResult(t *testing.T) {
	ts, h := newTestServer(t)
	var calls atomic.Int32
	dialScheduleConsoleFake(t, ts.URL, "fixture-app", "schedule-exec", "python", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgListSchedules: func(req map[string]any) map[string]any {
			body, _ := req["body"].(map[string]any)
			switch calls.Add(1) {
			case 1:
				want := map[string]any{
					"status": []any{"ACTIVE"}, "workflow_name": []any{"flow name"},
					"schedule_name_prefix": []any{"night/"}, "load_context": false,
				}
				if !reflect.DeepEqual(body, want) {
					t.Errorf("filtered list wire = %#v, want %#v", body, want)
				}
				return map[string]any{"output": []any{scheduleConsoleWireRecord("nightly")}}
			case 2, 3:
				if !reflect.DeepEqual(body, map[string]any{"load_context": false}) {
					t.Errorf("blank Console controls must mean All with context suppressed: %#v", body)
				}
				return map[string]any{"output": []any{}}
			default:
				t.Errorf("unexpected list call")
				return map[string]any{"output": []any{}}
			}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	query := url.Values{
		"status": {"ACTIVE"}, "workflowName": {"flow name"}, "scheduleNamePrefix": {"night/"},
	}.Encode()
	code, body := getBody(t, ts.URL+"/apps/fixture-app/schedules?"+query)
	if code != http.StatusOK {
		t.Fatalf("filtered schedule list status = %d: %s", code, body)
	}
	for _, want := range []string{
		`name="status" value="ACTIVE"`,
		`name="workflowName" value="flow name"`,
		`name="scheduleNamePrefix" value="night/"`,
		`class="link schedule-detail-link" href="/apps/fixture-app/schedule?name=nightly"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("filtered list did not preserve %q: %s", want, body)
		}
	}

	code, body = getBody(t, ts.URL+"/apps/fixture-app/schedules")
	if code != http.StatusOK || !strings.Contains(body, "No schedules match these filters.") {
		t.Fatalf("empty schedule list not visible: status=%d body=%s", code, body)
	}
	code, body = getBody(t, ts.URL+"/apps/fixture-app/schedules?status=&workflowName=&scheduleNamePrefix=")
	if code != http.StatusOK || !strings.Contains(body, "No schedules match these filters.") {
		t.Fatalf("blank All controls failed: status=%d body=%s", code, body)
	}
	if calls.Load() != 3 {
		t.Fatalf("LIST_SCHEDULES calls = %d, want 3", calls.Load())
	}
}

func TestScheduleConsoleNamesRoundTripThroughQueryLinks(t *testing.T) {
	const app = `team/alpha & beta`
	names := []string{".", "..", "ops/blue", `hostile ?x=<b>unsafe</b>&y=%#`}
	ts, h := newTestServer(t)
	var mu sync.Mutex
	var gotNames []string
	dialScheduleConsoleFake(t, ts.URL, app, "schedule-exec", "python", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgListSchedules: func(map[string]any) map[string]any {
			rows := make([]any, 0, len(names))
			for _, name := range names {
				rows = append(rows, scheduleConsoleWireRecord(name))
			}
			return map[string]any{"output": rows}
		},
		protocol.MsgGetSchedule: func(req map[string]any) map[string]any {
			name, _ := req["schedule_name"].(string)
			mu.Lock()
			gotNames = append(gotNames, name)
			mu.Unlock()
			return map[string]any{"output": scheduleConsoleWireRecord(name)}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	appPath := "/apps/" + url.PathEscape(app)
	code, list := getBodyNoRedirect(t, ts.URL+appPath+"/schedules")
	if code != http.StatusOK {
		t.Fatalf("schedule list status = %d: %s", code, list)
	}
	if strings.Contains(list, "<b>unsafe</b>") || !strings.Contains(list, "&lt;b&gt;unsafe&lt;/b&gt;") {
		t.Fatalf("hostile schedule name was not HTML escaped: %s", list)
	}
	hrefs := extractScheduleHrefs(t, list)
	if len(hrefs) != len(names) {
		t.Fatalf("schedule detail links = %d, want %d: %s", len(hrefs), len(names), list)
	}
	for i, href := range hrefs {
		parsed, err := url.Parse(href)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.EscapedPath() != appPath+"/schedule" || len(parsed.Query()) != 1 || parsed.Query().Get("name") != names[i] {
			t.Fatalf("schedule link did not preserve app/name: %q", href)
		}
		code, detail := getBodyNoRedirect(t, ts.URL+href)
		if code != http.StatusOK {
			t.Fatalf("schedule link %q status = %d without redirect: %s", href, code, detail)
		}
		if strings.Contains(detail, "<b>unsafe</b>") {
			t.Fatalf("hostile schedule name was not escaped in detail or breadcrumbs: %s", detail)
		}
		if got := scheduleFieldText(t, detail, "scheduleName"); got != names[i] {
			t.Fatalf("rendered schedule name = %q, want %q", got, names[i])
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(gotNames, names) {
		t.Fatalf("schedule names dispatched = %#v, want %#v", gotNames, names)
	}
}

func TestScheduleConsoleRejectsInvalidQueriesBeforeDispatch(t *testing.T) {
	ts, h := newTestServer(t)
	var calls atomic.Int32
	dialScheduleConsoleFake(t, ts.URL, "fixture-app", "schedule-exec", "python", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgListSchedules: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": []any{}}
		},
		protocol.MsgGetSchedule: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": scheduleConsoleWireRecord("unexpected")}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	for _, query := range []string{
		"?unknown=1", "?loadContext=true", "?status=ACTIVE&status=PAUSED", "?workflowName=a&workflowName=b",
		"?bad=%zz", "?status=%FF", "?scheduleNamePrefix=x;y", "?%FF=value",
	} {
		code, body := getBodyNoRedirect(t, ts.URL+"/apps/fixture-app/schedules"+query)
		if code != http.StatusBadRequest || !strings.Contains(body, "schedule list query") {
			t.Errorf("invalid list query %q: status=%d body=%s", query, code, body)
		}
	}
	for _, query := range []string{
		"", "?name=", "?name=nightly&name=other", "?name=nightly&extra=1",
		"?bad=%zz", "?name=%FF", "?name=nightly;bad",
	} {
		code, body := getBodyNoRedirect(t, ts.URL+"/apps/fixture-app/schedule"+query)
		if code != http.StatusBadRequest || !strings.Contains(body, "exactly one nonempty name") {
			t.Errorf("invalid detail query %q: status=%d body=%s", query, code, body)
		}
	}
	for _, path := range []string{"/apps/fixture-app/schedules", "/apps/fixture-app/schedule?name=nightly"} {
		code, _ := postBody(t, ts.URL+path)
		if code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s status = %d, want 405", path, code)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid or non-GET schedule requests dispatched %d executor calls", calls.Load())
	}
}

func TestScheduleConsoleErrorsRemainExplicit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    string
		command protocol.MessageType
		reply   func() map[string]any
		status  int
		text    string
	}{
		{
			name: "missing schedule", path: "/apps/fixture-app/schedule?name=absent", command: protocol.MsgGetSchedule,
			reply: func() map[string]any { return map[string]any{"output": nil} }, status: 404, text: `schedule &#34;absent&#34; not found`,
		},
		{
			name: "missing get output", path: "/apps/fixture-app/schedule?name=nightly", command: protocol.MsgGetSchedule,
			reply: func() map[string]any { return map[string]any{} }, status: 502, text: "executor response data unavailable",
		},
		{
			name: "missing list output", path: "/apps/fixture-app/schedules", command: protocol.MsgListSchedules,
			reply: func() map[string]any { return map[string]any{} }, status: 502, text: "executor response data unavailable",
		},
		{
			name: "null list output", path: "/apps/fixture-app/schedules", command: protocol.MsgListSchedules,
			reply: func() map[string]any { return map[string]any{"output": nil} }, status: 502, text: "schedule list unavailable",
		},
		{
			name: "invalid detail date", path: "/apps/fixture-app/schedule?name=nightly", command: protocol.MsgGetSchedule,
			reply: func() map[string]any {
				row := scheduleConsoleWireRecord("nightly")
				row["last_fired_at"] = "not-a-date"
				return map[string]any{"output": row}
			}, status: 502, text: "last_fired_at must be RFC3339",
		},
		{
			name: "missing list item field", path: "/apps/fixture-app/schedules", command: protocol.MsgListSchedules,
			reply: func() map[string]any {
				row := scheduleConsoleWireRecord("nightly")
				delete(row, "context")
				return map[string]any{"output": []any{row}}
			}, status: 502, text: "missing required SDK fields",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := newTestServer(t)
			dialScheduleConsoleFake(t, ts.URL, "fixture-app", "schedule-exec", "python", "3.1.0", map[protocol.MessageType]respondFn{
				tc.command: func(map[string]any) map[string]any { return tc.reply() },
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			code, body := getBodyNoRedirect(t, ts.URL+tc.path)
			if code != tc.status || !strings.Contains(body, tc.text) {
				t.Fatalf("explicit schedule error: status=%d want=%d body=%s", code, tc.status, body)
			}
		})
	}

	t.Run("unavailable", func(t *testing.T) {
		ts, _ := newTestServer(t)
		code, body := getBody(t, ts.URL+"/apps/fixture-app/schedules")
		if code != http.StatusServiceUnavailable || !strings.Contains(body, "maestro: application unavailable") {
			t.Fatalf("unavailable schedule list: status=%d body=%s", code, body)
		}
	})

	t.Run("disconnect", func(t *testing.T) {
		ts, h := newTestServer(t)
		var calls atomic.Int32
		dialScheduleConsoleFake(t, ts.URL, "fixture-app", "schedule-exec", "python", "3.1.0", map[protocol.MessageType]respondFn{
			protocol.MsgGetSchedule: func(map[string]any) map[string]any { calls.Add(1); return nil },
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		code, body := getBody(t, ts.URL+"/apps/fixture-app/schedule?name=nightly")
		if code != http.StatusBadGateway || calls.Load() != 1 || !strings.Contains(body, "maestro: executor connection closed") {
			t.Fatalf("schedule disconnect: status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})

	t.Run("refusal is final", func(t *testing.T) {
		ts, h := newTestServer(t)
		var calls atomic.Int32
		refuse := func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"error_message": "schedule <private> & refused"}
		}
		for _, id := range []string{"one", "two"} {
			dialScheduleConsoleFake(t, ts.URL, "fixture-app", id, "python", "3.1.0", map[protocol.MessageType]respondFn{protocol.MsgGetSchedule: refuse})
		}
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, body := getBody(t, ts.URL+"/apps/fixture-app/schedule?name=nightly")
		if code != http.StatusBadGateway || calls.Load() != 1 || !strings.Contains(body, "schedule &lt;private&gt; &amp; refused") || strings.Contains(body, "schedule <private>") {
			t.Fatalf("schedule refusal bypass/escaping: status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})
}

func TestScheduleConsoleCapabilityFailsClosedWithoutDispatch(t *testing.T) {
	for _, tc := range []struct{ language, version string }{
		{"python", "2.31.1"},
		{"typescript", "3.1.0"},
	} {
		t.Run(tc.language+"/"+tc.version, func(t *testing.T) {
			ts, h := newTestServer(t)
			var calls atomic.Int32
			dialScheduleConsoleFake(t, ts.URL, "fixture-app", "unsupported", tc.language, tc.version, map[protocol.MessageType]respondFn{
				protocol.MsgListSchedules: func(map[string]any) map[string]any {
					calls.Add(1)
					return map[string]any{"output": []any{}}
				},
				protocol.MsgGetSchedule: func(map[string]any) map[string]any {
					calls.Add(1)
					return map[string]any{"output": scheduleConsoleWireRecord("nightly")}
				},
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			for _, path := range []string{"/apps/fixture-app/schedules", "/apps/fixture-app/schedule?name=nightly"} {
				code, body := getBody(t, ts.URL+path)
				if code != http.StatusBadGateway || !strings.Contains(body, "maestro: unsupported executor capability") {
					t.Errorf("unsupported schedule read %s: status=%d body=%s", path, code, body)
				}
			}
			if calls.Load() != 0 {
				t.Fatalf("unsupported schedule commands dispatched %d times", calls.Load())
			}
		})
	}
}

func TestApplicationLandingLinksToSchedules(t *testing.T) {
	ts, _ := newTestServer(t)
	code, body := getBody(t, ts.URL+"/apps/team%2Falpha%20&%20beta")
	if code != http.StatusOK {
		t.Fatalf("application landing status = %d: %s", code, body)
	}
	match := regexp.MustCompile(`<a class="app-card application-schedules-link" href="([^"]+)"`).FindStringSubmatch(body)
	if match == nil {
		t.Fatalf("application landing missing Schedules card: %s", body)
	}
	if got, want := html.UnescapeString(match[1]), "/apps/team%2Falpha%20&%20beta/schedules"; got != want {
		t.Fatalf("Schedules card href = %q, want %q", got, want)
	}
	if !strings.Contains(body, "Browse registered schedules and their configuration.") {
		t.Fatalf("Schedules card lacks bounded read-only description: %s", body)
	}
}
