package api_test

import (
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zpaden/maestro/internal/protocol"
)

// queueConsoleWireRecord uses the reviewed Python 3.1.0 QueueOutput wire keys.
// The Console field inventory is checked separately against the generated OpenAPI.
func queueConsoleWireRecord(name string) map[string]any {
	return map[string]any{
		"name":                            name,
		"concurrency":                     0,
		"worker_concurrency":              7,
		"rate_limit_max":                  0,
		"rate_limit_period_sec":           0,
		"priority_enabled":                false,
		"partition_queue":                 false,
		"polling_interval_sec":            0,
		"application_name":                "fixture-app",
		"partition_concurrency":           0,
		"partition_worker_concurrency":    4,
		"partition_rate_limit_max":        0,
		"partition_rate_limit_period_sec": 0,
	}
}

func pinnedQueueFields(t *testing.T) []string {
	t.Helper()
	b := generatedOpenAPIJSON(t)
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
	fields := spec.Components.Schemas["Queue"].Required
	if len(fields) != 13 {
		t.Fatalf("documented Queue required fields = %d, want 13: %v", len(fields), fields)
	}
	return fields
}

func queueFieldText(t *testing.T, body, field string) string {
	t.Helper()
	re := regexp.MustCompile(`(?s)<td[^>]*data-field="` + regexp.QuoteMeta(field) + `"[^>]*>\s*([^<]*?)\s*</td>`)
	match := re.FindStringSubmatch(body)
	if match == nil {
		t.Fatalf("queue detail missing field %q: %s", field, body)
	}
	return strings.TrimSpace(html.UnescapeString(match[1]))
}

func extractQueueHref(t *testing.T, body, class string) string {
	t.Helper()
	re := regexp.MustCompile(`<a class="` + regexp.QuoteMeta(class) + `" href="([^"]+)"`)
	match := re.FindStringSubmatch(body)
	if match == nil {
		t.Fatalf("missing %s link: %s", class, body)
	}
	return html.UnescapeString(match[1])
}

func getBodyNoRedirect(t *testing.T, target string) (int, string) {
	t.Helper()
	client := &http.Client{
		Timeout: 3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(target)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

func TestQueueConsoleDetailSchemaAndReadOnlyWire(t *testing.T) {
	ts, h := newTestServer(t)
	var calls atomic.Int32
	fe := dialFake(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetQueue: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": queueConsoleWireRecord("jobs")}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	code, body := getBody(t, ts.URL+"/apps/fixture-app/queues/jobs")
	if code != 200 {
		t.Fatalf("queue detail status = %d, want 200: %s", code, body)
	}
	if !strings.Contains(body, `class="status-dot status-green"`) || !strings.Contains(body, `<div class="brand">maestro</div>`) {
		t.Fatal("queue detail must render maestro branding and ready application status")
	}

	want := map[string]string{
		"name":                         "jobs",
		"concurrency":                  "0",
		"workerConcurrency":            "7",
		"rateLimitMax":                 "0",
		"rateLimitPeriodSecs":          "0",
		"priorityEnabled":              "No",
		"partitionQueue":               "No",
		"pollingIntervalSecs":          "0",
		"applicationName":              "fixture-app",
		"partitionConcurrency":         "0",
		"partitionWorkerConcurrency":   "4",
		"partitionRateLimitMax":        "0",
		"partitionRateLimitPeriodSecs": "0",
	}
	for _, field := range pinnedQueueFields(t) {
		if got := queueFieldText(t, body, field); got != want[field] {
			t.Errorf("field %s = %q, want %q", field, got, want[field])
		}
	}
	if got := strings.Count(body, `data-field="`); got != 13 {
		t.Errorf("rendered queue field count = %d, want 13", got)
	}

	wire := fe.body(t, protocol.MsgGetQueue)
	if wire["type"] != "get_queue" || wire["name"] != "jobs" {
		t.Fatalf("unexpected queue read wire request: %#v", wire)
	}
	if _, ok := wire["body"]; ok || len(wire) != 3 {
		t.Fatalf("GET_QUEUE request grew unexpected fields: %#v", wire)
	}
	if calls.Load() != 1 {
		t.Fatalf("GET_QUEUE calls = %d, want 1", calls.Load())
	}

	postCode, _ := postBody(t, ts.URL+"/apps/fixture-app/queues/jobs")
	if postCode != 405 {
		t.Fatalf("queue detail POST status = %d, want 405", postCode)
	}
	if calls.Load() != 1 {
		t.Fatalf("non-GET dispatched to executor; calls = %d", calls.Load())
	}
}

func TestQueueConsoleDetailNullSemantics(t *testing.T) {
	record := queueConsoleWireRecord("nullable")
	for _, field := range []string{
		"concurrency", "worker_concurrency", "rate_limit_max", "rate_limit_period_sec",
		"application_name", "partition_concurrency", "partition_worker_concurrency",
		"partition_rate_limit_max", "partition_rate_limit_period_sec",
	} {
		record[field] = nil
	}
	ts, h := newTestServer(t)
	dialFake(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetQueue: func(map[string]any) map[string]any {
			return map[string]any{"output": record}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	code, body := getBody(t, ts.URL+"/apps/fixture-app/queues/nullable")
	if code != 200 {
		t.Fatalf("nullable queue status = %d: %s", code, body)
	}
	for _, field := range []string{
		"concurrency", "workerConcurrency", "rateLimitMax", "partitionConcurrency",
		"partitionWorkerConcurrency", "partitionRateLimitMax",
	} {
		if got := queueFieldText(t, body, field); got != "Unlimited" {
			t.Errorf("nullable limit %s = %q, want Unlimited", field, got)
		}
	}
	for _, field := range []string{"rateLimitPeriodSecs", "applicationName", "partitionRateLimitPeriodSecs"} {
		if got := queueFieldText(t, body, field); got != "Unavailable" {
			t.Errorf("nullable information %s = %q, want Unavailable", field, got)
		}
	}
	if got := queueFieldText(t, body, "priorityEnabled"); got != "No" {
		t.Errorf("explicit false priorityEnabled = %q, want No", got)
	}
	if got := queueFieldText(t, body, "partitionQueue"); got != "No" {
		t.Errorf("explicit false partitionQueue = %q, want No", got)
	}
	if got := queueFieldText(t, body, "pollingIntervalSecs"); got != "0" {
		t.Errorf("explicit zero pollingIntervalSecs = %q, want 0", got)
	}
}

func TestQueueConsoleListLinksAndHostileNameRoundTrip(t *testing.T) {
	const hostile = `ops/blue ?x=<b>unsafe</b>&y=%#`
	ts, h := newTestServer(t)
	var gotName atomic.Value
	dialFake(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]respondFn{
		protocol.MsgListQueues: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{queueConsoleWireRecord(hostile)}}
		},
		protocol.MsgGetQueue: func(req map[string]any) map[string]any {
			gotName.Store(req["name"])
			return map[string]any{"output": queueConsoleWireRecord(hostile)}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	code, list := getBody(t, ts.URL+"/apps/fixture-app/queues")
	if code != 200 {
		t.Fatalf("queue list status = %d: %s", code, list)
	}
	if strings.Contains(list, "<b>unsafe</b>") || !strings.Contains(list, "&lt;b&gt;unsafe&lt;/b&gt;") {
		t.Fatalf("hostile queue name was not HTML escaped: %s", list)
	}

	detailHref := extractQueueHref(t, list, "link queue-detail-link")
	if !strings.Contains(detailHref, "%2F") || strings.Contains(detailHref, hostile) {
		t.Fatalf("queue detail href was not path encoded: %q", detailHref)
	}
	code, detail := getBody(t, ts.URL+detailHref)
	if code != 200 {
		t.Fatalf("encoded queue detail status = %d: %s", code, detail)
	}
	if got, _ := gotName.Load().(string); got != hostile {
		t.Fatalf("encoded queue name round trip = %q, want %q", got, hostile)
	}
	if strings.Contains(detail, "<b>unsafe</b>") || queueFieldText(t, detail, "name") != hostile {
		t.Fatalf("hostile detail name was not safely rendered: %s", detail)
	}

	workflowsHref := extractQueueHref(t, list, "link queue-workflows-link")
	parsed, err := url.Parse(workflowsHref)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Path != "/apps/fixture-app/workflows" || parsed.Query().Get("queue") != hostile || parsed.Query().Get("children") != "true" {
		t.Fatalf("queue-to-workflow link did not preserve name: %q", workflowsHref)
	}
}

func TestQueueConsoleDotNamesRoundTripThroughListLinks(t *testing.T) {
	for _, name := range []string{".", ".."} {
		t.Run(name, func(t *testing.T) {
			ts, h := newTestServer(t)
			var calls atomic.Int32
			var gotName atomic.Value
			dialFake(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]respondFn{
				protocol.MsgListQueues: func(map[string]any) map[string]any {
					return map[string]any{"output": []any{queueConsoleWireRecord(name)}}
				},
				protocol.MsgGetQueue: func(req map[string]any) map[string]any {
					calls.Add(1)
					gotName.Store(req["name"])
					return map[string]any{"output": queueConsoleWireRecord(name)}
				},
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })

			code, list := getBody(t, ts.URL+"/apps/fixture-app/queues")
			if code != 200 {
				t.Fatalf("queue list status = %d: %s", code, list)
			}
			detailHref := extractQueueHref(t, list, "link queue-detail-link")
			parsed, err := url.Parse(detailHref)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Path != "/apps/fixture-app/queue" || len(parsed.Query()) != 1 || parsed.Query().Get("name") != name {
				t.Fatalf("dot queue link did not use exact-name alias: %q", detailHref)
			}
			code, detail := getBodyNoRedirect(t, ts.URL+detailHref)
			if code != 200 {
				t.Fatalf("dot queue link %q status = %d, want 200 without canonical redirect: %s", detailHref, code, detail)
			}
			if got, _ := gotName.Load().(string); got != name {
				t.Fatalf("dot queue name round trip = %q, want %q", got, name)
			}
			if calls.Load() != 1 {
				t.Fatalf("dot queue dispatched %d GET_QUEUE requests, want 1", calls.Load())
			}
			if got := queueFieldText(t, detail, "name"); got != name {
				t.Fatalf("rendered dot queue name = %q, want %q", got, name)
			}
		})
	}
}

func TestQueueConsoleDotPrefixNamesKeepPathRoute(t *testing.T) {
	for _, name := range []string{".hidden", "..hidden", "..."} {
		t.Run(name, func(t *testing.T) {
			ts, h := newTestServer(t)
			var gotName atomic.Value
			dialFake(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]respondFn{
				protocol.MsgListQueues: func(map[string]any) map[string]any {
					return map[string]any{"output": []any{queueConsoleWireRecord(name)}}
				},
				protocol.MsgGetQueue: func(req map[string]any) map[string]any {
					gotName.Store(req["name"])
					return map[string]any{"output": queueConsoleWireRecord(name)}
				},
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })

			code, list := getBody(t, ts.URL+"/apps/fixture-app/queues")
			if code != 200 {
				t.Fatalf("queue list status = %d: %s", code, list)
			}
			detailHref := extractQueueHref(t, list, "link queue-detail-link")
			if strings.Contains(detailHref, "/queue?") {
				t.Fatalf("non-dot queue name collided with alias: %q", detailHref)
			}
			code, detail := getBodyNoRedirect(t, ts.URL+detailHref)
			if code != 200 || queueFieldText(t, detail, "name") != name {
				t.Fatalf("ordinary path queue failed: status=%d href=%q body=%s", code, detailHref, detail)
			}
			if got, _ := gotName.Load().(string); got != name {
				t.Fatalf("ordinary queue name round trip = %q, want %q", got, name)
			}
		})
	}
}

func TestQueueConsoleDetailAliasRejectsInvalidQuery(t *testing.T) {
	ts, h := newTestServer(t)
	var calls atomic.Int32
	dialFake(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]respondFn{
		protocol.MsgGetQueue: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": queueConsoleWireRecord("unexpected")}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	for _, query := range []string{"", "?name=", "?name=.&name=..", "?name=.&extra=1", "?name=.;bad"} {
		code, body := getBodyNoRedirect(t, ts.URL+"/apps/fixture-app/queue"+query)
		if code != 400 || !strings.Contains(body, "exactly one nonempty name") {
			t.Errorf("invalid alias query %q: status=%d body=%s", query, code, body)
		}
	}
	postCode, _ := postBody(t, ts.URL+"/apps/fixture-app/queue?name=.")
	if postCode != 405 {
		t.Fatalf("queue detail alias POST status = %d, want 405", postCode)
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid or non-GET alias request dispatched %d executor requests", calls.Load())
	}
}

func TestQueueConsoleBrowserURLResolution(t *testing.T) {
	if os.Getenv("MAESTRO_QUEUE_URL_BROWSER_GATE") != "1" {
		t.Skip("set MAESTRO_QUEUE_URL_BROWSER_GATE=1 to run the explicit Node WHATWG URL gate")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("explicit browser URL gate requires Node: ", err)
	}
	for _, name := range []string{".", ".."} {
		t.Run(name, func(t *testing.T) {
			ts, h := newTestServer(t)
			var gotName atomic.Value
			dialFake(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]respondFn{
				protocol.MsgListQueues: func(map[string]any) map[string]any {
					return map[string]any{"output": []any{queueConsoleWireRecord(name)}}
				},
				protocol.MsgGetQueue: func(req map[string]any) map[string]any {
					gotName.Store(req["name"])
					return map[string]any{"output": queueConsoleWireRecord(name)}
				},
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })

			cmd := exec.Command(node, "../../dev/tests/queue_url_browser.mjs", ts.URL, name)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("Node WHATWG URL gate: %v\n%s", err, output)
			}
			t.Log(strings.TrimSpace(string(output)))
			if got, _ := gotName.Load().(string); got != name {
				t.Fatalf("browser-resolved queue name = %q, want %q", got, name)
			}
		})
	}
}

func TestQueueConsoleDetailErrorsAreVisible(t *testing.T) {
	t.Run("missing queue", func(t *testing.T) {
		ts, h := newTestServer(t)
		dialFake(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]respondFn{
			protocol.MsgGetQueue: func(map[string]any) map[string]any { return map[string]any{"output": nil} },
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		code, body := getBody(t, ts.URL+"/apps/fixture-app/queues/absent")
		if code != 404 || !strings.Contains(body, "queue &#34;absent&#34; not found") {
			t.Fatalf("missing queue not visible: %d %s", code, body)
		}
	})

	t.Run("no connected executor", func(t *testing.T) {
		ts, _ := newTestServer(t)
		code, body := getBody(t, ts.URL+"/apps/fixture-app/queues/jobs")
		if code != 503 || !strings.Contains(body, "maestro: application unavailable") {
			t.Fatalf("executor unavailability not visible: %d %s", code, body)
		}
	})

	t.Run("selected peer disconnect without alternate", func(t *testing.T) {
		ts, h := newTestServer(t)
		var calls atomic.Int32
		dialFake(t, ts, "fixture-app", "testkey", "disconnecting", map[protocol.MessageType]respondFn{
			protocol.MsgGetQueue: func(map[string]any) map[string]any {
				calls.Add(1)
				return nil
			},
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		code, body := getBody(t, ts.URL+"/apps/fixture-app/queues/jobs")
		if code != 502 || calls.Load() != 1 || !strings.Contains(body, "connection closed") {
			t.Fatalf("selected-peer disconnect not visible: status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})

	t.Run("selected peer disconnect retries pure read", func(t *testing.T) {
		ts, h := newTestServer(t)
		var calls atomic.Int32
		handle := func(map[string]any) map[string]any {
			if calls.Add(1) == 1 {
				return nil
			}
			return map[string]any{"output": queueConsoleWireRecord("jobs")}
		}
		for _, id := range []string{"one", "two"} {
			dialFake(t, ts, "fixture-app", "testkey", id, map[protocol.MessageType]respondFn{protocol.MsgGetQueue: handle})
		}
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, body := getBody(t, ts.URL+"/apps/fixture-app/queues/jobs")
		if code != 200 || calls.Load() != 2 || queueFieldText(t, body, "name") != "jobs" {
			t.Fatalf("GET_QUEUE disconnect retry failed: status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})

	t.Run("executor refusal", func(t *testing.T) {
		ts, h := newTestServer(t)
		var calls atomic.Int32
		refuse := func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"error_message": "metadata <only> & denied"}
		}
		for _, id := range []string{"one", "two"} {
			dialFake(t, ts, "fixture-app", "testkey", id, map[protocol.MessageType]respondFn{protocol.MsgGetQueue: refuse})
		}
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, body := getBody(t, ts.URL+"/apps/fixture-app/queues/jobs")
		if code != 502 || !strings.Contains(body, "metadata &lt;only&gt; &amp; denied") || strings.Contains(body, "metadata <only>") {
			t.Fatalf("executor refusal not safely visible: %d %s", code, body)
		}
		if calls.Load() != 1 {
			t.Fatalf("executor refusal was retried: %d", calls.Load())
		}
	})

	t.Run("missing SDK field", func(t *testing.T) {
		record := queueConsoleWireRecord("jobs")
		delete(record, "concurrency")
		ts, h := newTestServer(t)
		dialFake(t, ts, "fixture-app", "testkey", "queue-exec", map[protocol.MessageType]respondFn{
			protocol.MsgGetQueue: func(map[string]any) map[string]any { return map[string]any{"output": record} },
		})
		waitFor(t, func() bool { return len(h.Executors()) == 1 })
		code, body := getBody(t, ts.URL+"/apps/fixture-app/queues/jobs")
		if code != 502 || !strings.Contains(body, "missing required fields") || strings.Contains(body, `data-field="concurrency"`) {
			t.Fatalf("missing queue field was fabricated: %d %s", code, body)
		}
	})
}
