package api_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/zpaden/maestro/internal/protocol"
)

const localV2RelatedWorkflowRoot = "/v2/orgs/local/apps/fixture-app/workflows"

// These records are independent examples of the released Python 3.1.0
// EventOutput, NotificationOutput, and StreamEntryOutput wire shapes.
func relatedWireResponses() map[protocol.MessageType]map[string]any {
	return map[protocol.MessageType]map[string]any{
		protocol.MsgGetWorkflowEvents: {
			"events": []any{
				map[string]any{"key": "", "value": `{"opaque":1}`},
				map[string]any{"key": "unicode-事件", "value": "[\"exact\"]"},
			},
		},
		protocol.MsgGetWorkflowNotifications: {
			"notifications": []any{
				map[string]any{"topic": nil, "message": "[\"opaque\"]", "created_at_epoch_ms": int64(0), "consumed": false},
				map[string]any{"topic": "", "message": "", "created_at_epoch_ms": int64(1720000000123), "consumed": true},
			},
		},
		protocol.MsgGetWorkflowStreams: {
			"streams": []any{
				map[string]any{"key": "gate-stream", "values": []any{"\"first\"", "", `{"page":3}`}},
				map[string]any{"key": "empty-stream", "values": []any{}},
			},
		},
	}
}

func relatedExistingWorkflow(id any) map[string]any {
	// Deliberately omit strict official Workflow fields and retain legal SDK nulls.
	// Related-data existence must not depend on the Workflow HTTP mapper.
	return map[string]any{"WorkflowUUID": id, "Status": "SUCCESS", "UpdatedAt": nil, "Priority": nil}
}

func relatedHTTPType(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case float64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return fmt.Sprintf("%T", value)
	}
}

// assertRelatedSchema is an OpenAPI-snapshot oracle. It neither imports nor
// derives expected fields from maestro's mappers or protocol DTOs.
func assertRelatedSchema(t *testing.T, schemaName string, record map[string]any) {
	t.Helper()
	snapshot, err := os.ReadFile("../../docs/reference/conductor-openapi-2026-09-25.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Components struct {
			Schemas map[string]struct {
				AdditionalProperties bool `json:"additionalProperties"`
				Required             []string
				Properties           map[string]struct {
					Type   json.RawMessage
					Format string
					Items  *struct {
						Type string
					}
				}
			}
		}
	}
	if err := json.Unmarshal(snapshot, &spec); err != nil {
		t.Fatal(err)
	}
	schema, ok := spec.Components.Schemas[schemaName]
	if !ok || len(schema.Required) == 0 {
		t.Fatalf("pinned schema %s missing", schemaName)
	}
	for _, field := range schema.Required {
		if _, ok := record[field]; !ok {
			t.Errorf("%s missing required field %s", schemaName, field)
		}
	}
	for field, value := range record {
		property, ok := schema.Properties[field]
		if !ok {
			t.Errorf("%s contains undocumented field %s", schemaName, field)
			continue
		}
		var declared any
		if err := json.Unmarshal(property.Type, &declared); err != nil {
			t.Fatal(err)
		}
		allowed := map[string]bool{}
		switch typed := declared.(type) {
		case string:
			allowed[typed] = true
		case []any:
			for _, item := range typed {
				allowed[item.(string)] = true
			}
		}
		actual := relatedHTTPType(value)
		if actual == "number" && allowed["integer"] {
			actual = "integer"
		}
		if !allowed[actual] {
			t.Errorf("%s.%s type=%s want=%v", schemaName, field, actual, allowed)
		}
		if property.Format == "date-time" && value != nil {
			if _, err := time.Parse(time.RFC3339Nano, value.(string)); err != nil {
				t.Errorf("%s.%s is not RFC3339: %v", schemaName, field, err)
			}
		}
		if property.Items != nil && value != nil {
			for i, item := range value.([]any) {
				if relatedHTTPType(item) != property.Items.Type {
					t.Errorf("%s.%s[%d] type=%s", schemaName, field, i, relatedHTTPType(item))
				}
			}
		}
	}
	if !schema.AdditionalProperties && len(record) != len(schema.Properties) {
		t.Errorf("%s fields=%v want exactly %v", schemaName, record, schema.Properties)
	}
}

func TestLocalHTTPV2RelatedSchemaAndWire(t *testing.T) {
	ts, h := localV2Server(t, true)
	responses := relatedWireResponses()
	fe := dialScheduleFake(t, ts.URL, "related-exec", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
		},
		protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any {
			return responses[protocol.MsgGetWorkflowEvents]
		},
		protocol.MsgGetWorkflowNotifications: func(map[string]any) map[string]any {
			return responses[protocol.MsgGetWorkflowNotifications]
		},
		protocol.MsgGetWorkflowStreams: func(map[string]any) map[string]any {
			return responses[protocol.MsgGetWorkflowStreams]
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	cases := []struct {
		suffix, schema string
		want           []map[string]any
	}{
		{"events", "Event", []map[string]any{
			{"key": "", "value": `{"opaque":1}`},
			{"key": "unicode-事件", "value": "[\"exact\"]"},
		}},
		{"notifications", "Notification", []map[string]any{
			{"topic": nil, "message": "[\"opaque\"]", "createdAt": "1970-01-01T00:00:00.000Z", "consumed": false},
			{"topic": "", "message": "", "createdAt": "2024-07-03T09:46:40.123Z", "consumed": true},
		}},
		{"streams", "StreamEntry", []map[string]any{
			{"key": "gate-stream", "values": []any{"\"first\"", "", `{"page":3}`}},
			{"key": "empty-stream", "values": []any{}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.suffix, func(t *testing.T) {
			code, contentType, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/wf-相关/"+tc.suffix, "GET", "")
			if code != 200 || !strings.HasPrefix(contentType, "application/json") {
				t.Fatalf("status=%d content-type=%s body=%s", code, contentType, body)
			}
			var records []map[string]any
			if err := json.Unmarshal([]byte(body), &records); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(records, tc.want) {
				t.Fatalf("response=%#v want=%#v", records, tc.want)
			}
			for _, record := range records {
				assertRelatedSchema(t, tc.schema, record)
			}
		})
	}

	for _, command := range []protocol.MessageType{
		protocol.MsgGetWorkflowEvents,
		protocol.MsgGetWorkflowNotifications,
		protocol.MsgGetWorkflowStreams,
	} {
		request := fe.body(t, command)
		if len(request) != 3 || request["type"] != string(command) || request["workflow_id"] != "wf-相关" {
			t.Errorf("%s request invented or changed options: %#v", command, request)
		}
	}
	existence := fe.body(t, protocol.MsgGetWorkflow)
	if len(existence) != 5 || existence["workflow_id"] != "wf-相关" || existence["load_input"] != false || existence["load_output"] != false {
		t.Fatalf("existence wire=%#v", existence)
	}
	fe.mu.Lock()
	defer fe.mu.Unlock()
	for command := range fe.captured {
		switch protocol.MessageType(command) {
		case protocol.MsgGetWorkflow, protocol.MsgGetWorkflowEvents, protocol.MsgGetWorkflowNotifications, protocol.MsgGetWorkflowStreams:
		default:
			t.Errorf("official related read dispatched non-read command %s", command)
		}
	}
}

func TestLocalHTTPV2RelatedEmptyMissingAndMalformedResponses(t *testing.T) {
	commands := []struct {
		suffix  string
		command protocol.MessageType
		field   string
	}{
		{"events", protocol.MsgGetWorkflowEvents, "events"},
		{"notifications", protocol.MsgGetWorkflowNotifications, "notifications"},
		{"streams", protocol.MsgGetWorkflowStreams, "streams"},
	}
	for _, command := range commands {
		for _, response := range []struct {
			name    string
			payload map[string]any
			status  int
		}{
			{"empty", map[string]any{command.field: []any{}}, 200},
			{"null", map[string]any{command.field: nil}, 502},
			{"missing", map[string]any{}, 502},
			{"not-array", map[string]any{command.field: map[string]any{}}, 502},
		} {
			t.Run(command.suffix+"/"+response.name, func(t *testing.T) {
				ts, h := localV2Server(t, true)
				var relatedCalls atomic.Int32
				dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]respondFn{
					protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
						return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
					},
					command.command: func(map[string]any) map[string]any {
						relatedCalls.Add(1)
						return response.payload
					},
				})
				waitFor(t, func() bool { return len(h.Executors()) == 1 })
				code, contentType, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/wf-1/"+command.suffix, "GET", "")
				if code != response.status {
					t.Fatalf("status=%d want=%d body=%s", code, response.status, body)
				}
				if code == 200 && strings.TrimSpace(body) != "[]" {
					t.Fatalf("empty collection encoded as %s", body)
				}
				if code >= 400 && !strings.HasPrefix(contentType, "application/problem+json") {
					t.Fatalf("content-type=%s body=%s", contentType, body)
				}
				if relatedCalls.Load() != 1 {
					t.Fatalf("related calls=%d", relatedCalls.Load())
				}
			})
		}
	}

	malformed := []struct {
		name    string
		command protocol.MessageType
		field   string
		item    map[string]any
	}{
		{"event-missing-key", protocol.MsgGetWorkflowEvents, "events", map[string]any{"value": "v"}},
		{"event-null-key", protocol.MsgGetWorkflowEvents, "events", map[string]any{"key": nil, "value": "v"}},
		{"event-value-type", protocol.MsgGetWorkflowEvents, "events", map[string]any{"key": "k", "value": 1}},
		{"notification-missing-topic", protocol.MsgGetWorkflowNotifications, "notifications", map[string]any{"message": "m", "created_at_epoch_ms": 0, "consumed": false}},
		{"notification-null-message", protocol.MsgGetWorkflowNotifications, "notifications", map[string]any{"topic": nil, "message": nil, "created_at_epoch_ms": 0, "consumed": false}},
		{"notification-missing-consumed", protocol.MsgGetWorkflowNotifications, "notifications", map[string]any{"topic": nil, "message": "m", "created_at_epoch_ms": 0}},
		{"notification-consumed-type", protocol.MsgGetWorkflowNotifications, "notifications", map[string]any{"topic": nil, "message": "m", "created_at_epoch_ms": 0, "consumed": 0}},
		{"notification-fractional-time", protocol.MsgGetWorkflowNotifications, "notifications", map[string]any{"topic": nil, "message": "m", "created_at_epoch_ms": 1.5, "consumed": false}},
		{"notification-time-type", protocol.MsgGetWorkflowNotifications, "notifications", map[string]any{"topic": nil, "message": "m", "created_at_epoch_ms": "0", "consumed": false}},
		{"notification-time-overflow", protocol.MsgGetWorkflowNotifications, "notifications", map[string]any{"topic": nil, "message": "m", "created_at_epoch_ms": int64(253402300800000), "consumed": false}},
		{"stream-null-key", protocol.MsgGetWorkflowStreams, "streams", map[string]any{"key": nil, "values": []any{}}},
		{"stream-missing-values", protocol.MsgGetWorkflowStreams, "streams", map[string]any{"key": "k"}},
		{"stream-null-values", protocol.MsgGetWorkflowStreams, "streams", map[string]any{"key": "k", "values": nil}},
		{"stream-value-type", protocol.MsgGetWorkflowStreams, "streams", map[string]any{"key": "k", "values": []any{"v", 1}}},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			ts, h := localV2Server(t, true)
			dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]respondFn{
				protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
					return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
				},
				tc.command: func(map[string]any) map[string]any {
					return map[string]any{tc.field: []any{tc.item}}
				},
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			suffix := map[protocol.MessageType]string{
				protocol.MsgGetWorkflowEvents:        "events",
				protocol.MsgGetWorkflowNotifications: "notifications",
				protocol.MsgGetWorkflowStreams:       "streams",
			}[tc.command]
			code, contentType, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/wf-1/"+suffix, "GET", "")
			if code != 502 || !strings.HasPrefix(contentType, "application/problem+json") || !strings.Contains(body, "detail") {
				t.Fatalf("status=%d content-type=%s body=%s", code, contentType, body)
			}
		})
	}
}

func TestLocalHTTPV2RelatedDistinguishesMissingWorkflow(t *testing.T) {
	responses := []struct {
		name   string
		body   map[string]any
		status int
	}{
		{name: "explicit-null", body: map[string]any{"output": nil}, status: 404},
		{name: "absent-output", body: map[string]any{}, status: 502},
		{name: "non-object-output", body: map[string]any{"output": []any{}}, status: 502},
		{name: "missing-workflow-id", body: map[string]any{"output": map[string]any{"Status": "SUCCESS"}}, status: 502},
		{name: "wrong-workflow-id-type", body: map[string]any{"output": map[string]any{"WorkflowUUID": 7}}, status: 502},
	}
	for _, suffix := range []string{"events", "notifications", "streams"} {
		for _, response := range responses {
			t.Run(suffix+"/"+response.name, func(t *testing.T) {
				ts, h := localV2Server(t, true)
				var relatedCalls atomic.Int32
				dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]respondFn{
					protocol.MsgGetWorkflow: func(map[string]any) map[string]any { return response.body },
					protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any {
						relatedCalls.Add(1)
						return map[string]any{"events": []any{}}
					},
					protocol.MsgGetWorkflowNotifications: func(map[string]any) map[string]any {
						relatedCalls.Add(1)
						return map[string]any{"notifications": []any{}}
					},
					protocol.MsgGetWorkflowStreams: func(map[string]any) map[string]any {
						relatedCalls.Add(1)
						return map[string]any{"streams": []any{}}
					},
				})
				waitFor(t, func() bool { return len(h.Executors()) == 1 })
				code, contentType, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/missing/"+suffix, "GET", "")
				if code != response.status || !strings.HasPrefix(contentType, "application/problem+json") || relatedCalls.Load() != 0 {
					t.Fatalf("status=%d want=%d calls=%d body=%s", code, response.status, relatedCalls.Load(), body)
				}
			})
		}
	}
}

func TestLocalHTTPV2RelatedRequestValidationAndCapability(t *testing.T) {
	ts, h := localV2Server(t, true)
	var calls atomic.Int32
	fe := dialScheduleFake(t, ts.URL, "one", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
		},
		protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any {
			calls.Add(1)
			return map[string]any{"events": []any{}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	invalid := []string{
		localV2RelatedWorkflowRoot + "/wf-1/events?limit=0",
		localV2RelatedWorkflowRoot + "/wf-1/events?offset=1&offset=2",
		localV2RelatedWorkflowRoot + "/wf-1/events?topic=",
		localV2RelatedWorkflowRoot + "/wf-1/events?bad=%zz",
		localV2RelatedWorkflowRoot + "/wf-1/events?bad=%FF",
		"/v2/orgs/local/apps/ab/workflows/wf-1/events",
		"/v2/orgs/local/apps/Fixture-App/workflows/wf-1/events",
		"/v2/orgs/local/apps/fixture.app/workflows/wf-1/events",
		"/v2/orgs/local/apps/fixture-app/workflows/%FF/events",
	}
	for _, path := range invalid {
		code, contentType, body := localV2Request(t, ts.URL+path, "GET", "")
		if code != 400 || !strings.HasPrefix(contentType, "application/problem+json") {
			t.Errorf("invalid path/query %q: status=%d content-type=%s body=%s", path, code, contentType, body)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid request reached executor %d times", calls.Load())
	}

	unicodeID := "工作流-ß"
	if !utf8.ValidString(unicodeID) {
		t.Fatal("test workflow ID is invalid")
	}
	code, _, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/"+unicodeID+"/events", "GET", "")
	if code != 200 {
		t.Fatalf("valid Unicode workflow ID: status=%d body=%s", code, body)
	}
	if request := fe.body(t, protocol.MsgGetWorkflowEvents); request["workflow_id"] != unicodeID {
		t.Fatalf("Unicode workflow ID changed: %#v", request)
	}
	if calls.Load() != 2 {
		t.Fatalf("valid request dispatch count=%d", calls.Load())
	}

	code, _, _ = localV2Request(t, ts.URL+strings.Replace(localV2RelatedWorkflowRoot, "/local/", "/other/", 1)+"/wf-1/events", "GET", "")
	if code != 404 {
		t.Fatalf("unknown org status=%d", code)
	}
	unavailable, _ := localV2Server(t, true)
	code, _, _ = localV2Request(t, unavailable.URL+localV2RelatedWorkflowRoot+"/wf-1/events", "GET", "")
	if code != 503 {
		t.Fatalf("unavailable app status=%d", code)
	}
	disabled, _ := localV2Server(t, false)
	code, _, _ = localV2Request(t, disabled.URL+localV2RelatedWorkflowRoot+"/wf-1/events", "GET", "")
	if code != 404 {
		t.Fatalf("default-off status=%d", code)
	}

	unsupported, unsupportedHub := localV2Server(t, true)
	var unsupportedCalls atomic.Int32
	dialScheduleFake(t, unsupported.URL, "future", "3.1.1", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
			unsupportedCalls.Add(1)
			return map[string]any{"output": relatedExistingWorkflow("wf-1")}
		},
		protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any {
			unsupportedCalls.Add(1)
			return map[string]any{"events": []any{}}
		},
	})
	waitFor(t, func() bool { return len(unsupportedHub.Executors()) == 1 })
	code, contentType, body := localV2Request(t, unsupported.URL+localV2RelatedWorkflowRoot+"/wf-1/events", "GET", "")
	if code != 502 || !strings.HasPrefix(contentType, "application/problem+json") || unsupportedCalls.Load() != 0 {
		t.Fatalf("unknown SDK: status=%d calls=%d body=%s", code, unsupportedCalls.Load(), body)
	}
}

func TestLocalHTTPV2RelatedReviewedVersions(t *testing.T) {
	for _, version := range []string{"2.24.0", "2.31.1", "3.1.0"} {
		t.Run(version, func(t *testing.T) {
			ts, h := localV2Server(t, true)
			dialScheduleFake(t, ts.URL, "one", version, map[protocol.MessageType]respondFn{
				protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
					return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
				},
				protocol.MsgGetWorkflowEvents: func(map[string]any) map[string]any {
					return map[string]any{"events": []any{}}
				},
			})
			waitFor(t, func() bool { return len(h.Executors()) == 1 })
			code, _, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/wf-1/events", "GET", "")
			if code != 200 || strings.TrimSpace(body) != "[]" {
				t.Fatalf("reviewed SDK %s: status=%d body=%s", version, code, body)
			}
		})
	}
}

func TestLocalHTTPV2RelatedCapabilityAllowsReviewedPeerAlongsidePreexistingUnknownPeer(t *testing.T) {
	ts, h := localV2Server(t, true)
	var reviewedRelatedCalls atomic.Int32
	dialScheduleFake(t, ts.URL, "reviewed", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
		},
		protocol.MsgGetWorkflowStreams: func(map[string]any) map[string]any {
			reviewedRelatedCalls.Add(1)
			return map[string]any{"streams": []any{}}
		},
	})
	var unknownRelatedCalls atomic.Int32
	dialScheduleFake(t, ts.URL, "unknown", "3.1.1", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
		},
		protocol.MsgGetWorkflowStreams: func(map[string]any) map[string]any {
			unknownRelatedCalls.Add(1)
			return map[string]any{"streams": []any{map[string]any{"key": "private", "values": []any{"bypassed"}}}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 2 })

	code, _, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/wf-1/streams", "GET", "")
	if code != 200 || strings.TrimSpace(body) != "[]" {
		t.Fatalf("mixed peer request: status=%d body=%s", code, body)
	}
	if reviewedRelatedCalls.Load() != 1 || unknownRelatedCalls.Load() != 0 {
		t.Fatalf("related calls: reviewed=%d unknown=%d", reviewedRelatedCalls.Load(), unknownRelatedCalls.Load())
	}
}

func TestLocalHTTPV2RelatedCapabilityGatesPeersJoiningDuringExistenceRead(t *testing.T) {
	ts, h := localV2Server(t, true)
	existenceStarted := make(chan struct{})
	releaseExistence := make(chan struct{})
	dialScheduleFake(t, ts.URL, "reviewed", "3.1.0", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
			close(existenceStarted)
			<-releaseExistence
			return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
		},
		protocol.MsgGetWorkflowStreams: func(map[string]any) map[string]any {
			return nil // Retry must not route around this disconnect to an unknown peer.
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })

	type httpResult struct {
		status, contentType, body string
		err                       error
	}
	result := make(chan httpResult, 1)
	go func() {
		response, err := (&http.Client{Timeout: 3 * time.Second}).Get(ts.URL + localV2RelatedWorkflowRoot + "/wf-1/streams")
		if err != nil {
			result <- httpResult{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		result <- httpResult{status: response.Status, contentType: response.Header.Get("Content-Type"), body: string(body), err: err}
	}()
	select {
	case <-existenceStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("existence read did not start")
	}

	var unknownCalls atomic.Int32
	dialScheduleFake(t, ts.URL, "unknown", "3.1.1", map[protocol.MessageType]respondFn{
		protocol.MsgGetWorkflowStreams: func(map[string]any) map[string]any {
			unknownCalls.Add(1)
			return map[string]any{"streams": []any{map[string]any{"key": "private", "values": []any{"bypassed"}}}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 2 })
	close(releaseExistence)
	got := <-result
	if got.err != nil || !strings.HasPrefix(got.status, "502") || !strings.HasPrefix(got.contentType, "application/problem+json") {
		t.Fatalf("request result: %+v", got)
	}
	if unknownCalls.Load() != 0 {
		t.Fatalf("related read reached unknown retry peer %d times", unknownCalls.Load())
	}
}

func TestLocalHTTPV2RelatedRefusalIsFinalAndDisconnectRetries(t *testing.T) {
	t.Run("privacy refusal", func(t *testing.T) {
		ts, h := localV2Server(t, true)
		var calls atomic.Int32
		for _, id := range []string{"one", "two"} {
			dialScheduleFake(t, ts.URL, id, "3.1.0", map[protocol.MessageType]respondFn{
				protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
					return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
				},
				protocol.MsgGetWorkflowNotifications: func(map[string]any) map[string]any {
					calls.Add(1)
					return map[string]any{"error_message": "metadata-only mode refuses notifications", "notifications": nil}
				},
			})
		}
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, _, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/wf-1/notifications", "GET", "")
		if code != 502 || !strings.Contains(body, "metadata-only mode refuses notifications") || calls.Load() != 1 {
			t.Fatalf("status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})

	t.Run("pure read disconnect", func(t *testing.T) {
		ts, h := localV2Server(t, true)
		var calls atomic.Int32
		handlers := map[protocol.MessageType]respondFn{
			protocol.MsgGetWorkflow: func(req map[string]any) map[string]any {
				return map[string]any{"output": relatedExistingWorkflow(req["workflow_id"])}
			},
			protocol.MsgGetWorkflowStreams: func(map[string]any) map[string]any {
				if calls.Add(1) == 1 {
					return nil
				}
				return map[string]any{"streams": []any{map[string]any{"key": "k", "values": []any{"v"}}}}
			},
		}
		for _, id := range []string{"one", "two"} {
			dialScheduleFake(t, ts.URL, id, "3.1.0", handlers)
		}
		waitFor(t, func() bool { return len(h.Executors()) == 2 })
		code, _, body := localV2Request(t, ts.URL+localV2RelatedWorkflowRoot+"/wf-1/streams", "GET", "")
		if code != 200 || calls.Load() != 2 || !strings.Contains(body, `"values":["v"]`) {
			t.Fatalf("status=%d calls=%d body=%s", code, calls.Load(), body)
		}
	})
}
