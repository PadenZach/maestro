package protocol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The wire fixtures come from pinned Python protocol dataclasses and handler-
// checked request shapes, never from the Go types being tested. See testdata/README.md.
type sdkWire struct {
	Requests  map[string]json.RawMessage `json:"requests"`
	Responses map[string]json.RawMessage `json:"responses"`
}

func sdkFixtures(t *testing.T, version string) sdkWire {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", version, "wire.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures sdkWire
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	return fixtures
}

func wireMap(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func checkWire(t *testing.T, got any, expected json.RawMessage) {
	t.Helper()
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if want, actual := wireMap(t, expected), wireMap(t, encoded); !reflect.DeepEqual(actual, want) {
		t.Fatalf("SDK wire mismatch: got %s; want %s", encoded, expected)
	}
}

func TestBaselineRequestWireContract(t *testing.T) {
	id, limit, offset, queue := "wf-sanitized", 5, 2, "demo-queue"
	filtered := ListWorkflowsBody{Status: []string{"SUCCESS"}, WorkflowName: []string{"demo"}, WorkflowIDPrefix: []string{"wf-"}, Limit: &limit, Offset: &offset, SortDesc: true, LoadInput: true, WasForkedFrom: new(bool)}
	requests := map[string]Request{
		"executor_info":              NewRequest(MsgExecutorInfo),
		"list_workflows":             ListWorkflowsRequest(ListWorkflowsBody{}),
		"list_workflows_filtered":    ListWorkflowsRequest(filtered),
		"list_queued_workflows":      ListQueuedWorkflowsRequest(ListWorkflowsBody{}),
		"get_workflow":               GetWorkflowRequest(id, false, true),
		"list_steps":                 ListStepsRequest(id, true, nil, nil),
		"list_steps_paged":           ListStepsRequest(id, false, &limit, &offset),
		"get_workflow_events":        GetWorkflowEventsRequest(id),
		"get_workflow_notifications": GetWorkflowNotificationsRequest(id),
		"get_workflow_streams":       GetWorkflowStreamsRequest(id),
		"list_queues":                ListQueuesRequest(),
		"get_queue":                  GetQueueRequest("demo-queue"),
		"cancel":                     CancelRequest(id, false),
		"resume":                     ResumeRequest(id, nil),
		"resume_queued":              ResumeRequest(id, &queue),
	}
	for _, version := range []string{"2.24.0", "2.31.1", "3.1.0"} {
		fixtures := sdkFixtures(t, version)
		if len(fixtures.Requests) != len(requests) {
			t.Fatalf("%s: fixture requests = %d; builders = %d", version, len(fixtures.Requests), len(requests))
		}
		for name, want := range fixtures.Requests {
			t.Run(version+"/"+name, func(t *testing.T) {
				// The hub adds the correlation ID at send time. Do not mutate the
				// caller's builder value just to compare a fixture.
				frame := make(Request)
				for k, v := range requests[name] {
					frame[k] = v
				}
				frame["request_id"] = "req-sanitized"
				checkWire(t, frame, want)
				if typ, _ := frame["type"].(string); typ != strings.ToLower(typ) {
					t.Fatalf("type is not lowercase: %q", typ)
				}
			})
		}
	}
}

// Each alias is bound to its command independently of the SDK JSON under test.
var fixtureResponseTypes = map[string]MessageType{
	"executor_info":              MsgExecutorInfo,
	"list_workflows":             MsgListWorkflows,
	"list_queued_workflows":      MsgListQueuedWorkflows,
	"get_workflow":               MsgGetWorkflow,
	"get_workflow_missing":       MsgGetWorkflow,
	"list_steps":                 MsgListSteps,
	"list_steps_null":            MsgListSteps,
	"get_workflow_events":        MsgGetWorkflowEvents,
	"get_workflow_notifications": MsgGetWorkflowNotifications,
	"get_workflow_streams":       MsgGetWorkflowStreams,
	"list_queues":                MsgListQueues,
	"get_queue":                  MsgGetQueue,
	"get_queue_missing":          MsgGetQueue,
	"cancel":                     MsgCancel,
	"cancel_failed":              MsgCancel,
	"resume":                     MsgResume,
}

func TestBaselineResponseFixtures(t *testing.T) {
	for _, version := range []string{"2.24.0", "2.31.1", "3.1.0"} {
		fixtures := sdkFixtures(t, version)
		if len(fixtures.Responses) != len(fixtureResponseTypes) {
			t.Fatalf("%s: response fixtures = %d, want %d", version, len(fixtures.Responses), len(fixtureResponseTypes))
		}
		for name, raw := range fixtures.Responses {
			t.Run(version+"/"+name, func(t *testing.T) {
				wantType, known := fixtureResponseTypes[name]
				if !known {
					t.Fatalf("unrecognized response fixture %q", name)
				}
				m := wireMap(t, raw)
				if m["request_id"] != "req-sanitized" || m["type"] != string(wantType) || !hasKey(m, "error_message") || (m["error_message"] != nil && name != "cancel_failed") {
					t.Fatalf("SDK response has wrong command/envelope/null error_message: %v", m)
				}
				env, err := DecodeEnvelope(raw)
				if err != nil || env.Type != wantType || env.RequestID != "req-sanitized" {
					t.Fatalf("decode envelope: %+v, %v", env, err)
				}
				var response BaseResponse
				if err := json.Unmarshal(raw, &response); err != nil {
					t.Fatal(err)
				}
				if name == "cancel_failed" {
					if response.Err() == nil || response.Err().Error() != "sanitized failure" {
						t.Fatalf("lost executor error: %v", response.Err())
					}
				} else if response.Err() != nil {
					t.Fatalf("unexpected executor error: %v", response.Err())
				}
				switch name {
				case "executor_info":
					var v ExecutorInfoResponse
					unmarshalFixture(t, raw, &v)
					if v.ExecutorID != "exec-sanitized" || v.ApplicationVersion != "app-v1" || v.Hostname != nil || v.Language == nil || *v.Language != "python" || v.DBOSVersion == nil || *v.DBOSVersion != version || v.ExecutorMetadata["role"] != "fixture" {
						t.Fatalf("identity/null/metadata mismatch: %+v", v)
					}
					checkResponseFields(t, v, m)
				case "list_workflows", "list_queued_workflows":
					var v ListWorkflowsResponse
					unmarshalFixture(t, raw, &v)
					if len(v.Output) != 1 {
						t.Fatalf("workflow count: %d", len(v.Output))
					}
					checkWorkflow(t, v.Output[0], m["output"].([]any)[0].(map[string]any), version)
				case "get_workflow", "get_workflow_missing":
					var v GetWorkflowResponse
					unmarshalFixture(t, raw, &v)
					if name == "get_workflow_missing" {
						if v.Output != nil {
							t.Fatalf("expected null output: %+v", v.Output)
						}
					} else if v.Output == nil {
						t.Fatal("lost workflow")
					} else {
						checkWorkflow(t, *v.Output, m["output"].(map[string]any), version)
					}
				case "list_steps", "list_steps_null":
					var v ListStepsResponse
					unmarshalFixture(t, raw, &v)
					if name == "list_steps_null" {
						if v.Output != nil {
							t.Fatalf("expected null steps: %+v", v.Output)
						}
					} else if len(v.Output) != 1 || v.Output[0].FunctionID != 1 || v.Output[0].Output == nil || *v.Output[0].Output != `["opaque-step"]` || v.Output[0].Error != nil {
						t.Fatalf("step output/null mismatch: %+v", v.Output)
					} else {
						checkKnownFields(t, v.Output[0], m["output"].([]any)[0].(map[string]any))
					}
				case "get_workflow_events":
					var v GetWorkflowEventsResponse
					unmarshalFixture(t, raw, &v)
					if len(v.Events) != 1 || v.Events[0].Value != `{"opaque":1}` {
						t.Fatalf("events: %+v", v.Events)
					}
					checkKnownFields(t, v.Events[0], m["events"].([]any)[0].(map[string]any))
				case "get_workflow_notifications":
					var v GetWorkflowNotificationsResponse
					unmarshalFixture(t, raw, &v)
					if len(v.Notifications) != 1 || v.Notifications[0].Topic != nil || v.Notifications[0].Message != `["opaque"]` {
						t.Fatalf("notifications: %+v", v.Notifications)
					}
					checkKnownFields(t, v.Notifications[0], m["notifications"].([]any)[0].(map[string]any))
				case "get_workflow_streams":
					var v GetWorkflowStreamsResponse
					unmarshalFixture(t, raw, &v)
					if len(v.Streams) != 1 || len(v.Streams[0].Values) != 1 || v.Streams[0].Values[0] != `"opaque"` {
						t.Fatalf("streams: %+v", v.Streams)
					}
					checkKnownFields(t, v.Streams[0], m["streams"].([]any)[0].(map[string]any))
				case "list_queues":
					var v ListQueuesResponse
					unmarshalFixture(t, raw, &v)
					if len(v.Output) != 1 {
						t.Fatalf("queues: %+v", v.Output)
					}
					checkQueue(t, v.Output[0], m["output"].([]any)[0].(map[string]any), version)
				case "get_queue", "get_queue_missing":
					var v GetQueueResponse
					unmarshalFixture(t, raw, &v)
					if name == "get_queue_missing" {
						if v.Output != nil {
							t.Fatalf("expected null queue: %+v", v.Output)
						}
					} else if v.Output == nil {
						t.Fatal("lost queue")
					} else {
						checkQueue(t, *v.Output, m["output"].(map[string]any), version)
					}
				case "cancel", "resume", "cancel_failed":
					var v SuccessResponse
					unmarshalFixture(t, raw, &v)
					if v.Success != (name != "cancel_failed") {
						t.Fatalf("success: %+v", v)
					}
					checkResponseFields(t, v, m)
				default:
					t.Fatalf("unasserted fixture %q", name)
				}
			})
		}
	}
}

func hasKey(m map[string]any, key string) bool { _, ok := m[key]; return ok }

func unmarshalFixture(t *testing.T, raw []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatal(err)
	}
}

func checkWorkflow(t *testing.T, workflow WorkflowsOutput, sdk map[string]any, version string) {
	t.Helper()
	if workflow.WorkflowUUID != "wf-sanitized" || workflow.Status == nil || *workflow.Status != "SUCCESS" || workflow.WasForkedFrom || workflow.CreatedAt == nil || *workflow.CreatedAt != "123" || workflow.Input == nil || *workflow.Input != `["opaque\ninput"]` || workflow.Output == nil || *workflow.Output != `{"opaque":true}` || workflow.Error != nil {
		t.Fatalf("workflow/PascalCase/opaque/null mismatch: %+v", workflow)
	}
	if version == "2.24.0" {
		sdk = withNullFields(sdk, "Attributes", "ScheduleName", "ApplicationName")
	}
	checkKnownFields(t, workflow, sdk)
}

func checkQueue(t *testing.T, queue QueueOutput, sdk map[string]any, version string) {
	t.Helper()
	if queue.Name != "demo-queue" || queue.Concurrency != nil || !queue.PriorityEnabled || queue.PartitionQueue || queue.PollingIntervalSec != 0.5 {
		t.Fatalf("queue/null/default mismatch: %+v", queue)
	}
	if version == "2.24.0" {
		sdk = withNullFields(sdk, "application_name", "partition_concurrency", "partition_worker_concurrency", "partition_rate_limit_max", "partition_rate_limit_period_sec")
	}
	checkKnownFields(t, queue, sdk)
}

// The SDK explicitly emits null error_message; Go omits it when marshaling a
// nil pointer. Its decoded nil value is checked separately above.
func checkResponseFields(t *testing.T, response any, sdk map[string]any) {
	t.Helper()
	value, present := sdk["error_message"]
	if !present || (value != nil && value != "sanitized failure") {
		t.Fatalf("unexpected SDK error_message: %v", value)
	}
	want := make(map[string]any, len(sdk))
	for key, field := range sdk {
		if key != "error_message" || value != nil {
			want[key] = field
		}
	}
	checkKnownFields(t, response, want)
}

// Older SDKs omit the recent fields; the new Go DTO emits explicit nulls.
// Keep every old SDK field checked exactly; do not weaken existing assertions.
func withNullFields(sdk map[string]any, fields ...string) map[string]any {
	want := make(map[string]any, len(sdk)+len(fields))
	for key, value := range sdk {
		want[key] = value
	}
	for _, field := range fields {
		want[field] = nil
	}
	return want
}

func checkKnownFields(t *testing.T, got any, sdk map[string]any) {
	t.Helper()
	want := sdk
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if actual := wireMap(t, encoded); !reflect.DeepEqual(actual, want) {
		t.Fatalf("SDK fields/casing mismatch: got %s; want %v", encoded, want)
	}
}

func TestDecodeUnknownFields(t *testing.T) {
	fixture := sdkFixtures(t, "2.24.0")
	m := wireMap(t, fixture.Responses["get_workflow"])
	m["future_envelope_field"] = "ignored"
	m["output"].(map[string]any)["FutureWorkflowField"] = []any{"ignored"}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var v GetWorkflowResponse
	unmarshalFixture(t, data, &v)
	if v.Output == nil {
		t.Fatal("unknown fields removed known workflow")
	}
	checkWorkflow(t, *v.Output, wireMap(t, fixture.Responses["get_workflow"])["output"].(map[string]any), "2.24.0")
	// Omitted legacy optional fields must decode as nil, not fabricate values.
	delete(m["output"].(map[string]any), "Input")
	delete(m, "error_message")
	data, err = json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var omitted GetWorkflowResponse
	unmarshalFixture(t, data, &omitted)
	if omitted.Output == nil || omitted.Output.Input != nil || omitted.ErrorMessage != nil {
		t.Fatalf("missing optional became non-null: %+v", omitted)
	}
}
