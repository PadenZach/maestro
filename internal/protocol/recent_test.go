package protocol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// recent.json is serialized by hash-verified SDK protocol.py, with the chosen
// request shapes cross-checked against the pinned conductor.py handler branches.
func recentWire(t *testing.T, version string) sdkWire {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", version, "recent.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture sdkWire
	if err := json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func TestRecentWorkflowFields(t *testing.T) {
	for _, version := range []string{"2.31.1", "3.1.0"} {
		t.Run(version, func(t *testing.T) {
			raw := recentWire(t, version).Responses["get_workflow_recent"]
			var got GetWorkflowResponse
			unmarshalFixture(t, raw, &got)
			if got.Output == nil {
				t.Fatal("lost workflow")
			}
			output := wireMap(t, raw)["output"].(map[string]any)
			encoded, err := json.Marshal(got.Output)
			if err != nil {
				t.Fatal(err)
			}
			if actual := wireMap(t, encoded); !reflect.DeepEqual(actual, output) {
				t.Fatalf("new PascalCase/string attributes lost: got %s; want %v", encoded, output)
			}
			// Legacy omission and explicit null must not invent values.
			for _, old := range []json.RawMessage{sdkFixtures(t, "2.24.0").Responses["get_workflow"], sdkFixtures(t, version).Responses["get_workflow"]} {
				var oldGot GetWorkflowResponse
				unmarshalFixture(t, old, &oldGot)
				b, _ := json.Marshal(oldGot.Output)
				m := wireMap(t, b)
				for _, key := range []string{"Attributes", "ScheduleName", "ApplicationName"} {
					if m[key] != nil {
						t.Fatalf("legacy %s invented: %v", key, m[key])
					}
				}
			}
		})
	}
}

func TestRecentQueueFields(t *testing.T) {
	for _, version := range []string{"2.31.1", "3.1.0"} {
		t.Run(version, func(t *testing.T) {
			raw := recentWire(t, version).Responses["get_queue_recent"]
			var got GetQueueResponse
			unmarshalFixture(t, raw, &got)
			if got.Output == nil {
				t.Fatal("lost queue")
			}
			output := wireMap(t, raw)["output"].(map[string]any)
			encoded, err := json.Marshal(got.Output)
			if err != nil {
				t.Fatal(err)
			}
			if actual := wireMap(t, encoded); !reflect.DeepEqual(actual, output) {
				t.Fatalf("new queue limits lost: got %s; want %v", encoded, output)
			}
			for _, old := range []json.RawMessage{sdkFixtures(t, "2.24.0").Responses["get_queue"], sdkFixtures(t, version).Responses["get_queue"]} {
				var oldGot GetQueueResponse
				unmarshalFixture(t, old, &oldGot)
				b, _ := json.Marshal(oldGot.Output)
				m := wireMap(t, b)
				for _, key := range []string{"application_name", "partition_concurrency", "partition_worker_concurrency", "partition_rate_limit_max", "partition_rate_limit_period_sec"} {
					if m[key] != nil {
						t.Fatalf("legacy %s invented: %v", key, m[key])
					}
				}
			}
		})
	}
}

func TestRecentFilterWire(t *testing.T) {
	for _, version := range []string{"2.31.1", "3.1.0"} {
		t.Run(version, func(t *testing.T) {
			fixture := recentWire(t, version)
			var body ListWorkflowsBody
			// Decode the SDK-derived filter body so this test compiles before the
			// newer typed fields exist; the exact outbound frame must still match.
			raw := wireMap(t, fixture.Requests["list_workflows_recent"])["body"]
			b, _ := json.Marshal(raw)
			if err := json.Unmarshal(b, &body); err != nil {
				t.Fatal(err)
			}
			for name, req := range map[string]Request{
				"list_workflows_recent":        ListWorkflowsRequest(body),
				"list_queued_workflows_recent": ListQueuedWorkflowsRequest(body),
			} {
				req["request_id"] = "req-sanitized"
				checkWire(t, req, fixture.Requests[name])
			}
			// Existing no-body form remains byte-for-byte unchanged.
			legacy := ListQueuesRequest()
			legacy["request_id"] = "req-sanitized"
			checkWire(t, legacy, sdkFixtures(t, version).Requests["list_queues"])
			// The optional filtered form must be a typed builder, not an open-map escape.
			builder := reflect.ValueOf(ListQueuesRequest)
			if builder.Type().NumIn() != 1 || !builder.Type().IsVariadic() {
				t.Fatal("no optional typed list-queues body")
			}
			arg := reflect.New(builder.Type().In(0).Elem())
			b, _ = json.Marshal(wireMap(t, fixture.Requests["list_queues_recent"])["body"])
			if err := json.Unmarshal(b, arg.Interface()); err != nil {
				t.Fatal(err)
			}
			req := builder.Call([]reflect.Value{arg.Elem()})[0].Interface().(Request)
			req["request_id"] = "req-sanitized"
			checkWire(t, req, fixture.Requests["list_queues_recent"])
		})
	}
}
