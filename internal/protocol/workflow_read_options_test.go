package protocol

import (
	"reflect"
	"testing"
)

// The field names and defaults below come from the released Python 3.1.0
// ListWorkflowsBody definition and LIST_WORKFLOWS handler. The same handler
// options except attributes/schedule_name are also present in reviewed 2.24.0.
func TestWorkflowReadOptionsWireAndCapabilities(t *testing.T) {
	zero := 0
	f := false
	body := ListWorkflowsBody{
		WorkflowUUIDs:     []string{},
		WorkflowName:      []string{"job"},
		AuthenticatedUser: []string{"alice"},
		StartTime:         "2024-01-01T00:00:00Z", EndTime: "2025-01-01T00:00:00Z",
		CompletedAfter: "2024-02-01T00:00:00Z", CompletedBefore: "2024-12-01T00:00:00Z",
		DequeuedAfter: "2024-03-01T00:00:00Z", DequeuedBefore: "2024-11-01T00:00:00Z",
		Status:           []string{"SUCCESS"},
		ApplicationVer:   []string{"v1"},
		ForkedFrom:       []string{"source"},
		ParentWorkflowID: []string{"parent"},
		QueueName:        []string{"queue"},
		Limit:            &zero, Offset: &zero, SortDesc: false,
		WorkflowIDPrefix: []string{"wf-"},
		LoadInput:        true, LoadOutput: false,
		ExecutorID: []string{"exec"},
		QueuesOnly: false, WasForkedFrom: &f, HasParent: &f,
		Attributes: map[string]any{}, ScheduleName: []string{},
	}
	request := ListWorkflowsRequest(body)
	got := request["body"].(map[string]any)
	want := map[string]any{
		"workflow_uuids": []string{}, "workflow_name": []string{"job"},
		"authenticated_user": []string{"alice"}, "start_time": "2024-01-01T00:00:00Z",
		"end_time": "2025-01-01T00:00:00Z", "completed_after": "2024-02-01T00:00:00Z",
		"completed_before": "2024-12-01T00:00:00Z", "dequeued_after": "2024-03-01T00:00:00Z",
		"dequeued_before": "2024-11-01T00:00:00Z", "status": []string{"SUCCESS"},
		"application_version": []string{"v1"}, "forked_from": []string{"source"},
		"parent_workflow_id": []string{"parent"}, "queue_name": []string{"queue"},
		"limit": 0, "offset": 0, "sort_desc": false, "workflow_id_prefix": []string{"wf-"},
		"load_input": true, "load_output": false, "executor_id": []string{"exec"},
		"queues_only": false, "was_forked_from": false, "has_parent": false,
		"attributes": map[string]any{}, "schedule_name": []string{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("workflow options wire = %#v; want %#v", got, want)
	}
}
