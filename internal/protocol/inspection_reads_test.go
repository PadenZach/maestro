package protocol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func inspectionReadFixtures(t *testing.T) sdkWire {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "3.1.0", "inspection_reads.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures sdkWire
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	return fixtures
}

func TestInspectionReadRequestWireMatchesPythonSDK310(t *testing.T) {
	falseValue, trueValue := false, true
	zero := int64(0)
	start := "2026-01-01T00:00:00Z"
	end := "2026-01-02T00:00:00Z"
	completedAfter := "2026-01-03T00:00:00Z"
	completedBefore := "2026-01-04T00:00:00Z"
	dequeuedAfter := "2026-01-05T00:00:00Z"
	dequeuedBefore := "2026-01-06T00:00:00Z"
	stepCompletedAfter := "2026-02-01T00:00:00Z"
	stepCompletedBefore := "2026-02-02T00:00:00Z"

	workflowBody := WorkflowAggregatesBody{
		GroupByStatus:             &falseValue,
		GroupByName:               &trueValue,
		GroupByQueueName:          &falseValue,
		GroupByExecutorID:         &trueValue,
		GroupByApplicationVersion: &falseValue,
		GroupByApplicationName:    &trueValue,
		SelectCount:               &trueValue,
		SelectMinCreatedAt:        &falseValue,
		SelectMaxQueueWaitMS:      &trueValue,
		SelectMaxTotalLatencyMS:   &falseValue,
		TimeBucketSizeMS:          &zero,
		Status:                    []string{},
		StartTime:                 &start,
		EndTime:                   &end,
		CompletedAfter:            &completedAfter,
		CompletedBefore:           &completedBefore,
		DequeuedAfter:             &dequeuedAfter,
		DequeuedBefore:            &dequeuedBefore,
		Name:                      []string{"demo_workflow"},
		AppVersion:                []string{"app-v1"},
		ExecutorID:                []string{"exec-sanitized"},
		QueueName:                 []string{"demo-queue"},
		WorkflowIDPrefix:          []string{"wf-"},
		WorkflowIDs:               []string{"wf-sanitized"},
		ForkedFrom:                []string{"source-wf"},
		ParentWorkflowID:          []string{"parent-wf"},
		User:                      []string{"user-sanitized"},
		ScheduleName:              []string{"nightly-sanitized"},
		ApplicationName:           []string{"app-sanitized"},
		WasForkedFrom:             &falseValue,
		HasParent:                 &falseValue,
		Attributes:                map[string]any{},
	}
	stepBody := StepAggregatesBody{
		GroupByFunctionName: &falseValue,
		GroupByStatus:       &trueValue,
		SelectCount:         &falseValue,
		SelectMaxDurationMS: &trueValue,
		TimeBucketSizeMS:    &zero,
		Status:              []string{},
		FunctionName:        []string{"demo_step"},
		WorkflowIDPrefix:    []string{"wf-"},
		CompletedAfter:      &stepCompletedAfter,
		CompletedBefore:     &stepCompletedBefore,
		ApplicationName:     []string{"app-sanitized"},
	}
	requests := map[string]Request{
		"list_schedules": ListSchedulesRequest(ListSchedulesBody{}),
		"list_schedules_all_fields": ListSchedulesRequest(ListSchedulesBody{
			Status:             []string{"ACTIVE"},
			WorkflowName:       []string{"demo_workflow"},
			ScheduleNamePrefix: []string{"nightly"},
			ApplicationName:    []string{"app-sanitized"},
			LoadContext:        &falseValue,
		}),
		"get_schedule":                       GetScheduleRequest("nightly-sanitized"),
		"get_workflow_aggregates":            GetWorkflowAggregatesRequest(WorkflowAggregatesBody{}),
		"get_workflow_aggregates_all_fields": GetWorkflowAggregatesRequest(workflowBody),
		"get_step_aggregates":                GetStepAggregatesRequest(StepAggregatesBody{}),
		"get_step_aggregates_all_fields":     GetStepAggregatesRequest(stepBody),
		"export_workflow":                    ExportWorkflowRequest("wf-sanitized", false),
		"export_workflow_children":           ExportWorkflowRequest("wf-sanitized", true),
	}

	fixtures := inspectionReadFixtures(t)
	if len(fixtures.Requests) != len(requests) {
		t.Fatalf("fixture requests = %d; builders = %d", len(fixtures.Requests), len(requests))
	}
	for name, request := range requests {
		t.Run(name, func(t *testing.T) {
			request["request_id"] = "req-sanitized"
			checkWire(t, request, fixtures.Requests[name])
		})
	}
}

func TestInspectionReadResponsesPreserveSDKFieldsNullsAndOpaqueExport(t *testing.T) {
	fixtures := inspectionReadFixtures(t)
	for name, raw := range fixtures.Responses {
		t.Run(name, func(t *testing.T) {
			sdk := wireMap(t, raw)
			switch name {
			case "list_schedules":
				var response ListSchedulesResponse
				unmarshalFixture(t, raw, &response)
				if len(response.Output) != 1 {
					t.Fatalf("schedule count: %d", len(response.Output))
				}
				checkScheduleOutput(t, response.Output[0], sdk["output"].([]any)[0].(map[string]any))
				checkResponseFields(t, response, sdk)
			case "get_schedule", "get_schedule_missing":
				var response GetScheduleResponse
				unmarshalFixture(t, raw, &response)
				if name == "get_schedule_missing" {
					if response.Output != nil {
						t.Fatalf("expected null schedule: %+v", response.Output)
					}
				} else if response.Output == nil {
					t.Fatal("lost schedule")
				} else {
					checkScheduleOutput(t, *response.Output, sdk["output"].(map[string]any))
				}
				checkResponseFields(t, response, sdk)
			case "get_workflow_aggregates":
				var response GetWorkflowAggregatesResponse
				unmarshalFixture(t, raw, &response)
				if len(response.Output) != 1 {
					t.Fatalf("workflow aggregate count: %d", len(response.Output))
				}
				aggregate := response.Output[0]
				if aggregate.Count == nil || *aggregate.Count != 0 || aggregate.MaxQueueWaitMS == nil || *aggregate.MaxQueueWaitMS != 0 {
					t.Fatalf("explicit zero aggregate measures lost: %+v", aggregate)
				}
				if aggregate.MinCreatedAt != nil || aggregate.MaxTotalLatencyMS != nil || aggregate.Group["queue_name"] != nil {
					t.Fatalf("nullable aggregate values fabricated: %+v", aggregate)
				}
				checkKnownFields(t, aggregate, sdk["output"].([]any)[0].(map[string]any))
				checkResponseFields(t, response, sdk)
			case "get_step_aggregates":
				var response GetStepAggregatesResponse
				unmarshalFixture(t, raw, &response)
				if len(response.Output) != 1 {
					t.Fatalf("step aggregate count: %d", len(response.Output))
				}
				aggregate := response.Output[0]
				if aggregate.Count == nil || *aggregate.Count != 0 || aggregate.MaxDurationMS != nil || aggregate.Group["status"] != nil {
					t.Fatalf("step aggregate zero/null mismatch: %+v", aggregate)
				}
				checkKnownFields(t, aggregate, sdk["output"].([]any)[0].(map[string]any))
				checkResponseFields(t, response, sdk)
			case "export_workflow", "export_workflow_missing":
				var response ExportWorkflowResponse
				unmarshalFixture(t, raw, &response)
				if name == "export_workflow_missing" {
					if response.SerializedWorkflow != nil {
						t.Fatalf("expected null serialized workflow: %q", *response.SerializedWorkflow)
					}
				} else {
					const opaque = "H4sIAOpaqueSDKSerializedData==\nnot-decoded"
					if response.SerializedWorkflow == nil || *response.SerializedWorkflow != opaque {
						t.Fatalf("opaque SDK export changed: %+v", response.SerializedWorkflow)
					}
				}
				checkResponseFields(t, response, sdk)
			default:
				t.Fatalf("unasserted inspection response fixture %q", name)
			}
		})
	}
}

func TestScheduleOutputTracksEveryRequiredSDKField(t *testing.T) {
	fixture := inspectionReadFixtures(t)
	base := wireMap(t, fixture.Responses["get_schedule"])["output"].(map[string]any)
	allFields := []string{
		"schedule_id", "schedule_name", "workflow_name", "workflow_class_name",
		"schedule", "status", "context", "last_fired_at", "automatic_backfill",
		"cron_timezone", "queue_name", "application_name",
	}
	nonNullableFields := []string{
		"schedule_id", "schedule_name", "workflow_name", "schedule", "status",
		"automatic_backfill",
	}
	nullableFields := []string{
		"workflow_class_name", "context", "last_fired_at", "cron_timezone",
		"queue_name", "application_name",
	}

	decode := func(t *testing.T, fields map[string]any) ScheduleOutput {
		t.Helper()
		data, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		var schedule ScheduleOutput
		if err := json.Unmarshal(data, &schedule); err != nil {
			t.Fatal(err)
		}
		return schedule
	}
	clone := func() map[string]any {
		fields := make(map[string]any, len(base))
		for name, value := range base {
			fields[name] = value
		}
		return fields
	}

	if schedule := decode(t, clone()); !schedule.HasRequiredFields() || schedule.AutomaticBackfill {
		t.Fatalf("complete SDK schedule with explicit false rejected: %+v", schedule)
	}

	for _, missing := range allFields {
		t.Run("missing_"+missing, func(t *testing.T) {
			fields := clone()
			delete(fields, missing)
			if schedule := decode(t, fields); schedule.HasRequiredFields() {
				t.Fatalf("missing %s accepted: %+v", missing, schedule)
			}
		})
	}
	for _, nullable := range nullableFields {
		t.Run("nullable_null_"+nullable, func(t *testing.T) {
			fields := clone()
			fields[nullable] = nil
			if schedule := decode(t, fields); !schedule.HasRequiredFields() {
				t.Fatalf("required nullable %s=null rejected: %+v", nullable, schedule)
			}
		})
		t.Run("nullable_empty_"+nullable, func(t *testing.T) {
			fields := clone()
			fields[nullable] = ""
			if schedule := decode(t, fields); !schedule.HasRequiredFields() {
				t.Fatalf("required nullable %s empty string rejected: %+v", nullable, schedule)
			}
		})
	}
	for _, nonNullable := range nonNullableFields {
		t.Run("nonnull_null_"+nonNullable, func(t *testing.T) {
			fields := clone()
			fields[nonNullable] = nil
			if schedule := decode(t, fields); schedule.HasRequiredFields() {
				t.Fatalf("nonnullable %s=null accepted: %+v", nonNullable, schedule)
			}
		})
	}

	emptyValues := clone()
	for _, name := range nonNullableFields {
		if name != "automatic_backfill" {
			emptyValues[name] = ""
		}
	}
	emptyValues["automatic_backfill"] = false
	if schedule := decode(t, emptyValues); !schedule.HasRequiredFields() || schedule.AutomaticBackfill {
		t.Fatalf("present empty strings or explicit false rejected: %+v", schedule)
	}
}

func checkScheduleOutput(t *testing.T, schedule ScheduleOutput, sdk map[string]any) {
	t.Helper()
	if schedule.ScheduleID != "schedule-id-sanitized" || schedule.ScheduleName != "nightly-sanitized" || schedule.WorkflowName != "demo_workflow" || schedule.WorkflowClassName != nil || schedule.Context != nil || schedule.LastFiredAt == nil || *schedule.LastFiredAt != "2026-03-01T00:00:00Z" || schedule.AutomaticBackfill || schedule.CronTimezone != nil || schedule.QueueName == nil || *schedule.QueueName != "demo-queue" || schedule.ApplicationName == nil || *schedule.ApplicationName != "app-sanitized" {
		t.Fatalf("schedule fields/nulls/queue mismatch: %+v", schedule)
	}
	checkKnownFields(t, schedule, sdk)
}
