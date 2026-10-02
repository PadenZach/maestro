package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

// This file mirrors the Python 3.1.0 inspection-read definitions and the
// matching ConductorWebsocket dispatch branches. Request bodies retain omitted,
// explicit false/zero, and empty collection values; response measures use
// pointers so a wire null is never changed into zero.

// ListSchedulesBody carries the optional LIST_SCHEDULES filters.
type ListSchedulesBody struct {
	Status             []string
	WorkflowName       []string
	ScheduleNamePrefix []string
	ApplicationName    []string
	LoadContext        *bool
}

func (b ListSchedulesBody) toMap() map[string]any {
	body := make(map[string]any)
	putInspectionStrings(body, "status", b.Status)
	putInspectionStrings(body, "workflow_name", b.WorkflowName)
	putInspectionStrings(body, "schedule_name_prefix", b.ScheduleNamePrefix)
	putInspectionStrings(body, "application_name", b.ApplicationName)
	putInspectionValue(body, "load_context", b.LoadContext)
	return body
}

// ListSchedulesRequest builds a LIST_SCHEDULES frame.
func ListSchedulesRequest(body ListSchedulesBody) Request {
	request := NewRequest(MsgListSchedules)
	request["body"] = body.toMap()
	return request
}

// GetScheduleRequest builds a GET_SCHEDULE frame. The released SDK defaults
// load_context to true; the public read has no alternate argument.
func GetScheduleRequest(name string) Request {
	request := NewRequest(MsgGetSchedule)
	request["schedule_name"] = name
	request["load_context"] = true
	return request
}

// WorkflowAggregatesBody carries every optional workflow aggregate grouping,
// selection, and filter field accepted by the reviewed SDK handler.
type WorkflowAggregatesBody struct {
	GroupByStatus             *bool
	GroupByName               *bool
	GroupByQueueName          *bool
	GroupByExecutorID         *bool
	GroupByApplicationVersion *bool
	GroupByApplicationName    *bool
	SelectCount               *bool
	SelectMinCreatedAt        *bool
	SelectMaxQueueWaitMS      *bool
	SelectMaxTotalLatencyMS   *bool
	TimeBucketSizeMS          *int64
	Status                    []string
	StartTime                 *string
	EndTime                   *string
	CompletedAfter            *string
	CompletedBefore           *string
	DequeuedAfter             *string
	DequeuedBefore            *string
	Name                      []string
	AppVersion                []string
	ExecutorID                []string
	QueueName                 []string
	WorkflowIDPrefix          []string
	WorkflowIDs               []string
	ForkedFrom                []string
	ParentWorkflowID          []string
	User                      []string
	ScheduleName              []string
	ApplicationName           []string
	WasForkedFrom             *bool
	HasParent                 *bool
	Attributes                map[string]any
}

func (b WorkflowAggregatesBody) toMap() map[string]any {
	body := make(map[string]any)
	putInspectionValue(body, "group_by_status", b.GroupByStatus)
	putInspectionValue(body, "group_by_name", b.GroupByName)
	putInspectionValue(body, "group_by_queue_name", b.GroupByQueueName)
	putInspectionValue(body, "group_by_executor_id", b.GroupByExecutorID)
	putInspectionValue(body, "group_by_application_version", b.GroupByApplicationVersion)
	putInspectionValue(body, "group_by_application_name", b.GroupByApplicationName)
	putInspectionValue(body, "select_count", b.SelectCount)
	putInspectionValue(body, "select_min_created_at", b.SelectMinCreatedAt)
	putInspectionValue(body, "select_max_queue_wait_ms", b.SelectMaxQueueWaitMS)
	putInspectionValue(body, "select_max_total_latency_ms", b.SelectMaxTotalLatencyMS)
	putInspectionValue(body, "time_bucket_size_ms", b.TimeBucketSizeMS)
	putInspectionStrings(body, "status", b.Status)
	putInspectionValue(body, "start_time", b.StartTime)
	putInspectionValue(body, "end_time", b.EndTime)
	putInspectionValue(body, "completed_after", b.CompletedAfter)
	putInspectionValue(body, "completed_before", b.CompletedBefore)
	putInspectionValue(body, "dequeued_after", b.DequeuedAfter)
	putInspectionValue(body, "dequeued_before", b.DequeuedBefore)
	putInspectionStrings(body, "name", b.Name)
	putInspectionStrings(body, "app_version", b.AppVersion)
	putInspectionStrings(body, "executor_id", b.ExecutorID)
	putInspectionStrings(body, "queue_name", b.QueueName)
	putInspectionStrings(body, "workflow_id_prefix", b.WorkflowIDPrefix)
	putInspectionStrings(body, "workflow_ids", b.WorkflowIDs)
	putInspectionStrings(body, "forked_from", b.ForkedFrom)
	putInspectionStrings(body, "parent_workflow_id", b.ParentWorkflowID)
	putInspectionStrings(body, "user", b.User)
	putInspectionStrings(body, "schedule_name", b.ScheduleName)
	putInspectionStrings(body, "application_name", b.ApplicationName)
	putInspectionValue(body, "was_forked_from", b.WasForkedFrom)
	putInspectionValue(body, "has_parent", b.HasParent)
	if b.Attributes != nil {
		body["attributes"] = b.Attributes
	}
	return body
}

// GetWorkflowAggregatesRequest builds a GET_WORKFLOW_AGGREGATES frame.
func GetWorkflowAggregatesRequest(body WorkflowAggregatesBody) Request {
	request := NewRequest(MsgGetWorkflowAggregates)
	request["body"] = body.toMap()
	return request
}

// StepAggregatesBody carries every optional step aggregate grouping, selection,
// and filter field accepted by the reviewed SDK handler.
type StepAggregatesBody struct {
	GroupByFunctionName *bool
	GroupByStatus       *bool
	SelectCount         *bool
	SelectMaxDurationMS *bool
	TimeBucketSizeMS    *int64
	Status              []string
	FunctionName        []string
	WorkflowIDPrefix    []string
	CompletedAfter      *string
	CompletedBefore     *string
	ApplicationName     []string
}

func (b StepAggregatesBody) toMap() map[string]any {
	body := make(map[string]any)
	putInspectionValue(body, "group_by_function_name", b.GroupByFunctionName)
	putInspectionValue(body, "group_by_status", b.GroupByStatus)
	putInspectionValue(body, "select_count", b.SelectCount)
	putInspectionValue(body, "select_max_duration_ms", b.SelectMaxDurationMS)
	putInspectionValue(body, "time_bucket_size_ms", b.TimeBucketSizeMS)
	putInspectionStrings(body, "status", b.Status)
	putInspectionStrings(body, "function_name", b.FunctionName)
	putInspectionStrings(body, "workflow_id_prefix", b.WorkflowIDPrefix)
	putInspectionValue(body, "completed_after", b.CompletedAfter)
	putInspectionValue(body, "completed_before", b.CompletedBefore)
	putInspectionStrings(body, "application_name", b.ApplicationName)
	return body
}

// GetStepAggregatesRequest builds a GET_STEP_AGGREGATES frame.
func GetStepAggregatesRequest(body StepAggregatesBody) Request {
	request := NewRequest(MsgGetStepAggregates)
	request["body"] = body.toMap()
	return request
}

// ExportWorkflowRequest builds an EXPORT_WORKFLOW frame. The response remains
// an opaque SDK-serialized string; maestro must never decode or execute it.
func ExportWorkflowRequest(workflowID string, exportChildren bool) Request {
	request := NewRequest(MsgExportWorkflow)
	request["workflow_id"] = workflowID
	request["export_children"] = exportChildren
	return request
}

func putInspectionValue[T any](body map[string]any, name string, value *T) {
	if value != nil {
		body[name] = *value
	}
}

func putInspectionStrings(body map[string]any, name string, value []string) {
	if value != nil {
		body[name] = value
	}
}

// ScheduleOutput is the released SDK schedule wire representation. QueueName is
// SDK-only in the pinned HTTP snapshot but is retained for later local UI use.
type ScheduleOutput struct {
	hasRequiredFields bool
	ScheduleID        string  `json:"schedule_id"`
	ScheduleName      string  `json:"schedule_name"`
	WorkflowName      string  `json:"workflow_name"`
	WorkflowClassName *string `json:"workflow_class_name"`
	Schedule          string  `json:"schedule"`
	Status            string  `json:"status"`
	Context           *string `json:"context"`
	LastFiredAt       *string `json:"last_fired_at"`
	AutomaticBackfill bool    `json:"automatic_backfill"`
	CronTimezone      *string `json:"cron_timezone"`
	QueueName         *string `json:"queue_name"`
	ApplicationName   *string `json:"application_name"`
}

// UnmarshalJSON retains presence of all pinned SDK schedule keys while allowing
// required nullable keys to contain null and automatic_backfill to be false.
func (s *ScheduleOutput) UnmarshalJSON(data []byte) error {
	type fields ScheduleOutput
	var decoded fields
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	present, err := decodeFieldPresence(data)
	if err != nil {
		return err
	}
	allFieldsPresent := true
	for _, name := range [...]string{
		"schedule_id", "schedule_name", "workflow_name", "workflow_class_name",
		"schedule", "status", "context", "last_fired_at", "automatic_backfill",
		"cron_timezone", "queue_name", "application_name",
	} {
		if _, ok := present[name]; !ok {
			allFieldsPresent = false
			break
		}
	}
	*s = ScheduleOutput(decoded)
	s.hasRequiredFields = allFieldsPresent &&
		present["schedule_id"] &&
		present["schedule_name"] &&
		present["workflow_name"] &&
		present["schedule"] &&
		present["status"] &&
		present["automatic_backfill"]
	return nil
}

// HasRequiredFields reports whether all pinned SDK schedule keys were present
// and every nonnullable field was nonnull when decoded.
func (s ScheduleOutput) HasRequiredFields() bool { return s.hasRequiredFields }

// WorkflowAggregate is one released SDK workflow aggregate row. Nullable
// measures are pointers so explicit zero remains distinct from null.
type WorkflowAggregate struct {
	Group             map[string]*string `json:"group"`
	Count             *int64             `json:"count"`
	MinCreatedAt      *int64             `json:"min_created_at"`
	MaxQueueWaitMS    *int64             `json:"max_queue_wait_ms"`
	MaxTotalLatencyMS *int64             `json:"max_total_latency_ms"`
}

// StepAggregate is one released SDK step aggregate row.
type StepAggregate struct {
	Group         map[string]*string `json:"group"`
	Count         *int64             `json:"count"`
	MaxDurationMS *int64             `json:"max_duration_ms"`
}

// ListSchedulesResponse answers LIST_SCHEDULES.
type ListSchedulesResponse struct {
	BaseResponse
	Output []ScheduleOutput `json:"output"`
}

// GetScheduleResponse answers GET_SCHEDULE (output is null when not found).
type GetScheduleResponse struct {
	BaseResponse
	Output *ScheduleOutput `json:"output"`
}

// GetWorkflowAggregatesResponse answers GET_WORKFLOW_AGGREGATES.
type GetWorkflowAggregatesResponse struct {
	BaseResponse
	Output []WorkflowAggregate `json:"output"`
}

// GetStepAggregatesResponse answers GET_STEP_AGGREGATES.
type GetStepAggregatesResponse struct {
	BaseResponse
	Output []StepAggregate `json:"output"`
}

// ExportWorkflowResponse answers EXPORT_WORKFLOW. SerializedWorkflow is opaque
// SDK output and may be null when the executor reports an error.
type ExportWorkflowResponse struct {
	BaseResponse
	SerializedWorkflow *string `json:"serialized_workflow"`
}

// Validate checks the fields required to display a schedule.
func (s ScheduleOutput) Validate() error {
	if !s.HasRequiredFields() {
		return fmt.Errorf("schedule missing required SDK fields")
	}
	if s.LastFiredAt != nil {
		if _, err := time.Parse(time.RFC3339Nano, *s.LastFiredAt); err != nil {
			return fmt.Errorf("schedule last_fired_at must be RFC3339: %w", err)
		}
	}
	return nil
}
