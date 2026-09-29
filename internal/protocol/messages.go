package protocol

import (
	"encoding/json"
	"errors"
)

// This file holds the observability-read and basic-management data-transfer
// objects and response envelopes. They mirror dbos/_conductor/protocol.py field
// names and casing EXACTLY — the Python client (and every other SDK) emits these
// keys, so any divergence silently drops data.
//
// Casing note: WorkflowsOutput uses PascalCase keys (it doubles as the executor's
// admin-server shape); every other DTO is snake_case. Optional fields are
// pointers so a JSON `null` round-trips as nil rather than a zero value.

// WorkflowsOutput is one workflow's full status row. All values are stringified
// (or null) on the wire; WasForkedFrom is the one real bool. Mirrors
// protocol.py:WorkflowsOutput (PascalCase keys are wire-significant).
type WorkflowsOutput struct {
	WorkflowUUID            string  `json:"WorkflowUUID"`
	Status                  *string `json:"Status"`
	WorkflowName            *string `json:"WorkflowName"`
	WorkflowClassName       *string `json:"WorkflowClassName"`
	WorkflowConfigName      *string `json:"WorkflowConfigName"`
	AuthenticatedUser       *string `json:"AuthenticatedUser"`
	AssumedRole             *string `json:"AssumedRole"`
	AuthenticatedRoles      *string `json:"AuthenticatedRoles"`
	Input                   *string `json:"Input"`
	Output                  *string `json:"Output"`
	Error                   *string `json:"Error"`
	CreatedAt               *string `json:"CreatedAt"`
	UpdatedAt               *string `json:"UpdatedAt"`
	QueueName               *string `json:"QueueName"`
	ApplicationVersion      *string `json:"ApplicationVersion"`
	ExecutorID              *string `json:"ExecutorID"`
	WorkflowTimeoutMS       *string `json:"WorkflowTimeoutMS"`
	WorkflowDeadlineEpochMS *string `json:"WorkflowDeadlineEpochMS"`
	DeduplicationID         *string `json:"DeduplicationID"`
	Priority                *string `json:"Priority"`
	QueuePartitionKey       *string `json:"QueuePartitionKey"`
	ForkedFrom              *string `json:"ForkedFrom"`
	WasForkedFrom           bool    `json:"WasForkedFrom"`
	hasWasForkedFrom        bool
	ParentWorkflowID        *string `json:"ParentWorkflowID"`
	DequeuedAt              *string `json:"DequeuedAt"`
	DelayUntilEpochMS       *string `json:"DelayUntilEpochMS"`
	CompletedAt             *string `json:"CompletedAt"`
	Attributes              *string `json:"Attributes"` // JSON-encoded SDK string, opaque to maestro
	ScheduleName            *string `json:"ScheduleName"`
	ApplicationName         *string `json:"ApplicationName"`
}

// WorkflowSteps is one operation within a workflow, used by the step visualizer.
// child_workflow_id, when set, makes the timeline navigable into a sub-workflow.
// Mirrors protocol.py:WorkflowSteps.
type WorkflowSteps struct {
	FunctionID         int `json:"function_id"`
	hasFunctionID      bool
	FunctionName       string  `json:"function_name"`
	Output             *string `json:"output"`
	Error              *string `json:"error"`
	ChildWorkflowID    *string `json:"child_workflow_id"`
	StartedAtEpochMS   *string `json:"started_at_epoch_ms"`
	CompletedAtEpochMS *string `json:"completed_at_epoch_ms"`
}

// Presence is only needed by the strict local HTTP v2 mapper. Keep existing wire and
// /api JSON fields and zero-value behavior unchanged for other consumers.
func (w *WorkflowsOutput) UnmarshalJSON(data []byte) error {
	type fields WorkflowsOutput
	var decoded fields
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var required struct {
		WasForkedFrom *bool `json:"WasForkedFrom"`
	}
	if err := json.Unmarshal(data, &required); err != nil {
		return err
	}
	*w = WorkflowsOutput(decoded)
	w.hasWasForkedFrom = required.WasForkedFrom != nil
	return nil
}

func (w WorkflowsOutput) HasWasForkedFrom() bool { return w.hasWasForkedFrom }

func (s *WorkflowSteps) UnmarshalJSON(data []byte) error {
	type fields WorkflowSteps
	var decoded fields
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var required struct {
		FunctionID *int `json:"function_id"`
	}
	if err := json.Unmarshal(data, &required); err != nil {
		return err
	}
	*s = WorkflowSteps(decoded)
	s.hasFunctionID = required.FunctionID != nil
	return nil
}

func (s WorkflowSteps) HasFunctionID() bool { return s.hasFunctionID }

// QueueOutput is a queue's configuration. Mirrors protocol.py:QueueOutput.
type QueueOutput struct {
	Name                        string   `json:"name"`
	Concurrency                 *int     `json:"concurrency"`
	WorkerConcurrency           *int     `json:"worker_concurrency"`
	RateLimitMax                *int     `json:"rate_limit_max"`
	RateLimitPeriodSec          *float64 `json:"rate_limit_period_sec"`
	PriorityEnabled             bool     `json:"priority_enabled"`
	PartitionQueue              bool     `json:"partition_queue"`
	PollingIntervalSec          float64  `json:"polling_interval_sec"`
	ApplicationName             *string  `json:"application_name"`
	PartitionConcurrency        *int     `json:"partition_concurrency"`
	PartitionWorkerConcurrency  *int     `json:"partition_worker_concurrency"`
	PartitionRateLimitMax       *int     `json:"partition_rate_limit_max"`
	PartitionRateLimitPeriodSec *float64 `json:"partition_rate_limit_period_sec"`
}

// EventOutput is one set_event key/value pair. Mirrors protocol.py:EventOutput.
type EventOutput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NotificationOutput is one send/recv message. Mirrors protocol.py:NotificationOutput.
type NotificationOutput struct {
	Topic            *string `json:"topic"`
	Message          string  `json:"message"`
	CreatedAtEpochMS int64   `json:"created_at_epoch_ms"`
	Consumed         bool    `json:"consumed"`
}

// StreamEntryOutput is one stream key with its ordered values. Mirrors
// protocol.py:StreamEntryOutput.
type StreamEntryOutput struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// --- Response envelopes -----------------------------------------------------
//
// Every response echoes type + request_id (already routed by the read loop) and
// carries an optional error_message. We embed BaseResponse for the common
// fields and add the payload field each handler returns.

// ListWorkflowsResponse answers LIST_WORKFLOWS and LIST_QUEUED_WORKFLOWS.
type ListWorkflowsResponse struct {
	BaseResponse
	Output []WorkflowsOutput `json:"output"`
}

// GetWorkflowResponse answers GET_WORKFLOW (output is null when not found).
type GetWorkflowResponse struct {
	BaseResponse
	Output *WorkflowsOutput `json:"output"`
}

// ListStepsResponse answers LIST_STEPS.
type ListStepsResponse struct {
	BaseResponse
	Output []WorkflowSteps `json:"output"`
}

// GetWorkflowEventsResponse answers GET_WORKFLOW_EVENTS.
type GetWorkflowEventsResponse struct {
	BaseResponse
	Events []EventOutput `json:"events"`
}

// GetWorkflowNotificationsResponse answers GET_WORKFLOW_NOTIFICATIONS.
type GetWorkflowNotificationsResponse struct {
	BaseResponse
	Notifications []NotificationOutput `json:"notifications"`
}

// GetWorkflowStreamsResponse answers GET_WORKFLOW_STREAMS.
type GetWorkflowStreamsResponse struct {
	BaseResponse
	Streams []StreamEntryOutput `json:"streams"`
}

// ListQueuesResponse answers LIST_QUEUES.
type ListQueuesResponse struct {
	BaseResponse
	Output []QueueOutput `json:"output"`
}

// GetQueueResponse answers GET_QUEUE (output is null when not found).
type GetQueueResponse struct {
	BaseResponse
	Output *QueueOutput `json:"output"`
}

// SuccessResponse answers mutating commands (CANCEL, RESUME) that report only
// whether the operation succeeded.
type SuccessResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

// Success is required by the pinned cancel/resume response definitions. A
// missing or false success is never an acknowledgment, even without a message.
func (r SuccessResponse) Err() error {
	if err := r.BaseResponse.Err(); err != nil {
		return err
	}
	if !r.Success {
		return errors.New("executor reported unsuccessful command")
	}
	return nil
}
