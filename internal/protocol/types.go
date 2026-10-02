// Package protocol defines the JSON-over-WebSocket wire contract that
// conductor speaks with DBOS executors. It mirrors the Python client's
// dbos/_conductor/protocol.py exactly so existing DBOS apps connect unchanged.
//
// The protocol is server-initiated request/response: conductor PUSHES a
// request ({type, request_id, ...}); the executor only ever RESPONDS, echoing
// the request_id and carrying an optional error_message. Every inbound frame
// on a connection is therefore a response to a request we sent.
package protocol

import "errors"

// MessageType is the discriminator carried in every frame's "type" field.
type MessageType string

const (
	MsgExecutorInfo             MessageType = "executor_info"
	MsgRecovery                 MessageType = "recovery"
	MsgCancel                   MessageType = "cancel"
	MsgListWorkflows            MessageType = "list_workflows"
	MsgListQueuedWorkflows      MessageType = "list_queued_workflows"
	MsgResume                   MessageType = "resume"
	MsgGetWorkflow              MessageType = "get_workflow"
	MsgExistPendingWorkflows    MessageType = "exist_pending_workflows"
	MsgListSteps                MessageType = "list_steps"
	MsgForkWorkflow             MessageType = "fork_workflow"
	MsgGetMetrics               MessageType = "get_metrics"
	MsgExportWorkflow           MessageType = "export_workflow"
	MsgListSchedules            MessageType = "list_schedules"
	MsgGetSchedule              MessageType = "get_schedule"
	MsgListApplicationVersions  MessageType = "list_application_versions"
	MsgGetWorkflowEvents        MessageType = "get_workflow_events"
	MsgGetWorkflowNotifications MessageType = "get_workflow_notifications"
	MsgGetWorkflowStreams       MessageType = "get_workflow_streams"
	MsgGetWorkflowAggregates    MessageType = "get_workflow_aggregates"
	MsgGetStepAggregates        MessageType = "get_step_aggregates"
	MsgListQueues               MessageType = "list_queues"
	MsgGetQueue                 MessageType = "get_queue"
)

// BaseMessage is the common envelope present on every frame in both directions.
// Decoding any inbound frame into BaseMessage yields the type + request_id used
// to route the frame to its waiter.
type BaseMessage struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"request_id"`
}

// BaseResponse is the minimal response shape: envelope plus an optional error.
// Mutating commands extend this with a "success" bool; read commands extend it
// with their output payload.
type BaseResponse struct {
	Type         MessageType `json:"type"`
	RequestID    string      `json:"request_id"`
	ErrorMessage *string     `json:"error_message,omitempty"`
}

// Err converts a non-empty error_message into a Go error. Embedded in every
// typed response so the dispatcher can surface executor-side failures uniformly
// without tearing down the connection.
func (b BaseResponse) Err() error {
	if b.ErrorMessage != nil && *b.ErrorMessage != "" {
		return errors.New(*b.ErrorMessage)
	}
	return nil
}

// ExecutorInfoResponse is what an executor sends in reply to our EXECUTOR_INFO
// request. It identifies the connecting process. Fields mirror
// protocol.py:ExecutorInfoResponse; optional fields are pointers so a JSON null
// round-trips cleanly.
type ExecutorInfoResponse struct {
	Type               MessageType    `json:"type"`
	RequestID          string         `json:"request_id"`
	ExecutorID         string         `json:"executor_id"`
	ApplicationVersion string         `json:"application_version"`
	Hostname           *string        `json:"hostname"`
	Language           *string        `json:"language"`
	DBOSVersion        *string        `json:"dbos_version"`
	ExecutorMetadata   map[string]any `json:"executor_metadata,omitempty"`
	ErrorMessage       *string        `json:"error_message,omitempty"`
}
