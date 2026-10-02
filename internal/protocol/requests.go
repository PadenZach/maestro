package protocol

// Typed builders for server→executor requests. Each returns the open
// Request map (codec.go) with "type" set and the message's fields filled in; the
// connection layer adds "request_id" at send time. Building the body as a map
// and omitting unset filters keeps the wire frame minimal — the Python client's
// from_json allowlist ignores absent optional fields.

// RecoveryRequest asks an executor to recover pending workflows belonging to
// the listed executor IDs. The SDK applies its own application version.
func RecoveryRequest(executorIDs []string) Request {
	r := NewRequest(MsgRecovery)
	r["executor_ids"] = executorIDs
	return r
}

// ListWorkflowsBody carries the LIST_WORKFLOWS filters (protocol.py:ListWorkflowsBody).
// All fields are optional; only the non-nil ones are sent. Slice
// filters (name/status/version/...) accept one or many values on the wire.
type ListWorkflowsBody struct {
	WorkflowUUIDs     []string
	WorkflowName      []string
	AuthenticatedUser []string
	StartTime         string
	EndTime           string
	CompletedAfter    string
	CompletedBefore   string
	DequeuedAfter     string
	DequeuedBefore    string
	Status            []string
	ApplicationVer    []string
	ForkedFrom        []string
	ParentWorkflowID  []string
	QueueName         []string
	Limit             *int
	Offset            *int
	SortDesc          bool
	WorkflowIDPrefix  []string
	LoadInput         bool
	LoadOutput        bool
	ExecutorID        []string
	QueuesOnly        bool
	WasForkedFrom     *bool
	HasParent         *bool
	Attributes        map[string]any `json:"attributes"`
	ScheduleName      []string       `json:"schedule_name"`
	ApplicationName   []string       `json:"application_name"`
}

// toMap renders the body to its wire form, omitting empty/unset filters.
func (b ListWorkflowsBody) toMap() map[string]any {
	m := map[string]any{
		// Bools are always sent: they gate behavior (load_*/sort_desc/queues_only)
		// and default to false, which is meaningful.
		"sort_desc":   b.SortDesc,
		"load_input":  b.LoadInput,
		"load_output": b.LoadOutput,
		"queues_only": b.QueuesOnly,
	}
	putStrs := func(k string, v []string) {
		// A non-nil empty slice is an explicit [] filter. A nil slice is omitted,
		// preserving the HTTP distinction between empty, null, and absent arrays.
		if v != nil {
			m[k] = v
		}
	}
	putStr := func(k, v string) {
		if v != "" {
			m[k] = v
		}
	}
	putStrs("workflow_uuids", b.WorkflowUUIDs)
	putStrs("workflow_name", b.WorkflowName)
	putStrs("authenticated_user", b.AuthenticatedUser)
	putStr("start_time", b.StartTime)
	putStr("end_time", b.EndTime)
	putStr("completed_after", b.CompletedAfter)
	putStr("completed_before", b.CompletedBefore)
	putStr("dequeued_after", b.DequeuedAfter)
	putStr("dequeued_before", b.DequeuedBefore)
	putStrs("status", b.Status)
	putStrs("application_version", b.ApplicationVer)
	putStrs("forked_from", b.ForkedFrom)
	putStrs("parent_workflow_id", b.ParentWorkflowID)
	putStrs("queue_name", b.QueueName)
	putStrs("workflow_id_prefix", b.WorkflowIDPrefix)
	putStrs("executor_id", b.ExecutorID)
	if b.Attributes != nil {
		m["attributes"] = b.Attributes
	}
	putStrs("schedule_name", b.ScheduleName)
	putStrs("application_name", b.ApplicationName)
	if b.Limit != nil {
		m["limit"] = *b.Limit
	}
	if b.Offset != nil {
		m["offset"] = *b.Offset
	}
	if b.WasForkedFrom != nil {
		m["was_forked_from"] = *b.WasForkedFrom
	}
	if b.HasParent != nil {
		m["has_parent"] = *b.HasParent
	}
	return m
}

// ListWorkflowsRequest builds a LIST_WORKFLOWS frame.
func ListWorkflowsRequest(body ListWorkflowsBody) Request {
	r := NewRequest(MsgListWorkflows)
	r["body"] = body.toMap()
	return r
}

// ListQueuedWorkflowsRequest builds a LIST_QUEUED_WORKFLOWS frame. The executor
// forces queues_only=true regardless, so we drop it from the body (its body type
// has no queues_only field).
func ListQueuedWorkflowsRequest(body ListWorkflowsBody) Request {
	r := NewRequest(MsgListQueuedWorkflows)
	m := body.toMap()
	delete(m, "queues_only")
	r["body"] = m
	return r
}

// GetWorkflowRequest builds a GET_WORKFLOW frame.
func GetWorkflowRequest(workflowID string, loadInput, loadOutput bool) Request {
	r := NewRequest(MsgGetWorkflow)
	r["workflow_id"] = workflowID
	r["load_input"] = loadInput
	r["load_output"] = loadOutput
	return r
}

// ListStepsRequest builds a LIST_STEPS frame. limit/offset are optional.
func ListStepsRequest(workflowID string, loadOutput bool, limit, offset *int) Request {
	r := NewRequest(MsgListSteps)
	r["workflow_id"] = workflowID
	r["load_output"] = loadOutput
	if limit != nil {
		r["limit"] = *limit
	}
	if offset != nil {
		r["offset"] = *offset
	}
	return r
}

// GetWorkflowEventsRequest builds a GET_WORKFLOW_EVENTS frame.
func GetWorkflowEventsRequest(workflowID string) Request {
	r := NewRequest(MsgGetWorkflowEvents)
	r["workflow_id"] = workflowID
	return r
}

// GetWorkflowNotificationsRequest builds a GET_WORKFLOW_NOTIFICATIONS frame.
func GetWorkflowNotificationsRequest(workflowID string) Request {
	r := NewRequest(MsgGetWorkflowNotifications)
	r["workflow_id"] = workflowID
	return r
}

// GetWorkflowStreamsRequest builds a GET_WORKFLOW_STREAMS frame.
func GetWorkflowStreamsRequest(workflowID string) Request {
	r := NewRequest(MsgGetWorkflowStreams)
	r["workflow_id"] = workflowID
	return r
}

// ListQueuesBody optionally filters queues by application on reviewed SDKs.
type ListQueuesBody struct {
	ApplicationName []string `json:"application_name"`
}

// ListQueuesRequest retains the legacy no-body form when called without a filter.
func ListQueuesRequest(body ...ListQueuesBody) Request {
	r := NewRequest(MsgListQueues)
	if len(body) > 0 && len(body[0].ApplicationName) > 0 {
		r["body"] = map[string]any{"application_name": body[0].ApplicationName}
	}
	return r
}

// GetQueueRequest builds a GET_QUEUE frame.
func GetQueueRequest(name string) Request {
	r := NewRequest(MsgGetQueue)
	r["name"] = name
	return r
}

// CancelRequest builds a CANCEL frame for a single workflow.
func CancelRequest(workflowID string, cancelChildren bool) Request {
	r := NewRequest(MsgCancel)
	r["workflow_id"] = workflowID
	r["cancel_children"] = cancelChildren
	return r
}

// ResumeRequest builds a RESUME frame for a single workflow, optionally onto a
// named queue.
func ResumeRequest(workflowID string, queueName *string) Request {
	r := NewRequest(MsgResume)
	r["workflow_id"] = workflowID
	if queueName != nil {
		r["queue_name"] = *queueName
	}
	return r
}
