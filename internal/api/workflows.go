package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/PadenZach/maestro/internal/protocol"
)

// These models are both the JSON responses and the source of their OpenAPI schemas.
type Workflow struct {
	WorkflowID        string  `json:"workflowId" minLength:"1"`
	Status            string  `json:"status" minLength:"1"`
	WorkflowName      *string `json:"workflowName"`
	WorkflowClass     *string `json:"workflowClass"`
	WorkflowConfig    *string `json:"workflowConfig"`
	User              *string `json:"user"`
	AssumedRole       *string `json:"assumedRole"`
	Roles             *string `json:"roles"`
	Input             *string `json:"input"`
	Output            *string `json:"output"`
	Error             *string `json:"error"`
	QueueName         *string `json:"queueName"`
	AppVersion        *string `json:"appVersion"`
	ExecutorID        *string `json:"executorId"`
	DeduplicationID   *string `json:"deduplicationId"`
	QueuePartitionKey *string `json:"queuePartitionKey"`
	ForkedFrom        *string `json:"forkedFrom"`
	WasForkedFrom     bool    `json:"wasForkedFrom"`
	ParentWorkflowID  *string `json:"parentWorkflowId"`
	Attributes        *string `json:"attributes"`
	ScheduleName      *string `json:"scheduleName"`
	ApplicationName   *string `json:"applicationName"`
	CreatedAt         *string `json:"createdAt" format:"date-time" nullable:"false"`
	UpdatedAt         *string `json:"updatedAt" format:"date-time"`
	Deadline          *string `json:"deadline" format:"date-time"`
	DequeuedAt        *string `json:"dequeuedAt" format:"date-time"`
	DelayUntil        *string `json:"delayUntil" format:"date-time"`
	CompletedAt       *string `json:"completedAt" format:"date-time"`
	Priority          *int64  `json:"priority" format:"int32" minimum:"-2147483648" maximum:"2147483647"`
	TimeoutMS         *int64  `json:"timeoutMs"`
}

type Step struct {
	StepID          int     `json:"stepId" format:"int32" minimum:"-2147483648" maximum:"2147483647"`
	StepName        string  `json:"stepName" minLength:"1"`
	Output          *string `json:"output"`
	Error           *string `json:"error"`
	ChildWorkflowID *string `json:"childWorkflowId"`
	StartedAt       *string `json:"startedAt" format:"date-time"`
	CompletedAt     *string `json:"completedAt" format:"date-time"`
}

type workflowListQuery struct {
	Status       string `json:"status,omitempty"`
	WorkflowName string `json:"workflowName,omitempty"`
	Limit        *int   `json:"limit,omitempty" minimum:"0"`
	Offset       *int   `json:"offset,omitempty" minimum:"0"`
	SortDesc     bool   `json:"sortDesc,omitempty"`
	LoadInput    bool   `json:"loadInput,omitempty"`
	LoadOutput   bool   `json:"loadOutput,omitempty"`
}

type stepsQuery struct {
	Limit  *int `json:"limit,omitempty" minimum:"0"`
	Offset *int `json:"offset,omitempty" minimum:"0"`
}

func (s *handler) getWorkflow(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrganization(w, r) {
		return
	}
	query, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil || len(query) != 0 {
		writeProblem(w, 400, "get query parameters are unsupported or malformed")
		return
	}
	row, err := s.hub.Workflow(r.Context(), r.PathValue("app"), r.PathValue("id"), true, true)
	if err != nil {
		writeFailure(w, err)
		return
	}
	if row == nil {
		writeProblem(w, 404, "workflow not found")
		return
	}
	if row.WorkflowUUID != r.PathValue("id") {
		writeFailure(w, errors.New("invalid executor workflow output: inconsistent WorkflowUUID"))
		return
	}
	v, err := workflowResponse(*row)
	if err != nil {
		writeFailure(w, err)
		return
	}
	writeJSON(w, 200, v)
}

func (s *handler) listSteps(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrganization(w, r) {
		return
	}
	limit, offset, err := parseStepQuery(r)
	if err != nil {
		writeProblem(w, 400, err.Error())
		return
	}
	app, id := r.PathValue("app"), r.PathValue("id")
	// The executor returns [] for both absent workflow and an existing empty or
	// offset-exhausted steps list; check existence without loading opaque blobs.
	row, err := s.hub.Workflow(r.Context(), app, id, false, false)
	if err != nil {
		writeFailure(w, err)
		return
	}
	if row == nil {
		writeProblem(w, 404, "workflow not found")
		return
	}
	if row.WorkflowUUID != id {
		writeFailure(w, errors.New("invalid executor workflow output: inconsistent WorkflowUUID"))
		return
	}
	steps, err := s.hub.Steps(r.Context(), app, id, true, limit, offset)
	if err != nil {
		writeFailure(w, err)
		return
	}
	if steps == nil {
		writeFailure(w, errors.New("invalid executor list_steps output: null"))
		return
	}
	out := make([]*Step, 0, len(steps))
	for _, step := range steps {
		v, err := stepResponse(step)
		if err != nil {
			writeFailure(w, err)
			return
		}
		out = append(out, v)
	}
	writeJSON(w, 200, out)
}

func workflowTime(s *string, name string, required bool) (*string, error) {
	if s == nil {
		if required {
			return nil, fmt.Errorf("invalid executor %s: missing timestamp", name)
		}
		return nil, nil
	}
	n, err := strconv.ParseInt(*s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid executor %s: epoch milliseconds required", name)
	}
	// Checked range prevents overflow when converting milliseconds to nanoseconds.
	sec, ms := n/1000, n%1000
	if n < -62135596800000 || n > 253402300799999 {
		return nil, fmt.Errorf("invalid executor %s: timestamp out of range", name)
	}
	value := time.Unix(sec, ms*int64(time.Millisecond)).UTC().Format("2006-01-02T15:04:05.000Z07:00")
	return &value, nil
}

func workflowNumber(s *string, name string, required bool) (*int64, error) {
	if s == nil {
		if required {
			return nil, fmt.Errorf("invalid executor %s: missing number", name)
		}
		return nil, nil
	}
	n, err := strconv.ParseInt(*s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid executor %s: integer required", name)
	}
	if name == "priority" && (n < -2147483648 || n > 2147483647) {
		return nil, fmt.Errorf("invalid executor %s: int32 required", name)
	}
	return &n, nil
}

func workflowResponse(w protocol.WorkflowsOutput) (*Workflow, error) {
	if w.WorkflowUUID == "" || w.Status == nil || *w.Status == "" {
		return nil, errors.New("invalid executor workflow identity/status")
	}
	if !w.HasWasForkedFrom() {
		return nil, errors.New("invalid executor WasForkedFrom: missing or null")
	}
	// The released SDK permits explicit nulls here. Preserve them rather than
	// inventing defaults; a missing key is still a malformed response.
	for _, field := range []string{"Priority", "UpdatedAt"} {
		if !w.FieldPresent(field) {
			return nil, fmt.Errorf("invalid executor workflow: missing %s", field)
		}
	}
	out := &Workflow{WorkflowID: w.WorkflowUUID, Status: *w.Status,
		WorkflowName: w.WorkflowName, WorkflowClass: w.WorkflowClassName, WorkflowConfig: w.WorkflowConfigName,
		User: w.AuthenticatedUser, AssumedRole: w.AssumedRole, Roles: w.AuthenticatedRoles,
		Input: w.Input, Output: w.Output, Error: w.Error, QueueName: w.QueueName,
		AppVersion: w.ApplicationVersion, ExecutorID: w.ExecutorID, DeduplicationID: w.DeduplicationID,
		QueuePartitionKey: w.QueuePartitionKey, ForkedFrom: w.ForkedFrom, WasForkedFrom: w.WasForkedFrom,
		ParentWorkflowID: w.ParentWorkflowID, Attributes: w.Attributes, ScheduleName: w.ScheduleName, ApplicationName: w.ApplicationName,
	}
	for _, field := range []struct {
		name     string
		value    *string
		target   **string
		required bool
	}{
		{"createdAt", w.CreatedAt, &out.CreatedAt, true}, {"updatedAt", w.UpdatedAt, &out.UpdatedAt, false},
		{"deadline", w.WorkflowDeadlineEpochMS, &out.Deadline, false}, {"dequeuedAt", w.DequeuedAt, &out.DequeuedAt, false},
		{"delayUntil", w.DelayUntilEpochMS, &out.DelayUntil, false}, {"completedAt", w.CompletedAt, &out.CompletedAt, false},
	} {
		value, err := workflowTime(field.value, field.name, field.required)
		if err != nil {
			return nil, err
		}
		*field.target = value
	}
	var err error
	out.Priority, err = workflowNumber(w.Priority, "priority", false)
	if err != nil {
		return nil, err
	}
	out.TimeoutMS, err = workflowNumber(w.WorkflowTimeoutMS, "timeoutMs", false)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func stepResponse(s protocol.WorkflowSteps) (*Step, error) {
	if s.FunctionName == "" {
		return nil, errors.New("invalid executor step name")
	}
	if !s.HasFunctionID() {
		return nil, errors.New("invalid executor stepId: missing or null function_id")
	}
	if s.FunctionID < -2147483648 || s.FunctionID > 2147483647 {
		return nil, errors.New("invalid executor stepId: int32 required")
	}
	out := &Step{StepID: s.FunctionID, StepName: s.FunctionName, Output: s.Output, Error: s.Error, ChildWorkflowID: s.ChildWorkflowID}
	var err error
	out.StartedAt, err = workflowTime(s.StartedAtEpochMS, "startedAt", false)
	if err != nil {
		return nil, err
	}
	out.CompletedAt, err = workflowTime(s.CompletedAtEpochMS, "completedAt", false)
	if err != nil {
		return nil, err
	}
	return out, nil
}
