package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/protocol"
)

// localV2Allowed is defense in depth; startup separately checks the actual bound listener.
func (s *Server) localV2Allowed(w http.ResponseWriter, r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(host)
	if !s.cfg.AllowRemote && (err != nil || ip == nil || !ip.IsLoopback()) {
		localV2Problem(w, 403, "local HTTP v2 requires a loopback client")
		return false
	}
	if r.PathValue("org") != s.cfg.OrgName {
		localV2Problem(w, 404, "organization not found")
		return false
	}
	return true
}
func localV2Problem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Problem{Type: "about:blank", Title: http.StatusText(status), Status: status, Detail: detail})
}
func localV2Failure(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, hub.ErrAppUnavailable) {
		status = http.StatusServiceUnavailable
	}
	localV2Problem(w, status, err.Error())
}

func localV2Int(raw json.RawMessage, name string) (int, error) {
	if len(raw) == 0 || raw[0] < '0' || raw[0] > '9' {
		return 0, fmt.Errorf("%s must be a nonnegative integer", name)
	}
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || value < 0 || int64(int(value)) != value {
		return 0, fmt.Errorf("%s must be a nonnegative representable int64", name)
	}
	return int(value), nil
}
func localV2Query(r *http.Request) (limit, offset *int, err error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, nil, errors.New("malformed query")
	}
	for name, values := range query {
		if name != "limit" && name != "offset" {
			return nil, nil, fmt.Errorf("unsupported query %q", name)
		}
		if len(values) != 1 {
			return nil, nil, fmt.Errorf("duplicate query %q", name)
		}
		n, e := localV2Int(json.RawMessage(values[0]), name)
		if e != nil {
			return nil, nil, e
		}
		if name == "limit" {
			limit = &n
		} else {
			offset = &n
		}
	}
	return
}

// Shared read operations also serve the existing UI/API; only the HTTP representation differs.
func (s *Server) readWorkflow(ctx context.Context, app, id string, loadInput, loadOutput bool) (*protocol.WorkflowsOutput, error) {
	var resp protocol.GetWorkflowResponse
	if err := s.dispatch(ctx, app, protocol.GetWorkflowRequest(id, loadInput, loadOutput), &resp); err != nil {
		return nil, err
	}
	return resp.Output, nil
}
func (s *Server) readSteps(ctx context.Context, app, id string, loadOutput bool, limit, offset *int) ([]protocol.WorkflowSteps, error) {
	var resp protocol.ListStepsResponse
	if err := s.dispatch(ctx, app, protocol.ListStepsRequest(id, loadOutput, limit, offset), &resp); err != nil {
		return nil, err
	}
	return resp.Output, nil
}
func (s *Server) readWorkflows(ctx context.Context, app string, b protocol.ListWorkflowsBody) ([]protocol.WorkflowsOutput, error) {
	var resp protocol.ListWorkflowsResponse
	if err := s.dispatch(ctx, app, protocol.ListWorkflowsRequest(b), &resp); err != nil {
		return nil, err
	}
	return resp.Output, nil
}
func (s *Server) localV2Get(w http.ResponseWriter, r *http.Request) {
	if !s.localV2Allowed(w, r) {
		return
	}
	query, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil || len(query) != 0 {
		localV2Problem(w, 400, "get query parameters are unsupported or malformed")
		return
	}
	row, err := s.readWorkflow(r.Context(), r.PathValue("app"), r.PathValue("id"), true, true)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	if row == nil {
		localV2Problem(w, 404, "workflow not found")
		return
	}
	if row.WorkflowUUID != r.PathValue("id") {
		localV2Failure(w, errors.New("invalid executor workflow output: inconsistent WorkflowUUID"))
		return
	}
	v, err := localV2Workflow(*row)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	writeJSON(w, 200, v)
}
func (s *Server) localV2Steps(w http.ResponseWriter, r *http.Request) {
	if !s.localV2Allowed(w, r) {
		return
	}
	limit, offset, err := localV2Query(r)
	if err != nil {
		localV2Problem(w, 400, err.Error())
		return
	}
	app, id := r.PathValue("app"), r.PathValue("id")
	// The executor returns [] for both absent workflow and an existing empty or
	// offset-exhausted steps list; check existence without loading opaque blobs.
	row, err := s.readWorkflow(r.Context(), app, id, false, false)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	if row == nil {
		localV2Problem(w, 404, "workflow not found")
		return
	}
	if row.WorkflowUUID != id {
		localV2Failure(w, errors.New("invalid executor workflow output: inconsistent WorkflowUUID"))
		return
	}
	steps, err := s.readSteps(r.Context(), app, id, true, limit, offset)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	if steps == nil {
		localV2Failure(w, errors.New("invalid executor list_steps output: null"))
		return
	}
	out := make([]*Step, 0, len(steps))
	for _, step := range steps {
		v, err := localV2Step(step)
		if err != nil {
			localV2Failure(w, err)
			return
		}
		out = append(out, v)
	}
	writeJSON(w, 200, out)
}

func localV2Time(s *string, name string, required bool) (*string, error) {
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
func localV2Number(s *string, name string, required bool) (*int64, error) {
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
func localV2Workflow(w protocol.WorkflowsOutput) (*Workflow, error) {
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
		value, err := localV2Time(field.value, field.name, field.required)
		if err != nil {
			return nil, err
		}
		*field.target = value
	}
	var err error
	out.Priority, err = localV2Number(w.Priority, "priority", false)
	if err != nil {
		return nil, err
	}
	out.TimeoutMS, err = localV2Number(w.WorkflowTimeoutMS, "timeoutMs", false)
	if err != nil {
		return nil, err
	}
	return out, nil
}
func localV2Step(s protocol.WorkflowSteps) (*Step, error) {
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
	out.StartedAt, err = localV2Time(s.StartedAtEpochMS, "startedAt", false)
	if err != nil {
		return nil, err
	}
	out.CompletedAt, err = localV2Time(s.CompletedAtEpochMS, "completedAt", false)
	if err != nil {
		return nil, err
	}
	return out, nil
}
