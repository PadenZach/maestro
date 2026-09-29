package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/protocol"
)

// localV2Allowed is defense in depth; startup separately checks the actual bound listener.
func localV2Allowed(w http.ResponseWriter, r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() {
		localV2Problem(w, 403, "local HTTP v2 requires a loopback client")
		return false
	}
	if r.PathValue("org") != "local" {
		localV2Problem(w, 404, "organization not found")
		return false
	}
	return true
}
func localV2Problem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "about:blank", "title": http.StatusText(status), "status": status, "detail": detail})
}
func localV2Failure(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, hub.ErrAppUnavailable) {
		status = http.StatusServiceUnavailable
	}
	localV2Problem(w, status, err.Error())
}

// Only the approved subset of WorkflowSearchBody is accepted. Unknown fields and
// empty lists cannot silently broaden an agent's search.
var localSearchFields = map[string]struct{}{
	"workflowIds": {}, "user": {}, "status": {}, "workflowName": {}, "appVersion": {}, "queueName": {}, "startTime": {}, "endTime": {}, "sortDesc": {}, "queuesOnly": {}, "limit": {}, "offset": {},
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
func localV2SearchBody(raw []byte) (protocol.ListWorkflowsBody, error) {
	fields := make(map[string]json.RawMessage)
	d := json.NewDecoder(bytes.NewReader(raw))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return protocol.ListWorkflowsBody{}, errors.New("expected JSON search object")
	}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return protocol.ListWorkflowsBody{}, errors.New("invalid search key")
		}
		name := token.(string)
		if _, exists := fields[name]; exists {
			return protocol.ListWorkflowsBody{}, fmt.Errorf("duplicate search field %q", name)
		}
		var value json.RawMessage
		if err := d.Decode(&value); err != nil {
			return protocol.ListWorkflowsBody{}, errors.New("invalid search value")
		}
		fields[name] = value
	}
	if _, err := d.Token(); err != nil {
		return protocol.ListWorkflowsBody{}, errors.New("invalid search object")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return protocol.ListWorkflowsBody{}, errors.New("unexpected trailing JSON")
	}
	var b protocol.ListWorkflowsBody
	for name, value := range fields {
		if _, ok := localSearchFields[name]; !ok {
			return b, fmt.Errorf("unsupported search field %q", name)
		}
		if name == "limit" || name == "offset" {
			n, err := localV2Int(value, name)
			if err != nil {
				return b, err
			}
			if name == "limit" {
				b.Limit = &n
			} else {
				b.Offset = &n
			}
			continue
		}
		if name == "sortDesc" || name == "queuesOnly" {
			if string(value) == "null" {
				return b, fmt.Errorf("%s cannot be null", name)
			}
			var v bool
			if err := json.Unmarshal(value, &v); err != nil || string(value) != "true" && string(value) != "false" {
				return b, fmt.Errorf("%s must be boolean", name)
			}
			if name == "sortDesc" {
				b.SortDesc = v
			} else {
				b.QueuesOnly = v
			}
			continue
		}
		if name == "startTime" || name == "endTime" {
			var v string
			if err := json.Unmarshal(value, &v); err != nil || v == "" {
				return b, fmt.Errorf("%s must be RFC3339", name)
			}
			if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
				return b, fmt.Errorf("%s must be RFC3339: %w", name, err)
			}
			if name == "startTime" {
				b.StartTime = v
			} else {
				b.EndTime = v
			}
			continue
		}
		// The OpenAPI makes these arrays nullable. Null means no filter; an explicit
		// empty array is rejected rather than silently matching everything.
		if string(value) == "null" {
			continue
		}
		var arr []string
		if err := json.Unmarshal(value, &arr); err != nil || len(arr) == 0 {
			return b, fmt.Errorf("%s must be a nonempty string array", name)
		}
		for _, v := range arr {
			if v == "" {
				return b, fmt.Errorf("%s contains an empty value", name)
			}
		}
		switch name {
		case "workflowIds":
			b.WorkflowUUIDs = arr
		case "user":
			b.AuthenticatedUser = arr
		case "status":
			b.Status = arr
		case "workflowName":
			b.WorkflowName = arr
		case "appVersion":
			b.ApplicationVer = arr
		case "queueName":
			b.QueueName = arr
		}
	}
	return b, nil
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
func (s *Server) localV2Search(w http.ResponseWriter, r *http.Request) {
	if !localV2Allowed(w, r) {
		return
	}
	query, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil || len(query) != 0 {
		localV2Problem(w, 400, "search query parameters are unsupported or malformed")
		return
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		localV2Problem(w, 400, "cannot read search body")
		return
	}
	b, err := localV2SearchBody(raw)
	if err != nil {
		localV2Problem(w, 400, err.Error())
		return
	}
	rows, err := s.readWorkflows(r.Context(), r.PathValue("app"), b)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	if rows == nil {
		localV2Failure(w, errors.New("invalid executor list_workflows output: null"))
		return
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		v, err := localV2Workflow(row)
		if err != nil {
			localV2Failure(w, err)
			return
		}
		out = append(out, v)
	}
	writeJSON(w, 200, out)
}
func (s *Server) localV2Get(w http.ResponseWriter, r *http.Request) {
	if !localV2Allowed(w, r) {
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
	v, err := localV2Workflow(*row)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	writeJSON(w, 200, v)
}
func (s *Server) localV2Steps(w http.ResponseWriter, r *http.Request) {
	if !localV2Allowed(w, r) {
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
	steps, err := s.readSteps(r.Context(), app, id, true, limit, offset)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	if steps == nil {
		localV2Failure(w, errors.New("invalid executor list_steps output: null"))
		return
	}
	out := make([]map[string]any, 0, len(steps))
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

func localV2Time(s *string, name string, required bool) (any, error) {
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
	if sec < -62135596800 || sec > 253402300799 {
		return nil, fmt.Errorf("invalid executor %s: timestamp out of range", name)
	}
	return time.Unix(sec, ms*int64(time.Millisecond)).UTC().Format("2006-01-02T15:04:05.000Z07:00"), nil
}
func localV2Number(s *string, name string, required bool) (any, error) {
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
	return n, nil
}
func localV2Workflow(w protocol.WorkflowsOutput) (map[string]any, error) {
	if w.WorkflowUUID == "" || w.Status == nil || *w.Status == "" {
		return nil, errors.New("invalid executor workflow identity/status")
	}
	if !w.HasWasForkedFrom() {
		return nil, errors.New("invalid executor WasForkedFrom: missing or null")
	}
	out := map[string]any{"workflowId": w.WorkflowUUID, "status": *w.Status,
		"workflowName": w.WorkflowName, "workflowClass": w.WorkflowClassName, "workflowConfig": w.WorkflowConfigName,
		"user": w.AuthenticatedUser, "assumedRole": w.AssumedRole, "roles": w.AuthenticatedRoles,
		"input": w.Input, "output": w.Output, "error": w.Error, "queueName": w.QueueName,
		"appVersion": w.ApplicationVersion, "executorId": w.ExecutorID, "deduplicationId": w.DeduplicationID,
		"queuePartitionKey": w.QueuePartitionKey, "forkedFrom": w.ForkedFrom, "wasForkedFrom": w.WasForkedFrom,
		"parentWorkflowId": w.ParentWorkflowID, "attributes": w.Attributes, "scheduleName": w.ScheduleName, "applicationName": w.ApplicationName,
	}
	var err error
	for _, field := range []struct {
		name     string
		value    *string
		required bool
	}{
		{"createdAt", w.CreatedAt, true}, {"updatedAt", w.UpdatedAt, true}, {"deadline", w.WorkflowDeadlineEpochMS, false}, {"dequeuedAt", w.DequeuedAt, false}, {"delayUntil", w.DelayUntilEpochMS, false}, {"completedAt", w.CompletedAt, false},
	} {
		out[field.name], err = localV2Time(field.value, field.name, field.required)
		if err != nil {
			return nil, err
		}
	}
	for _, field := range []struct {
		name     string
		value    *string
		required bool
	}{{"priority", w.Priority, true}, {"timeoutMs", w.WorkflowTimeoutMS, false}} {
		out[field.name], err = localV2Number(field.value, field.name, field.required)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
func localV2Step(s protocol.WorkflowSteps) (map[string]any, error) {
	if s.FunctionName == "" {
		return nil, errors.New("invalid executor step name")
	}
	if !s.HasFunctionID() {
		return nil, errors.New("invalid executor stepId: missing or null function_id")
	}
	if s.FunctionID < -2147483648 || s.FunctionID > 2147483647 {
		return nil, errors.New("invalid executor stepId: int32 required")
	}
	out := map[string]any{"stepId": s.FunctionID, "stepName": s.FunctionName, "output": s.Output, "error": s.Error, "childWorkflowId": s.ChildWorkflowID}
	var err error
	out["startedAt"], err = localV2Time(s.StartedAtEpochMS, "startedAt", false)
	if err != nil {
		return nil, err
	}
	out["completedAt"], err = localV2Time(s.CompletedAtEpochMS, "completedAt", false)
	if err != nil {
		return nil, err
	}
	return out, nil
}
