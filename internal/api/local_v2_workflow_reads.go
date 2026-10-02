package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
	"unicode/utf8"

	"github.com/zpaden/maestro/internal/protocol"
)

var localV2WorkflowSearchFields = requestFields[WorkflowSearchBody]()

func localV2WorkflowQueryValues(rawQuery string) (url.Values, error) {
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return nil, errors.New("malformed query")
	}
	for name, values := range query {
		if !utf8.ValidString(name) {
			return nil, errors.New("malformed query")
		}
		for _, value := range values {
			if !utf8.ValidString(value) {
				return nil, errors.New("malformed query")
			}
		}
	}
	return query, nil
}

func localV2WorkflowListQuery(r *http.Request) (protocol.ListWorkflowsBody, error) {
	query, err := localV2WorkflowQueryValues(r.URL.RawQuery)
	if err != nil {
		return protocol.ListWorkflowsBody{}, err
	}
	var body protocol.ListWorkflowsBody
	for name, values := range query {
		// All pinned listWorkflows query properties are scalars. In particular,
		// status/workflowName are single strings with explode=false: a comma is
		// data, not an invented array separator, and repeated scalars are invalid.
		if len(values) != 1 {
			return body, fmt.Errorf("duplicate query %q", name)
		}
		value := values[0]
		switch name {
		case "status":
			body.Status = []string{value}
		case "workflowName":
			body.WorkflowName = []string{value}
		case "limit", "offset":
			n, err := localV2Int(json.RawMessage(value), name)
			if err != nil {
				return body, err
			}
			if name == "limit" {
				body.Limit = &n
			} else {
				body.Offset = &n
			}
		case "sortDesc", "loadInput", "loadOutput":
			if value != "true" && value != "false" {
				return body, fmt.Errorf("%s must be boolean", name)
			}
			parsed := value == "true"
			switch name {
			case "sortDesc":
				body.SortDesc = parsed
			case "loadInput":
				body.LoadInput = parsed
			case "loadOutput":
				body.LoadOutput = parsed
			}
		default:
			return body, fmt.Errorf("unsupported query %q", name)
		}
	}
	return body, nil
}

func localV2SearchObject(raw []byte) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		// requestBody.required is omitted in the pinned operation, so no body has
		// the same option semantics as an empty object.
		return map[string]json.RawMessage{}, nil
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("search body must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("expected JSON search object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errors.New("invalid search field")
		}
		name, ok := token.(string)
		if !ok || !utf8.ValidString(name) {
			return nil, errors.New("invalid search field")
		}
		if _, ok := localV2WorkflowSearchFields[name]; !ok {
			return nil, fmt.Errorf("unsupported search field %q", name)
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, fmt.Errorf("duplicate search field %q", name)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("invalid search field %q", name)
		}
		fields[name] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, errors.New("invalid search object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("unexpected trailing JSON")
	}
	return fields, nil
}

func localV2SearchBool(raw json.RawMessage, name string) (bool, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false, fmt.Errorf("%s cannot be null", name)
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, fmt.Errorf("%s must be boolean", name)
	}
	return value, nil
}

func localV2SearchDate(raw json.RawMessage, name string) (string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", fmt.Errorf("%s cannot be null", name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" || !utf8.ValidString(value) {
		return "", fmt.Errorf("%s must be RFC3339", name)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return "", fmt.Errorf("%s must be RFC3339: %w", name, err)
	}
	return value, nil
}

func localV2SearchStrings(raw json.RawMessage, name string) ([]string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil || items == nil {
		return nil, fmt.Errorf("%s must be a string array or null", name)
	}
	values := make([]string, 0, len(items))
	for _, item := range items {
		if bytes.Equal(bytes.TrimSpace(item), []byte("null")) {
			return nil, fmt.Errorf("%s must contain only strings", name)
		}
		var value string
		if err := json.Unmarshal(item, &value); err != nil || !utf8.ValidString(value) {
			return nil, fmt.Errorf("%s must contain only strings", name)
		}
		values = append(values, value)
	}
	return values, nil
}

func localV2SearchAttributes(raw json.RawMessage) (map[string]any, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, errors.New("attributes must be an object")
	}
	var attributes map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&attributes); err != nil || attributes == nil {
		return nil, errors.New("attributes must be an object")
	}
	return attributes, nil
}

func localV2SearchBody(raw []byte) (protocol.ListWorkflowsBody, error) {
	fields, err := localV2SearchObject(raw)
	if err != nil {
		return protocol.ListWorkflowsBody{}, err
	}
	var body protocol.ListWorkflowsBody
	for name, value := range fields {
		switch name {
		case "limit", "offset":
			n, err := localV2Int(value, name)
			if err != nil {
				return body, err
			}
			if name == "limit" {
				body.Limit = &n
			} else {
				body.Offset = &n
			}
		case "sortDesc", "queuesOnly", "loadInput", "loadOutput", "wasForkedFrom", "hasParent":
			parsed, err := localV2SearchBool(value, name)
			if err != nil {
				return body, err
			}
			switch name {
			case "sortDesc":
				body.SortDesc = parsed
			case "queuesOnly":
				body.QueuesOnly = parsed
			case "loadInput":
				body.LoadInput = parsed
			case "loadOutput":
				body.LoadOutput = parsed
			case "wasForkedFrom":
				body.WasForkedFrom = &parsed
			case "hasParent":
				body.HasParent = &parsed
			}
		case "startTime", "endTime", "completedAfter", "completedBefore", "dequeuedAfter", "dequeuedBefore":
			parsed, err := localV2SearchDate(value, name)
			if err != nil {
				return body, err
			}
			switch name {
			case "startTime":
				body.StartTime = parsed
			case "endTime":
				body.EndTime = parsed
			case "completedAfter":
				body.CompletedAfter = parsed
			case "completedBefore":
				body.CompletedBefore = parsed
			case "dequeuedAfter":
				body.DequeuedAfter = parsed
			case "dequeuedBefore":
				body.DequeuedBefore = parsed
			}
		case "attributes":
			body.Attributes, err = localV2SearchAttributes(value)
			if err != nil {
				return body, err
			}
		default:
			values, err := localV2SearchStrings(value, name)
			if err != nil {
				return body, err
			}
			switch name {
			case "workflowIds":
				body.WorkflowUUIDs = values
			case "user":
				body.AuthenticatedUser = values
			case "status":
				body.Status = values
			case "workflowName":
				body.WorkflowName = values
			case "appVersion":
				body.ApplicationVer = values
			case "executorId":
				body.ExecutorID = values
			case "forkedFrom":
				body.ForkedFrom = values
			case "parentWorkflowId":
				body.ParentWorkflowID = values
			case "queueName":
				body.QueueName = values
			case "scheduleName":
				body.ScheduleName = values
			case "workflowIdPrefix":
				body.WorkflowIDPrefix = values
			default:
				return body, fmt.Errorf("unsupported search field %q", name)
			}
		}
	}
	return body, nil
}

func (s *Server) localV2WriteWorkflows(w http.ResponseWriter, r *http.Request, body protocol.ListWorkflowsBody) {
	rows, err := s.readWorkflows(r.Context(), r.PathValue("app"), body)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	if rows == nil {
		localV2Failure(w, errors.New("invalid executor list_workflows output: null"))
		return
	}
	out := make([]*Workflow, 0, len(rows))
	for _, row := range rows {
		value, err := localV2Workflow(row)
		if err != nil {
			localV2Failure(w, err)
			return
		}
		out = append(out, value)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) localV2ListWorkflows(w http.ResponseWriter, r *http.Request) {
	if !s.localV2Allowed(w, r) {
		return
	}
	if err := localV2InspectionApp(r.PathValue("app")); err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return
	}
	body, err := localV2WorkflowListQuery(r)
	if err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return
	}
	s.localV2WriteWorkflows(w, r, body)
}

func (s *Server) localV2Search(w http.ResponseWriter, r *http.Request) {
	if !s.localV2Allowed(w, r) {
		return
	}
	if err := localV2InspectionApp(r.PathValue("app")); err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return
	}
	query, err := localV2WorkflowQueryValues(r.URL.RawQuery)
	if err != nil || len(query) != 0 {
		localV2Problem(w, http.StatusBadRequest, "search query parameters are unsupported or malformed")
		return
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		localV2Problem(w, http.StatusBadRequest, "cannot read search body")
		return
	}
	body, err := localV2SearchBody(raw)
	if err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return
	}
	s.localV2WriteWorkflows(w, r, body)
}
