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

var localV2WorkflowAggregateFields = map[string]struct{}{
	"groupByStatus": {}, "groupByWorkflowName": {}, "groupByQueueName": {},
	"groupByExecutorId": {}, "groupByAppVersion": {}, "groupByApplicationName": {},
	"selectCount": {}, "selectMinCreatedAt": {}, "selectMaxQueueWaitMs": {},
	"selectMaxTotalLatencyMs": {}, "timeBucketSizeMs": {}, "status": {},
	"startTime": {}, "endTime": {}, "completedAfter": {}, "completedBefore": {},
	"dequeuedAfter": {}, "dequeuedBefore": {}, "workflowName": {}, "appVersion": {},
	"executorId": {}, "queueName": {}, "workflowIdPrefix": {}, "workflowIds": {},
	"forkedFrom": {}, "parentWorkflowId": {}, "user": {}, "scheduleName": {},
	"wasForkedFrom": {}, "hasParent": {}, "attributes": {},
}

var localV2StepAggregateFields = map[string]struct{}{
	"groupByFunctionName": {}, "groupByStatus": {}, "selectCount": {},
	"selectMaxDurationMs": {}, "timeBucketSizeMs": {}, "status": {},
	"stepName": {}, "workflowIdPrefix": {}, "completedAfter": {}, "completedBefore": {},
}

func localV2InspectionApp(app string) error {
	if !utf8.ValidString(app) || utf8.RuneCountInString(app) < 3 || utf8.RuneCountInString(app) > 256 || !localV2AppName.MatchString(app) {
		return errors.New("invalid application name")
	}
	return nil
}

// localV2InspectionObject preserves omitted, null, false, zero, and empty values
// while rejecting duplicate and unknown top-level fields before dispatch.
func localV2InspectionObject(raw []byte, allowed map[string]struct{}) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		// requestBody is optional in both pinned aggregate operations.
		return map[string]json.RawMessage{}, nil
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("aggregate body must be valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("expected JSON aggregate object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errors.New("invalid aggregate field")
		}
		name, ok := token.(string)
		if !ok || !utf8.ValidString(name) {
			return nil, errors.New("invalid aggregate field")
		}
		if _, ok := allowed[name]; !ok {
			return nil, fmt.Errorf("unsupported aggregate field %q", name)
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, fmt.Errorf("duplicate aggregate field %q", name)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("invalid aggregate field %q", name)
		}
		fields[name] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, errors.New("invalid aggregate object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("unexpected trailing JSON")
	}
	return fields, nil
}

func localV2InspectionBool(raw json.RawMessage, name string) (*bool, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("%s must be boolean", name)
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%s must be boolean", name)
	}
	return &value, nil
}

func localV2InspectionInt64(raw json.RawMessage, name string) (*int64, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("%s must be an int64", name)
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%s must be an int64", name)
	}
	return &value, nil
}

func localV2InspectionDate(raw json.RawMessage, name string) (*string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("%s must be RFC3339", name)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || value == "" {
		return nil, fmt.Errorf("%s must be RFC3339", name)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return nil, fmt.Errorf("%s must be RFC3339: %w", name, err)
	}
	return &value, nil
}

func localV2InspectionStrings(raw json.RawMessage, name string, nullable bool) ([]string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if nullable {
			return nil, nil
		}
		return nil, fmt.Errorf("%s must be a string array", name)
	}
	var rawValues []json.RawMessage
	if err := json.Unmarshal(raw, &rawValues); err != nil || rawValues == nil {
		return nil, fmt.Errorf("%s must be a string array", name)
	}
	values := make([]string, len(rawValues))
	for i, rawValue := range rawValues {
		if bytes.Equal(bytes.TrimSpace(rawValue), []byte("null")) || json.Unmarshal(rawValue, &values[i]) != nil {
			return nil, fmt.Errorf("%s[%d] must be a string", name, i)
		}
	}
	return values, nil
}

func localV2InspectionAttributes(raw json.RawMessage) (map[string]any, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, errors.New("attributes must be an object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil || value == nil {
		return nil, errors.New("attributes must be an object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("attributes must be an object")
	}
	return value, nil
}

func localV2WorkflowAggregateBody(fields map[string]json.RawMessage) (protocol.WorkflowAggregatesBody, error) {
	var body protocol.WorkflowAggregatesBody
	for name, raw := range fields {
		var err error
		switch name {
		case "groupByStatus":
			body.GroupByStatus, err = localV2InspectionBool(raw, name)
		case "groupByWorkflowName":
			body.GroupByName, err = localV2InspectionBool(raw, name)
		case "groupByQueueName":
			body.GroupByQueueName, err = localV2InspectionBool(raw, name)
		case "groupByExecutorId":
			body.GroupByExecutorID, err = localV2InspectionBool(raw, name)
		case "groupByAppVersion":
			body.GroupByApplicationVersion, err = localV2InspectionBool(raw, name)
		case "groupByApplicationName":
			body.GroupByApplicationName, err = localV2InspectionBool(raw, name)
		case "selectCount":
			body.SelectCount, err = localV2InspectionBool(raw, name)
		case "selectMinCreatedAt":
			body.SelectMinCreatedAt, err = localV2InspectionBool(raw, name)
		case "selectMaxQueueWaitMs":
			body.SelectMaxQueueWaitMS, err = localV2InspectionBool(raw, name)
		case "selectMaxTotalLatencyMs":
			body.SelectMaxTotalLatencyMS, err = localV2InspectionBool(raw, name)
		case "timeBucketSizeMs":
			body.TimeBucketSizeMS, err = localV2InspectionInt64(raw, name)
		case "status":
			body.Status, err = localV2InspectionStrings(raw, name, true)
		case "startTime":
			body.StartTime, err = localV2InspectionDate(raw, name)
		case "endTime":
			body.EndTime, err = localV2InspectionDate(raw, name)
		case "completedAfter":
			body.CompletedAfter, err = localV2InspectionDate(raw, name)
		case "completedBefore":
			body.CompletedBefore, err = localV2InspectionDate(raw, name)
		case "dequeuedAfter":
			body.DequeuedAfter, err = localV2InspectionDate(raw, name)
		case "dequeuedBefore":
			body.DequeuedBefore, err = localV2InspectionDate(raw, name)
		case "workflowName":
			body.Name, err = localV2InspectionStrings(raw, name, true)
		case "appVersion":
			body.AppVersion, err = localV2InspectionStrings(raw, name, true)
		case "executorId":
			body.ExecutorID, err = localV2InspectionStrings(raw, name, true)
		case "queueName":
			body.QueueName, err = localV2InspectionStrings(raw, name, true)
		case "workflowIdPrefix":
			body.WorkflowIDPrefix, err = localV2InspectionStrings(raw, name, true)
		case "workflowIds":
			body.WorkflowIDs, err = localV2InspectionStrings(raw, name, true)
		case "forkedFrom":
			body.ForkedFrom, err = localV2InspectionStrings(raw, name, true)
		case "parentWorkflowId":
			body.ParentWorkflowID, err = localV2InspectionStrings(raw, name, true)
		case "user":
			body.User, err = localV2InspectionStrings(raw, name, true)
		case "scheduleName":
			body.ScheduleName, err = localV2InspectionStrings(raw, name, true)
		case "wasForkedFrom":
			body.WasForkedFrom, err = localV2InspectionBool(raw, name)
		case "hasParent":
			body.HasParent, err = localV2InspectionBool(raw, name)
		case "attributes":
			body.Attributes, err = localV2InspectionAttributes(raw)
		}
		if err != nil {
			return body, err
		}
	}
	return body, nil
}

func localV2StepAggregateBody(fields map[string]json.RawMessage) (protocol.StepAggregatesBody, error) {
	var body protocol.StepAggregatesBody
	for name, raw := range fields {
		var err error
		switch name {
		case "groupByFunctionName":
			body.GroupByFunctionName, err = localV2InspectionBool(raw, name)
		case "groupByStatus":
			body.GroupByStatus, err = localV2InspectionBool(raw, name)
		case "selectCount":
			body.SelectCount, err = localV2InspectionBool(raw, name)
		case "selectMaxDurationMs":
			body.SelectMaxDurationMS, err = localV2InspectionBool(raw, name)
		case "timeBucketSizeMs":
			body.TimeBucketSizeMS, err = localV2InspectionInt64(raw, name)
		case "status":
			body.Status, err = localV2InspectionStrings(raw, name, false)
		case "stepName":
			body.FunctionName, err = localV2InspectionStrings(raw, name, false)
		case "workflowIdPrefix":
			body.WorkflowIDPrefix, err = localV2InspectionStrings(raw, name, false)
		case "completedAfter":
			body.CompletedAfter, err = localV2InspectionDate(raw, name)
		case "completedBefore":
			body.CompletedBefore, err = localV2InspectionDate(raw, name)
		}
		if err != nil {
			return body, err
		}
	}
	return body, nil
}

func localV2InspectionAggregateRequest(w http.ResponseWriter, r *http.Request, allowed map[string]struct{}) (map[string]json.RawMessage, bool) {
	if !localV2Allowed(w, r) || !localV2NoQuery(w, r) {
		return nil, false
	}
	if err := localV2InspectionApp(r.PathValue("app")); err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		localV2Problem(w, http.StatusBadRequest, "cannot read aggregate body")
		return nil, false
	}
	fields, err := localV2InspectionObject(raw, allowed)
	if err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	return fields, true
}

func localV2InspectionPayload(raw []byte) ([]json.RawMessage, error) {
	fields, err := localV2RawObject(raw, "aggregate response")
	if err != nil {
		return nil, err
	}
	var base protocol.BaseResponse
	if err := json.Unmarshal(raw, &base); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	if err := base.Err(); err != nil {
		return nil, err
	}
	output, ok := fields["output"]
	if !ok || bytes.Equal(bytes.TrimSpace(output), []byte("null")) {
		return nil, errors.New("invalid executor aggregate output: missing or null")
	}
	var records []json.RawMessage
	if err := json.Unmarshal(output, &records); err != nil {
		return nil, errors.New("invalid executor aggregate output: array required")
	}
	return records, nil
}

func localV2InspectionGroup(raw json.RawMessage, record string) (map[string]*string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("invalid executor %s.group: object required", record)
	}
	fields, err := localV2RawObject(raw, record+".group")
	if err != nil {
		return nil, err
	}
	group := make(map[string]*string, len(fields))
	for name, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			group[name] = nil
			continue
		}
		var decoded string
		if err := json.Unmarshal(value, &decoded); err != nil {
			return nil, fmt.Errorf("invalid executor %s.group.%s: string or null required", record, name)
		}
		group[name] = &decoded
	}
	return group, nil
}

func localV2InspectionNullableInt(fields map[string]json.RawMessage, field, record string) (*int64, error) {
	raw, ok := fields[field]
	if !ok {
		return nil, fmt.Errorf("invalid executor %s: missing %s", record, field)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("invalid executor %s.%s: integer or null required", record, field)
	}
	return &value, nil
}

func localV2InspectionTime(milliseconds int64, record string) (string, error) {
	const (
		minRFC3339Milliseconds int64 = -62135596800000
		maxRFC3339Milliseconds int64 = 253402300799999
	)
	if milliseconds < minRFC3339Milliseconds || milliseconds > maxRFC3339Milliseconds {
		return "", fmt.Errorf("invalid executor %s.min_created_at: timestamp out of range", record)
	}
	seconds, remainder := milliseconds/1000, milliseconds%1000
	return time.Unix(seconds, remainder*int64(time.Millisecond)).UTC().Format("2006-01-02T15:04:05.000Z07:00"), nil
}

func localV2WorkflowAggregateRecord(raw json.RawMessage) (map[string]any, error) {
	const record = "workflow aggregate"
	fields, err := localV2RawObject(raw, record)
	if err != nil {
		return nil, err
	}
	if len(fields) != 5 {
		return nil, errors.New("invalid executor workflow aggregate: unexpected or missing fields")
	}
	groupRaw, ok := fields["group"]
	if !ok {
		return nil, errors.New("invalid executor workflow aggregate: missing group")
	}
	group, err := localV2InspectionGroup(groupRaw, record)
	if err != nil {
		return nil, err
	}
	count, err := localV2InspectionNullableInt(fields, "count", record)
	if err != nil {
		return nil, err
	}
	minCreatedAt, err := localV2InspectionNullableInt(fields, "min_created_at", record)
	if err != nil {
		return nil, err
	}
	maxQueueWaitMS, err := localV2InspectionNullableInt(fields, "max_queue_wait_ms", record)
	if err != nil {
		return nil, err
	}
	maxTotalLatencyMS, err := localV2InspectionNullableInt(fields, "max_total_latency_ms", record)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"group": group}
	if count != nil {
		out["count"] = *count
	}
	if minCreatedAt != nil {
		converted, err := localV2InspectionTime(*minCreatedAt, record)
		if err != nil {
			return nil, err
		}
		out["minCreatedAt"] = converted
	}
	if maxQueueWaitMS != nil {
		out["maxQueueWaitMs"] = *maxQueueWaitMS
	}
	if maxTotalLatencyMS != nil {
		out["maxTotalLatencyMs"] = *maxTotalLatencyMS
	}
	return out, nil
}

func localV2StepAggregateRecord(raw json.RawMessage) (map[string]any, error) {
	const record = "step aggregate"
	fields, err := localV2RawObject(raw, record)
	if err != nil {
		return nil, err
	}
	if len(fields) != 3 {
		return nil, errors.New("invalid executor step aggregate: unexpected or missing fields")
	}
	groupRaw, ok := fields["group"]
	if !ok {
		return nil, errors.New("invalid executor step aggregate: missing group")
	}
	group, err := localV2InspectionGroup(groupRaw, record)
	if err != nil {
		return nil, err
	}
	count, err := localV2InspectionNullableInt(fields, "count", record)
	if err != nil {
		return nil, err
	}
	maxDurationMS, err := localV2InspectionNullableInt(fields, "max_duration_ms", record)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"group": group}
	if count != nil {
		out["count"] = *count
	}
	if maxDurationMS != nil {
		out["maxDurationMs"] = *maxDurationMS
	}
	return out, nil
}

func (s *Server) localV2WorkflowAggregates(w http.ResponseWriter, r *http.Request) {
	fields, ok := localV2InspectionAggregateRequest(w, r, localV2WorkflowAggregateFields)
	if !ok {
		return
	}
	body, err := localV2WorkflowAggregateBody(fields)
	if err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return
	}
	raw, err := s.hub.Request(r.Context(), r.PathValue("app"), protocol.GetWorkflowAggregatesRequest(body))
	if err != nil {
		localV2Failure(w, err)
		return
	}
	records, err := localV2InspectionPayload(raw)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	out := make([]map[string]any, 0, len(records))
	for _, record := range records {
		mapped, err := localV2WorkflowAggregateRecord(record)
		if err != nil {
			localV2Failure(w, err)
			return
		}
		out = append(out, mapped)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) localV2StepAggregates(w http.ResponseWriter, r *http.Request) {
	fields, ok := localV2InspectionAggregateRequest(w, r, localV2StepAggregateFields)
	if !ok {
		return
	}
	body, err := localV2StepAggregateBody(fields)
	if err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return
	}
	raw, err := s.hub.Request(r.Context(), r.PathValue("app"), protocol.GetStepAggregatesRequest(body))
	if err != nil {
		localV2Failure(w, err)
		return
	}
	records, err := localV2InspectionPayload(raw)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	out := make([]map[string]any, 0, len(records))
	for _, record := range records {
		mapped, err := localV2StepAggregateRecord(record)
		if err != nil {
			localV2Failure(w, err)
			return
		}
		out = append(out, mapped)
	}
	writeJSON(w, http.StatusOK, out)
}

func localV2ExportQuery(r *http.Request) (bool, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return false, errors.New("malformed query")
	}
	exportChildren := false
	for name, values := range query {
		if !utf8.ValidString(name) {
			return false, errors.New("malformed query")
		}
		for _, value := range values {
			if !utf8.ValidString(value) {
				return false, errors.New("malformed query")
			}
		}
		if name != "exportChildren" {
			return false, fmt.Errorf("unsupported query %q", name)
		}
		if len(values) != 1 {
			return false, errors.New("duplicate query \"exportChildren\"")
		}
		if values[0] != "true" && values[0] != "false" {
			return false, errors.New("exportChildren must be boolean")
		}
		exportChildren = values[0] == "true"
	}
	// The SDK wire field is required. The optional HTTP boolean has the natural
	// false value when omitted; this does not enable recursive child export.
	return exportChildren, nil
}

func localV2InspectionWorkflowExists(raw []byte, expectedID string) (bool, error) {
	fields, err := localV2RawObject(raw, "workflow response")
	if err != nil {
		return false, err
	}
	var base protocol.BaseResponse
	if err := json.Unmarshal(raw, &base); err != nil {
		return false, fmt.Errorf("decode response: %w", err)
	}
	if err := base.Err(); err != nil {
		return false, err
	}
	output, ok := fields["output"]
	if !ok {
		return false, errors.New("invalid executor workflow output: missing")
	}
	if bytes.Equal(bytes.TrimSpace(output), []byte("null")) {
		return false, nil
	}
	outputFields, err := localV2RawObject(output, "workflow output")
	if err != nil {
		return false, err
	}
	workflowID, err := localV2RequiredString(outputFields, "WorkflowUUID", "workflow output")
	if err != nil {
		return false, err
	}
	var workflow protocol.WorkflowsOutput
	if err := json.Unmarshal(output, &workflow); err != nil {
		return false, fmt.Errorf("invalid executor workflow output: %w", err)
	}
	if workflow.WorkflowUUID != workflowID || workflowID != expectedID {
		return false, errors.New("invalid executor workflow output: inconsistent WorkflowUUID")
	}
	return true, nil
}

func localV2ExportValue(raw []byte) (string, error) {
	fields, err := localV2RawObject(raw, "export response")
	if err != nil {
		return "", err
	}
	var base protocol.BaseResponse
	if err := json.Unmarshal(raw, &base); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if err := base.Err(); err != nil {
		return "", err
	}
	return localV2RequiredString(fields, "serialized_workflow", "export response")
}

func (s *Server) localV2ExportWorkflow(w http.ResponseWriter, r *http.Request) {
	if !localV2Allowed(w, r) {
		return
	}
	app, workflowID := r.PathValue("app"), r.PathValue("id")
	if err := localV2InspectionApp(app); err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return
	}
	if workflowID == "" || !utf8.ValidString(workflowID) {
		localV2Problem(w, http.StatusBadRequest, "invalid workflow ID")
		return
	}
	exportChildren, err := localV2ExportQuery(r)
	if err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return
	}
	// Python export raises for an absent workflow, but reports it through the
	// generic executor error channel. The official resource route distinguishes
	// that case with the same raw, blob-free existence boundary as related reads.
	existenceRaw, err := s.hub.Request(r.Context(), app, protocol.GetWorkflowRequest(workflowID, false, false))
	if err != nil {
		localV2Failure(w, err)
		return
	}
	exists, err := localV2InspectionWorkflowExists(existenceRaw, workflowID)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	if !exists {
		localV2Problem(w, http.StatusNotFound, "workflow not found")
		return
	}
	raw, err := s.hub.Request(r.Context(), app, protocol.ExportWorkflowRequest(workflowID, exportChildren))
	if err != nil {
		localV2Failure(w, err)
		return
	}
	serialized, err := localV2ExportValue(raw)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"serializedWorkflow": serialized})
}
