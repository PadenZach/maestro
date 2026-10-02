package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/PadenZach/maestro/internal/protocol"
)

type WorkflowAggregate struct {
	Group             map[string]*string `json:"group"`
	Count             *int64             `json:"count,omitempty"`
	MinCreatedAt      *string            `json:"minCreatedAt,omitempty" format:"date-time"`
	MaxQueueWaitMS    *int64             `json:"maxQueueWaitMs,omitempty"`
	MaxTotalLatencyMS *int64             `json:"maxTotalLatencyMs,omitempty"`
}

type StepAggregate struct {
	Group         map[string]*string `json:"group"`
	Count         *int64             `json:"count,omitempty"`
	MaxDurationMS *int64             `json:"maxDurationMs,omitempty"`
}

type WorkflowAggregatesBody struct {
	workflowFilters
	GroupByStatus           bool  `json:"groupByStatus,omitempty"`
	GroupByWorkflowName     bool  `json:"groupByWorkflowName,omitempty"`
	GroupByQueueName        bool  `json:"groupByQueueName,omitempty"`
	GroupByExecutorID       bool  `json:"groupByExecutorId,omitempty"`
	GroupByAppVersion       bool  `json:"groupByAppVersion,omitempty"`
	GroupByApplicationName  bool  `json:"groupByApplicationName,omitempty"`
	SelectCount             bool  `json:"selectCount,omitempty"`
	SelectMinCreatedAt      bool  `json:"selectMinCreatedAt,omitempty"`
	SelectMaxQueueWaitMS    bool  `json:"selectMaxQueueWaitMs,omitempty"`
	SelectMaxTotalLatencyMS bool  `json:"selectMaxTotalLatencyMs,omitempty"`
	TimeBucketSizeMS        int64 `json:"timeBucketSizeMs,omitempty"`
}

type StepAggregatesBody struct {
	GroupByFunctionName bool     `json:"groupByFunctionName,omitempty"`
	GroupByStatus       bool     `json:"groupByStatus,omitempty"`
	SelectCount         bool     `json:"selectCount,omitempty"`
	SelectMaxDurationMS bool     `json:"selectMaxDurationMs,omitempty"`
	TimeBucketSizeMS    int64    `json:"timeBucketSizeMs,omitempty"`
	Status              []string `json:"status,omitempty" nullable:"false"`
	StepName            []string `json:"stepName,omitempty" nullable:"false"`
	WorkflowIDPrefix    []string `json:"workflowIdPrefix,omitempty" nullable:"false"`
	CompletedAfter      string   `json:"completedAfter,omitempty" format:"date-time"`
	CompletedBefore     string   `json:"completedBefore,omitempty" format:"date-time"`
}

var workflowAggregateFields = requestFields[WorkflowAggregatesBody]()

var stepAggregateFields = requestFields[StepAggregatesBody]()

// aggregateObject preserves omitted, null, false, zero, and empty values
// while rejecting duplicate and unknown top-level fields before dispatch.
func aggregateObject(raw []byte, allowed map[string]struct{}) (map[string]json.RawMessage, error) {
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

func aggregateBool(raw json.RawMessage, name string) (*bool, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("%s must be boolean", name)
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%s must be boolean", name)
	}
	return &value, nil
}

func aggregateInt64(raw json.RawMessage, name string) (*int64, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("%s must be an int64", name)
	}
	var value int64
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, fmt.Errorf("%s must be an int64", name)
	}
	return &value, nil
}

func aggregateDate(raw json.RawMessage, name string) (*string, error) {
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

func aggregateStrings(raw json.RawMessage, name string, nullable bool) ([]string, error) {
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

func aggregateAttributes(raw json.RawMessage) (map[string]any, error) {
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

func parseWorkflowAggregateBody(fields map[string]json.RawMessage) (protocol.WorkflowAggregatesBody, error) {
	var body protocol.WorkflowAggregatesBody
	for name, raw := range fields {
		var err error
		switch name {
		case "groupByStatus":
			body.GroupByStatus, err = aggregateBool(raw, name)
		case "groupByWorkflowName":
			body.GroupByName, err = aggregateBool(raw, name)
		case "groupByQueueName":
			body.GroupByQueueName, err = aggregateBool(raw, name)
		case "groupByExecutorId":
			body.GroupByExecutorID, err = aggregateBool(raw, name)
		case "groupByAppVersion":
			body.GroupByApplicationVersion, err = aggregateBool(raw, name)
		case "groupByApplicationName":
			body.GroupByApplicationName, err = aggregateBool(raw, name)
		case "selectCount":
			body.SelectCount, err = aggregateBool(raw, name)
		case "selectMinCreatedAt":
			body.SelectMinCreatedAt, err = aggregateBool(raw, name)
		case "selectMaxQueueWaitMs":
			body.SelectMaxQueueWaitMS, err = aggregateBool(raw, name)
		case "selectMaxTotalLatencyMs":
			body.SelectMaxTotalLatencyMS, err = aggregateBool(raw, name)
		case "timeBucketSizeMs":
			body.TimeBucketSizeMS, err = aggregateInt64(raw, name)
		case "status":
			body.Status, err = aggregateStrings(raw, name, true)
		case "startTime":
			body.StartTime, err = aggregateDate(raw, name)
		case "endTime":
			body.EndTime, err = aggregateDate(raw, name)
		case "completedAfter":
			body.CompletedAfter, err = aggregateDate(raw, name)
		case "completedBefore":
			body.CompletedBefore, err = aggregateDate(raw, name)
		case "dequeuedAfter":
			body.DequeuedAfter, err = aggregateDate(raw, name)
		case "dequeuedBefore":
			body.DequeuedBefore, err = aggregateDate(raw, name)
		case "workflowName":
			body.Name, err = aggregateStrings(raw, name, true)
		case "appVersion":
			body.AppVersion, err = aggregateStrings(raw, name, true)
		case "executorId":
			body.ExecutorID, err = aggregateStrings(raw, name, true)
		case "queueName":
			body.QueueName, err = aggregateStrings(raw, name, true)
		case "workflowIdPrefix":
			body.WorkflowIDPrefix, err = aggregateStrings(raw, name, true)
		case "workflowIds":
			body.WorkflowIDs, err = aggregateStrings(raw, name, true)
		case "forkedFrom":
			body.ForkedFrom, err = aggregateStrings(raw, name, true)
		case "parentWorkflowId":
			body.ParentWorkflowID, err = aggregateStrings(raw, name, true)
		case "user":
			body.User, err = aggregateStrings(raw, name, true)
		case "scheduleName":
			body.ScheduleName, err = aggregateStrings(raw, name, true)
		case "wasForkedFrom":
			body.WasForkedFrom, err = aggregateBool(raw, name)
		case "hasParent":
			body.HasParent, err = aggregateBool(raw, name)
		case "attributes":
			body.Attributes, err = aggregateAttributes(raw)
		}
		if err != nil {
			return body, err
		}
	}
	return body, nil
}

func parseStepAggregateBody(fields map[string]json.RawMessage) (protocol.StepAggregatesBody, error) {
	var body protocol.StepAggregatesBody
	for name, raw := range fields {
		var err error
		switch name {
		case "groupByFunctionName":
			body.GroupByFunctionName, err = aggregateBool(raw, name)
		case "groupByStatus":
			body.GroupByStatus, err = aggregateBool(raw, name)
		case "selectCount":
			body.SelectCount, err = aggregateBool(raw, name)
		case "selectMaxDurationMs":
			body.SelectMaxDurationMS, err = aggregateBool(raw, name)
		case "timeBucketSizeMs":
			body.TimeBucketSizeMS, err = aggregateInt64(raw, name)
		case "status":
			body.Status, err = aggregateStrings(raw, name, false)
		case "stepName":
			body.FunctionName, err = aggregateStrings(raw, name, false)
		case "workflowIdPrefix":
			body.WorkflowIDPrefix, err = aggregateStrings(raw, name, false)
		case "completedAfter":
			body.CompletedAfter, err = aggregateDate(raw, name)
		case "completedBefore":
			body.CompletedBefore, err = aggregateDate(raw, name)
		}
		if err != nil {
			return body, err
		}
	}
	return body, nil
}

func (s *handler) aggregateRequest(w http.ResponseWriter, r *http.Request, allowed map[string]struct{}) (map[string]json.RawMessage, bool) {
	if !s.allowOrganization(w, r) || !noQuery(w, r) {
		return nil, false
	}
	if err := validateApp(r.PathValue("app")); err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "cannot read aggregate body")
		return nil, false
	}
	fields, err := aggregateObject(raw, allowed)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	return fields, true
}

func (s *handler) workflowAggregates(w http.ResponseWriter, r *http.Request) {
	fields, ok := s.aggregateRequest(w, r, workflowAggregateFields)
	if !ok {
		return
	}
	body, err := parseWorkflowAggregateBody(fields)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	serveAggregates(s, w, r, protocol.GetWorkflowAggregatesRequest(body), workflowAggregateRecord)
}

func (s *handler) stepAggregates(w http.ResponseWriter, r *http.Request) {
	fields, ok := s.aggregateRequest(w, r, stepAggregateFields)
	if !ok {
		return
	}
	body, err := parseStepAggregateBody(fields)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	serveAggregates(s, w, r, protocol.GetStepAggregatesRequest(body), stepAggregateRecord)
}

func serveAggregates[T any](s *handler, w http.ResponseWriter, r *http.Request, request protocol.Request, decode func(json.RawMessage) (*T, error)) {
	raw, err := s.hub.Request(r.Context(), r.PathValue("app"), request)
	if err != nil {
		writeFailure(w, err)
		return
	}
	records, err := protocol.DecodeAggregatePayload(raw)
	if err != nil {
		writeFailure(w, err)
		return
	}
	out := make([]*T, 0, len(records))
	for _, raw := range records {
		row, err := decode(raw)
		if err != nil {
			writeFailure(w, err)
			return
		}
		out = append(out, row)
	}
	writeJSON(w, http.StatusOK, out)
}

func workflowAggregateRecord(raw json.RawMessage) (*WorkflowAggregate, error) {
	row, err := protocol.DecodeWorkflowAggregate(raw)
	if err != nil {
		return nil, err
	}
	out := &WorkflowAggregate{Group: row.Group, Count: row.Count, MaxQueueWaitMS: row.MaxQueueWaitMS, MaxTotalLatencyMS: row.MaxTotalLatencyMS}
	if row.MinCreatedAt != nil {
		value := time.UnixMilli(*row.MinCreatedAt).UTC().Format("2006-01-02T15:04:05.000Z07:00")
		out.MinCreatedAt = &value
	}
	return out, nil
}

func stepAggregateRecord(raw json.RawMessage) (*StepAggregate, error) {
	row, err := protocol.DecodeStepAggregate(raw)
	if err != nil {
		return nil, err
	}
	return &StepAggregate{Group: row.Group, Count: row.Count, MaxDurationMS: row.MaxDurationMS}, nil
}
