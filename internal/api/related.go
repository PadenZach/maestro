package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/PadenZach/maestro/internal/protocol"
)

var appName = regexp.MustCompile(`^[a-z0-9-_]+$`)

func (s *handler) relatedRequest(w http.ResponseWriter, r *http.Request) (app, workflowID string, ok bool) {
	if !s.allowOrganization(w, r) || !noQuery(w, r) {
		return "", "", false
	}
	app, workflowID = r.PathValue("app"), r.PathValue("id")
	if err := validateApp(app); err != nil {
		writeProblem(w, http.StatusBadRequest, "invalid application name")
		return "", "", false
	}
	if workflowID == "" || !utf8.ValidString(workflowID) {
		writeProblem(w, http.StatusBadRequest, "invalid workflow ID")
		return "", "", false
	}
	return app, workflowID, true
}

func requiredString(fields map[string]json.RawMessage, field, record string) (string, error) {
	raw, ok := fields[field]
	if !ok || bytes.Equal(raw, []byte("null")) {
		return "", fmt.Errorf("invalid executor %s: missing or null %s", record, field)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("invalid executor %s.%s: string required", record, field)
	}
	return value, nil
}

func relatedPayload(raw []byte, field string) ([]json.RawMessage, error) {
	fields, err := protocol.DecodeObject(raw, "related response")
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
	payload, ok := fields[field]
	if !ok || bytes.Equal(payload, []byte("null")) {
		return nil, fmt.Errorf("invalid executor %s output: missing or null", field)
	}
	var records []json.RawMessage
	if err := json.Unmarshal(payload, &records); err != nil {
		return nil, fmt.Errorf("invalid executor %s output: array required", field)
	}
	return records, nil
}

func workflowExists(raw []byte, expectedID string) (bool, error) {
	fields, err := protocol.DecodeObject(raw, "workflow response")
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
	outputFields, err := protocol.DecodeObject(output, "workflow output")
	if err != nil {
		return false, err
	}
	workflowID, err := requiredString(outputFields, "WorkflowUUID", "workflow output")
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

func (s *handler) readRelated(r *http.Request, app, workflowID, field string, request protocol.Request) ([]json.RawMessage, int, error) {
	// The related-data handlers return an empty collection for both an absent
	// workflow and an existing workflow with no data. Check existence without
	// opaque blobs and without applying the strict official Workflow mapper.
	existenceRaw, err := s.hub.Request(r.Context(), app, protocol.GetWorkflowRequest(workflowID, false, false))
	if err != nil {
		return nil, 0, err
	}
	exists, err := workflowExists(existenceRaw, workflowID)
	if err != nil {
		return nil, 0, err
	}
	if !exists {
		return nil, http.StatusNotFound, errors.New("workflow not found")
	}
	raw, err := s.hub.Request(r.Context(), app, request)
	if err != nil {
		return nil, 0, err
	}
	records, err := relatedPayload(raw, field)
	return records, 0, err
}

type Event struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func eventRecord(raw json.RawMessage) (Event, error) {
	fields, err := protocol.DecodeObject(raw, "event")
	if err != nil {
		return Event{}, err
	}
	if len(fields) != 2 {
		return Event{}, errors.New("invalid executor event: unexpected fields")
	}
	key, err := requiredString(fields, "key", "event")
	if err != nil {
		return Event{}, err
	}
	value, err := requiredString(fields, "value", "event")
	if err != nil {
		return Event{}, err
	}
	return Event{Key: key, Value: value}, nil
}

type Notification struct {
	Topic     *string `json:"topic"`
	Message   string  `json:"message"`
	CreatedAt string  `json:"createdAt" format:"date-time"`
	Consumed  bool    `json:"consumed"`
}

func notificationTime(milliseconds int64) (string, error) {
	const (
		minRFC3339Milliseconds int64 = -62135596800000
		maxRFC3339Milliseconds int64 = 253402300799999
	)
	if milliseconds < minRFC3339Milliseconds || milliseconds > maxRFC3339Milliseconds {
		return "", errors.New("invalid executor notification.created_at_epoch_ms: timestamp out of range")
	}
	seconds, remainder := milliseconds/1000, milliseconds%1000
	return time.Unix(seconds, remainder*int64(time.Millisecond)).UTC().Format("2006-01-02T15:04:05.000Z07:00"), nil
}

func notificationRecord(raw json.RawMessage) (Notification, error) {
	fields, err := protocol.DecodeObject(raw, "notification")
	if err != nil {
		return Notification{}, err
	}
	if len(fields) != 4 {
		return Notification{}, errors.New("invalid executor notification: unexpected fields")
	}
	topicRaw, ok := fields["topic"]
	if !ok {
		return Notification{}, errors.New("invalid executor notification: missing topic")
	}
	var topic *string
	if !bytes.Equal(topicRaw, []byte("null")) {
		var value string
		if err := json.Unmarshal(topicRaw, &value); err != nil {
			return Notification{}, errors.New("invalid executor notification.topic: string or null required")
		}
		topic = &value
	}
	message, err := requiredString(fields, "message", "notification")
	if err != nil {
		return Notification{}, err
	}
	createdRaw, ok := fields["created_at_epoch_ms"]
	if !ok || bytes.Equal(createdRaw, []byte("null")) {
		return Notification{}, errors.New("invalid executor notification: missing or null created_at_epoch_ms")
	}
	var milliseconds int64
	if err := json.Unmarshal(createdRaw, &milliseconds); err != nil {
		return Notification{}, errors.New("invalid executor notification.created_at_epoch_ms: integer required")
	}
	createdAt, err := notificationTime(milliseconds)
	if err != nil {
		return Notification{}, err
	}
	consumedRaw, ok := fields["consumed"]
	if !ok || bytes.Equal(consumedRaw, []byte("null")) {
		return Notification{}, errors.New("invalid executor notification: missing or null consumed")
	}
	var consumed bool
	if err := json.Unmarshal(consumedRaw, &consumed); err != nil {
		return Notification{}, errors.New("invalid executor notification.consumed: boolean required")
	}
	return Notification{Topic: topic, Message: message, CreatedAt: createdAt, Consumed: consumed}, nil
}

type StreamEntry struct {
	Key    string   `json:"key"`
	Values []string `json:"values" nullable:"false"`
}

func streamRecord(raw json.RawMessage) (StreamEntry, error) {
	fields, err := protocol.DecodeObject(raw, "stream entry")
	if err != nil {
		return StreamEntry{}, err
	}
	if len(fields) != 2 {
		return StreamEntry{}, errors.New("invalid executor stream entry: unexpected fields")
	}
	key, err := requiredString(fields, "key", "stream entry")
	if err != nil {
		return StreamEntry{}, err
	}
	valuesRaw, ok := fields["values"]
	if !ok || bytes.Equal(valuesRaw, []byte("null")) {
		return StreamEntry{}, errors.New("invalid executor stream entry: missing or null values")
	}
	var rawValues []json.RawMessage
	if err := json.Unmarshal(valuesRaw, &rawValues); err != nil {
		return StreamEntry{}, errors.New("invalid executor stream entry.values: array required")
	}
	values := make([]string, len(rawValues))
	for i, rawValue := range rawValues {
		if bytes.Equal(rawValue, []byte("null")) || json.Unmarshal(rawValue, &values[i]) != nil {
			return StreamEntry{}, fmt.Errorf("invalid executor stream entry.values[%d]: string required", i)
		}
	}
	return StreamEntry{Key: key, Values: values}, nil
}

func (s *handler) events(w http.ResponseWriter, r *http.Request) {
	serveRelated(s, w, r, "events", protocol.GetWorkflowEventsRequest(r.PathValue("id")), eventRecord)
}

func (s *handler) notifications(w http.ResponseWriter, r *http.Request) {
	serveRelated(s, w, r, "notifications", protocol.GetWorkflowNotificationsRequest(r.PathValue("id")), notificationRecord)
}

func (s *handler) streams(w http.ResponseWriter, r *http.Request) {
	serveRelated(s, w, r, "streams", protocol.GetWorkflowStreamsRequest(r.PathValue("id")), streamRecord)
}

func serveRelated[T any](s *handler, w http.ResponseWriter, r *http.Request, field string, request protocol.Request, decode func(json.RawMessage) (T, error)) {
	app, workflowID, ok := s.relatedRequest(w, r)
	if !ok {
		return
	}
	records, status, err := s.readRelated(r, app, workflowID, field, request)
	if err != nil {
		if status == http.StatusNotFound {
			writeProblem(w, status, err.Error())
		} else {
			writeFailure(w, err)
		}
		return
	}
	out := make([]T, 0, len(records))
	for _, raw := range records {
		record, err := decode(raw)
		if err != nil {
			writeFailure(w, err)
			return
		}
		out = append(out, record)
	}
	writeJSON(w, http.StatusOK, out)
}
