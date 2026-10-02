package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// DecodeObject retains field presence and rejects duplicate keys. Callers choose
// this stricter boundary without changing the tolerant SDK DTO unmarshalling.
func DecodeObject(raw []byte, name string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, fmt.Errorf("invalid executor %s: object required", name)
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("invalid executor %s: malformed key", name)
		}
		field, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("invalid executor %s: malformed key", name)
		}
		if _, duplicate := fields[field]; duplicate {
			return nil, fmt.Errorf("invalid executor %s: duplicate field %q", name, field)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("invalid executor %s.%s: malformed value", name, field)
		}
		fields[field] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("invalid executor %s: malformed object", name)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("invalid executor %s: trailing JSON", name)
	}
	return fields, nil
}

// Response reports an executor-side error from a decoded reply.
type Response interface{ Err() error }

// DecodeResponse preserves SDK fields and rejects replies missing their data payload.
func DecodeResponse(raw []byte, out Response) error {
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if err := out.Err(); err != nil {
		return err
	}
	// A 3.1 metadata-only refusal normally carries error_message, but a
	// BaseResponse-only reply must not become an empty successful read panel.
	key := ""
	switch out.(type) {
	case *ListWorkflowsResponse, *GetWorkflowResponse,
		*ListStepsResponse, *ListQueuesResponse, *GetQueueResponse,
		*ListSchedulesResponse, *GetScheduleResponse:
		key = "output"
	case *GetWorkflowEventsResponse:
		key = "events"
	case *GetWorkflowNotificationsResponse:
		key = "notifications"
	case *GetWorkflowStreamsResponse:
		key = "streams"
	}
	if key != "" {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		if _, ok := fields[key]; !ok {
			return errors.New("executor response data unavailable")
		}
	}
	return nil
}
