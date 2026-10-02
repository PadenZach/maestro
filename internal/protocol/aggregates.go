package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

func DecodeAggregatePayload(raw []byte) ([]json.RawMessage, error) {
	fields, err := DecodeObject(raw, "aggregate response")
	if err != nil {
		return nil, err
	}
	var base BaseResponse
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

func decodeAggregateGroup(raw json.RawMessage, record string) (map[string]*string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("invalid executor %s.group: object required", record)
	}
	fields, err := DecodeObject(raw, record+".group")
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

func decodeAggregateInt(fields map[string]json.RawMessage, field, record string) (*int64, error) {
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

func DecodeWorkflowAggregate(raw json.RawMessage) (*WorkflowAggregate, error) {
	const record = "workflow aggregate"
	fields, err := DecodeObject(raw, record)
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
	group, err := decodeAggregateGroup(groupRaw, record)
	if err != nil {
		return nil, err
	}
	count, err := decodeAggregateInt(fields, "count", record)
	if err != nil {
		return nil, err
	}
	minCreatedAt, err := decodeAggregateInt(fields, "min_created_at", record)
	if err != nil {
		return nil, err
	}
	maxQueueWaitMS, err := decodeAggregateInt(fields, "max_queue_wait_ms", record)
	if err != nil {
		return nil, err
	}
	maxTotalLatencyMS, err := decodeAggregateInt(fields, "max_total_latency_ms", record)
	if err != nil {
		return nil, err
	}
	out := &WorkflowAggregate{Group: group, Count: count, MaxQueueWaitMS: maxQueueWaitMS, MaxTotalLatencyMS: maxTotalLatencyMS}
	if minCreatedAt != nil && (*minCreatedAt < -62135596800000 || *minCreatedAt > 253402300799999) {
		return nil, fmt.Errorf("invalid executor %s.min_created_at: timestamp out of range", record)
	}
	out.MinCreatedAt = minCreatedAt
	return out, nil
}

func DecodeStepAggregate(raw json.RawMessage) (*StepAggregate, error) {
	const record = "step aggregate"
	fields, err := DecodeObject(raw, record)
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
	group, err := decodeAggregateGroup(groupRaw, record)
	if err != nil {
		return nil, err
	}
	count, err := decodeAggregateInt(fields, "count", record)
	if err != nil {
		return nil, err
	}
	maxDurationMS, err := decodeAggregateInt(fields, "max_duration_ms", record)
	if err != nil {
		return nil, err
	}
	out := &StepAggregate{Group: group, Count: count, MaxDurationMS: maxDurationMS}
	return out, nil
}
