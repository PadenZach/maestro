package api

import (
	"context"
	"encoding/json"

	"github.com/zpaden/maestro/internal/protocol"
)

type aggregateResult interface {
	consoleRow() aggregateRow
}

// Both HTTP representations use the same executor read and response validation.
func (s *Server) readAggregates(ctx context.Context, app string, request protocol.Request, mapRecord func(json.RawMessage) (aggregateResult, error)) ([]aggregateResult, error) {
	raw, err := s.hub.Request(ctx, app, request)
	if err != nil {
		return nil, err
	}
	records, err := localV2InspectionPayload(raw)
	if err != nil {
		return nil, err
	}
	out := make([]aggregateResult, 0, len(records))
	for _, record := range records {
		mapped, err := mapRecord(record)
		if err != nil {
			return nil, err
		}
		out = append(out, mapped)
	}
	return out, nil
}
