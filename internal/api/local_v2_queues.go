package api

import (
	"fmt"
	"math"
	"net/http"
	"net/url"

	"github.com/zpaden/maestro/internal/protocol"
)

// The official Queue schema differs from executor QueueOutput, including the
// plural Secs suffix. Nullable values stay null, including legacy SDK fields.
func localV2Queue(q protocol.QueueOutput) (map[string]any, error) {
	if !q.HasRequiredFields() {
		return nil, fmt.Errorf("queue missing non-nullable fields")
	}
	for _, n := range []*int{q.Concurrency, q.WorkerConcurrency, q.RateLimitMax, q.PartitionConcurrency, q.PartitionWorkerConcurrency, q.PartitionRateLimitMax} {
		if n != nil && (int64(*n) < math.MinInt32 || int64(*n) > math.MaxInt32) {
			return nil, fmt.Errorf("queue integer outside int32 range")
		}
	}
	return map[string]any{
		"name": q.Name, "concurrency": q.Concurrency, "workerConcurrency": q.WorkerConcurrency,
		"rateLimitMax": q.RateLimitMax, "rateLimitPeriodSecs": q.RateLimitPeriodSec,
		"priorityEnabled": q.PriorityEnabled, "partitionQueue": q.PartitionQueue,
		"pollingIntervalSecs": q.PollingIntervalSec, "applicationName": q.ApplicationName,
		"partitionConcurrency": q.PartitionConcurrency, "partitionWorkerConcurrency": q.PartitionWorkerConcurrency,
		"partitionRateLimitMax": q.PartitionRateLimitMax, "partitionRateLimitPeriodSecs": q.PartitionRateLimitPeriodSec,
	}, nil
}

func localV2NoQuery(w http.ResponseWriter, r *http.Request) bool {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(query) != 0 {
		localV2Problem(w, 400, "query parameters are unsupported or malformed")
		return false
	}
	return true
}

func (s *Server) localV2Queues(w http.ResponseWriter, r *http.Request) {
	if !localV2Allowed(w, r) || !localV2NoQuery(w, r) {
		return
	}
	var resp protocol.ListQueuesResponse
	if err := s.dispatch(r.Context(), r.PathValue("app"), protocol.ListQueuesRequest(), &resp); err != nil {
		localV2Failure(w, err)
		return
	}
	if resp.Output == nil {
		localV2Problem(w, 502, "queue list unavailable")
		return
	}
	rows := make([]map[string]any, 0, len(resp.Output))
	for _, q := range resp.Output {
		row, err := localV2Queue(q)
		if err != nil {
			localV2Failure(w, err)
			return
		}
		rows = append(rows, row)
	}
	writeJSON(w, 200, rows)
}

func (s *Server) localV2GetQueue(w http.ResponseWriter, r *http.Request) {
	if !localV2Allowed(w, r) || !localV2NoQuery(w, r) {
		return
	}
	var resp protocol.GetQueueResponse
	if err := s.dispatch(r.Context(), r.PathValue("app"), protocol.GetQueueRequest(r.PathValue("name")), &resp); err != nil {
		localV2Failure(w, err)
		return
	}
	if resp.Output == nil {
		localV2Problem(w, 404, "queue not found")
		return
	}
	row, err := localV2Queue(*resp.Output)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	writeJSON(w, 200, row)
}
