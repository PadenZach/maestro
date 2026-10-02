package api

import (
	"fmt"
	"math"
	"net/http"
	"net/url"

	"github.com/PadenZach/maestro/internal/protocol"
)

type Queue struct {
	Name                         string   `json:"name"`
	Concurrency                  *int     `json:"concurrency" format:"int32"`
	WorkerConcurrency            *int     `json:"workerConcurrency" format:"int32"`
	RateLimitMax                 *int     `json:"rateLimitMax" format:"int32"`
	RateLimitPeriodSecs          *float64 `json:"rateLimitPeriodSecs"`
	PriorityEnabled              bool     `json:"priorityEnabled"`
	PartitionQueue               bool     `json:"partitionQueue"`
	PollingIntervalSecs          float64  `json:"pollingIntervalSecs"`
	ApplicationName              *string  `json:"applicationName"`
	PartitionConcurrency         *int     `json:"partitionConcurrency" format:"int32"`
	PartitionWorkerConcurrency   *int     `json:"partitionWorkerConcurrency" format:"int32"`
	PartitionRateLimitMax        *int     `json:"partitionRateLimitMax" format:"int32"`
	PartitionRateLimitPeriodSecs *float64 `json:"partitionRateLimitPeriodSecs"`
}

// The official Queue schema differs from executor QueueOutput, including the
// plural Secs suffix. Nullable values stay null, including legacy SDK fields.
func queueResponse(q protocol.QueueOutput) (*Queue, error) {
	if !q.HasRequiredFields() {
		return nil, fmt.Errorf("queue missing non-nullable fields")
	}
	for _, n := range []*int{q.Concurrency, q.WorkerConcurrency, q.RateLimitMax, q.PartitionConcurrency, q.PartitionWorkerConcurrency, q.PartitionRateLimitMax} {
		if n != nil && (int64(*n) < math.MinInt32 || int64(*n) > math.MaxInt32) {
			return nil, fmt.Errorf("queue integer outside int32 range")
		}
	}
	return &Queue{
		Name: q.Name, Concurrency: q.Concurrency, WorkerConcurrency: q.WorkerConcurrency,
		RateLimitMax: q.RateLimitMax, RateLimitPeriodSecs: q.RateLimitPeriodSec,
		PriorityEnabled: q.PriorityEnabled, PartitionQueue: q.PartitionQueue,
		PollingIntervalSecs: q.PollingIntervalSec, ApplicationName: q.ApplicationName,
		PartitionConcurrency: q.PartitionConcurrency, PartitionWorkerConcurrency: q.PartitionWorkerConcurrency,
		PartitionRateLimitMax: q.PartitionRateLimitMax, PartitionRateLimitPeriodSecs: q.PartitionRateLimitPeriodSec,
	}, nil
}

func noQuery(w http.ResponseWriter, r *http.Request) bool {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(query) != 0 {
		writeProblem(w, 400, "query parameters are unsupported or malformed")
		return false
	}
	return true
}

func (s *handler) listQueues(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrganization(w, r) || !noQuery(w, r) {
		return
	}
	var resp protocol.ListQueuesResponse
	if err := s.hub.Call(r.Context(), r.PathValue("app"), protocol.ListQueuesRequest(), &resp); err != nil {
		writeFailure(w, err)
		return
	}
	if resp.Output == nil {
		writeProblem(w, 502, "queue list unavailable")
		return
	}
	rows := make([]*Queue, 0, len(resp.Output))
	for _, q := range resp.Output {
		row, err := queueResponse(q)
		if err != nil {
			writeFailure(w, err)
			return
		}
		rows = append(rows, row)
	}
	writeJSON(w, 200, rows)
}

func (s *handler) getQueue(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrganization(w, r) || !noQuery(w, r) {
		return
	}
	var resp protocol.GetQueueResponse
	if err := s.hub.Call(r.Context(), r.PathValue("app"), protocol.GetQueueRequest(r.PathValue("name")), &resp); err != nil {
		writeFailure(w, err)
		return
	}
	if resp.Output == nil {
		writeProblem(w, 404, "queue not found")
		return
	}
	row, err := queueResponse(*resp.Output)
	if err != nil {
		writeFailure(w, err)
		return
	}
	writeJSON(w, 200, row)
}
