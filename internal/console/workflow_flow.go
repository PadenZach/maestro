package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/PadenZach/maestro/internal/hub"
	"github.com/PadenZach/maestro/internal/protocol"
)

const (
	FlowStepPageSize = 50
	flowMaxOffset    = 1000
	flowStatusMaxIDs = 25
	flowReadTimeout  = 10 * time.Second
)

func flowOffset(query url.Values) (int, error) {
	values, exists := query["offset"]
	if !exists {
		return 0, nil
	}
	if len(values) != 1 {
		return 0, errors.New("offset must occur once")
	}
	n, err := strconv.Atoi(values[0])
	if err != nil || n < 0 || n > flowMaxOffset || n%FlowStepPageSize != 0 {
		return 0, fmt.Errorf("offset must be a multiple of %d between 0 and %d", FlowStepPageSize, flowMaxOffset)
	}
	return n, nil
}

func flowFailure(err error) (string, string) {
	if errors.Is(err, hub.ErrAppUnavailable) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "unavailable", err.Error()
	}
	return "error", err.Error()
}

// Each request names one workflow and reads one bounded page. Expansion belongs
// to the browser so cycles and family size cannot cause recursive server reads.
func (s *handler) handleWorkflowFlow(w http.ResponseWriter, r *http.Request) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err == nil {
		for key := range query {
			if key != "offset" && key != "details" {
				err = fmt.Errorf("unsupported query %q", key)
				break
			}
		}
	}
	var offset int
	details := true
	if err == nil {
		offset, err = flowOffset(query)
	}
	if err == nil {
		if values, exists := query["details"]; exists {
			if len(values) != 1 || (values[0] != "true" && values[0] != "false") {
				err = errors.New("details must be true or false and occur once")
			} else {
				details = values[0] == "true"
			}
		}
	}
	if err != nil {
		writeFlowProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	app, id := r.PathValue("app"), r.PathValue("id")
	f := NewFlow(app, id, offset)
	ctx, cancel := context.WithTimeout(r.Context(), flowReadTimeout)
	defer cancel()
	wf, err := s.hub.Workflow(ctx, app, id, false, false)
	if err != nil {
		f.State, f.Error = flowFailure(err)
	} else if wf == nil {
		f.State, f.Error = "missing", "Workflow not found."
	} else if wf.WorkflowUUID != id {
		f.State, f.Error = "error", "Executor returned an inconsistent workflow identity."
	} else {
		f.SetWorkflow(*wf)
		limit := FlowStepPageSize + 1
		steps, stepErr := s.hub.Steps(ctx, app, id, details, &limit, &offset)
		if stepErr == nil && steps == nil {
			stepErr = errors.New("executor step data unavailable")
		}
		if stepErr == nil && len(steps) > limit {
			stepErr = errors.New("executor exceeded the step page limit")
		}
		if stepErr != nil {
			f.State, f.Error = flowFailure(stepErr)
		} else {
			f.State = "ready"
			f.DetailsLoaded = details
			f.HasMore = len(steps) > FlowStepPageSize
			if f.HasMore {
				steps = steps[:FlowStepPageSize]
			}
			if f.HasMore && offset < flowMaxOffset {
				next := offset + FlowStepPageSize
				f.NextOffset = &next
			}
			f.Limited = f.HasMore && f.NextOffset == nil
			// The SDK orders pages by function ID. Keep recorded ordering explicit
			// within the page while retaining unnamed/unknown-ID records at the end.
			sort.SliceStable(steps, func(i, j int) bool {
				if steps[i].HasFunctionID() != steps[j].HasFunctionID() {
					return steps[i].HasFunctionID()
				}
				return steps[i].HasFunctionID() && steps[i].FunctionID < steps[j].FunctionID
			})
			for _, step := range steps {
				recorded := RecordedFlowStep(app, id, offset, step)
				if !details {
					// Some executors ignore read flags. A metadata discovery read
					// never promises that error evidence was requested or complete.
					recorded.HasError, recorded.ErrorKnown = false, false
				}
				f.Steps = append(f.Steps, recorded)
			}
		}
	}
	f.ReadAt = time.Now().UTC().Format(time.RFC3339Nano)
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, f)
}

type flowStatuses struct {
	State     string `json:"state"`
	Error     string `json:"error"`
	ReadAt    string `json:"readAt"`
	Workflows []Flow `json:"workflows"`
}

// Refresh active loaded nodes even when their initially selected parent is
// terminal. One LIST_WORKFLOWS query, no steps or payloads, at most 25 identities.
func (s *handler) handleWorkflowFlowStatus(w http.ResponseWriter, r *http.Request) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err == nil {
		for key := range query {
			if key != "workflow_id" {
				err = fmt.Errorf("unsupported query %q", key)
				break
			}
		}
	}
	ids := query["workflow_id"]
	if err == nil && (len(ids) == 0 || len(ids) > flowStatusMaxIDs) {
		err = fmt.Errorf("request between 1 and %d workflow IDs", flowStatusMaxIDs)
	}
	requested := map[string]bool{}
	if err == nil {
		for _, id := range ids {
			if id == "" || len(id) > 1024 || requested[id] {
				err = errors.New("workflow IDs must be nonempty, unique, and at most 1024 bytes")
				break
			}
			requested[id] = true
		}
	}
	if err != nil {
		writeFlowProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), flowReadTimeout)
	defer cancel()
	limit := len(ids)
	rows, err := s.hub.Workflows(ctx, r.PathValue("app"), protocol.ListWorkflowsBody{WorkflowUUIDs: ids, Limit: &limit, LoadInput: false, LoadOutput: false})
	if err == nil && rows == nil {
		err = errors.New("executor workflow status data unavailable")
	}
	found := map[string]protocol.WorkflowsOutput{}
	if err == nil {
		for _, row := range rows {
			if !requested[row.WorkflowUUID] || found[row.WorkflowUUID].WorkflowUUID != "" {
				err = errors.New("executor returned inconsistent workflow identities")
				break
			}
			found[row.WorkflowUUID] = row
		}
	}
	result := flowStatuses{State: "ready", ReadAt: time.Now().UTC().Format(time.RFC3339Nano), Workflows: []Flow{}}
	if err != nil {
		result.State, result.Error = flowFailure(err)
	}
	for _, id := range ids {
		f := NewFlow(r.PathValue("app"), id, 0)
		f.State, f.Error, f.ReadAt = result.State, result.Error, result.ReadAt
		if err == nil {
			if row, exists := found[id]; exists {
				f.SetWorkflow(row)
			} else {
				f.State, f.Error = "missing", "Workflow not found."
			}
		}
		result.Workflows = append(result.Workflows, f)
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, result)
}

func writeFlowProblem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Type   string `json:"type"`
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}{"about:blank", http.StatusText(status), status, detail})
}
