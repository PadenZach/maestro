package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/zpaden/maestro/internal/protocol"
	"github.com/zpaden/maestro/internal/web"
)

type workflowInspection struct {
	WorkflowID string
	StepID     string
	IsStep     bool
	Title      string
	FullURL    string
	Fields     []web.InspectionField
	Related    detailData
}

// The drawer is a read-only view of the same SDK fields as the full page.
// Selection stays in the browser; every read names its own workflow and step.
func (s *Server) handleWorkflowInspection(w http.ResponseWriter, r *http.Request) {
	app, id := r.PathValue("app"), r.PathValue("id")
	query := r.URL.Query()
	stepValues, isStep := query["step"]
	var stepID int
	var pageOffset *int
	if isStep {
		var err error
		if len(stepValues) == 1 {
			stepID, err = strconv.Atoi(stepValues[0])
		}
		if len(stepValues) != 1 || err != nil || stepID < 0 {
			partialError(w, fmt.Errorf("invalid step identifier"))
			return
		}
		if _, hasOffset := query["offset"]; hasOffset {
			offset, err := flowOffset(query)
			if err != nil {
				partialError(w, err)
				return
			}
			pageOffset = &offset
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), flowReadTimeout)
	defer cancel()
	wf, err := s.readWorkflow(ctx, app, id, !isStep, !isStep)
	if err != nil {
		partialError(w, err)
		return
	}
	if wf == nil {
		partialError(w, fmt.Errorf("workflow %q not found", id))
		return
	}
	if wf.WorkflowUUID != id {
		partialError(w, errors.New("executor returned an inconsistent workflow identity"))
		return
	}
	data := workflowInspection{WorkflowID: id, Title: "Workflow", FullURL: web.WorkflowURL(app, id)}
	if isStep {
		step, found, limited, err := s.readInspectionStep(ctx, app, id, stepID, pageOffset)
		if err != nil {
			partialError(w, err)
			return
		}
		if found {
			data.IsStep, data.StepID, data.Title = true, strconv.Itoa(stepID), step.FunctionName
			data.Fields = web.StepFields(step)
			s.web.Partial(w, "workflow_inspection", data)
			return
		}
		if limited {
			partialMessage(w, fmt.Sprintf("Step %d in workflow %s was not found within the bounded inspection read. Open its Flow page and select the step there.", stepID, id))
			return
		}
		partialMessage(w, fmt.Sprintf("Step %d in workflow %s is unavailable after refresh.", stepID, id))
		return
	}
	data.Fields = web.WorkflowFields(wf)
	data.Related = s.readWorkflowRelated(ctx, app, id)
	s.web.Partial(w, "workflow_inspection", data)
}

// Flow links identify the page where the record was observed. Older Timeline
// links get a bounded fallback; never issue an unlimited payload read.
func (s *Server) readInspectionStep(ctx context.Context, app, workflowID string, stepID int, pageOffset *int) (protocol.WorkflowSteps, bool, bool, error) {
	const maxFallbackPages = 20
	limit, offset, pages := FlowStepPageSize, 0, maxFallbackPages
	if pageOffset != nil {
		offset, pages = *pageOffset, 1
	}
	for page := 0; page < pages; page++ {
		steps, err := s.readSteps(ctx, app, workflowID, true, &limit, &offset)
		if err != nil {
			return protocol.WorkflowSteps{}, false, false, err
		}
		if steps == nil {
			return protocol.WorkflowSteps{}, false, false, errors.New("executor step data unavailable")
		}
		if len(steps) > limit {
			return protocol.WorkflowSteps{}, false, false, errors.New("executor exceeded the step page limit")
		}
		for _, step := range steps {
			if step.HasFunctionID() && step.FunctionID == stepID {
				return step, true, false, nil
			}
		}
		if pageOffset != nil || len(steps) < limit {
			return protocol.WorkflowSteps{}, false, false, nil
		}
		offset += limit
	}
	return protocol.WorkflowSteps{}, false, true, nil
}

func (s *Server) readWorkflowRelated(ctx context.Context, app, id string) detailData {
	var events protocol.GetWorkflowEventsResponse
	eventsErr := s.dispatch(ctx, app, protocol.GetWorkflowEventsRequest(id), &events)
	var notes protocol.GetWorkflowNotificationsResponse
	notesErr := s.dispatch(ctx, app, protocol.GetWorkflowNotificationsRequest(id), &notes)
	var streams protocol.GetWorkflowStreamsResponse
	streamsErr := s.dispatch(ctx, app, protocol.GetWorkflowStreamsRequest(id), &streams)
	return detailData{
		Events: events.Events, Notifications: notes.Notifications, Streams: streams.Streams,
		EventsError: htmlErrorText(eventsErr), NotificationsError: htmlErrorText(notesErr), StreamsError: htmlErrorText(streamsErr),
	}
}
