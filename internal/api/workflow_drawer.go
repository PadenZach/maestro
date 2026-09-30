package api

import (
	"context"
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
	if isStep {
		var err error
		if len(stepValues) == 1 {
			stepID, err = strconv.Atoi(stepValues[0])
		}
		if len(stepValues) != 1 || err != nil || stepID < 0 {
			partialError(w, fmt.Errorf("invalid step identifier"))
			return
		}
	}
	wf, err := s.readWorkflow(r.Context(), app, id, !isStep, !isStep)
	if err != nil {
		partialError(w, err)
		return
	}
	if wf == nil {
		partialError(w, fmt.Errorf("workflow %q not found", id))
		return
	}
	data := workflowInspection{WorkflowID: id, Title: "Workflow", FullURL: web.WorkflowURL(app, id)}
	if isStep {
		steps, err := s.readSteps(r.Context(), app, id, true, nil, nil)
		if err != nil {
			partialError(w, err)
			return
		}
		for _, step := range steps {
			if step.HasFunctionID() && step.FunctionID == stepID {
				data.IsStep, data.StepID, data.Title = true, strconv.Itoa(stepID), step.FunctionName
				data.Fields = web.StepFields(step)
				s.web.Partial(w, "workflow_inspection", data)
				return
			}
		}
		partialMessage(w, fmt.Sprintf("Step %d in workflow %s is unavailable after refresh.", stepID, id))
		return
	}
	data.Fields = web.WorkflowFields(wf)
	data.Related = s.readWorkflowRelated(r.Context(), app, id)
	s.web.Partial(w, "workflow_inspection", data)
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
