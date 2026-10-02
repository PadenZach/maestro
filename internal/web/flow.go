package web

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/zpaden/maestro/internal/protocol"
)

// Flow contains only recorded relationships and evidence, never opaque payloads.
type Flow struct {
	ID             string     `json:"id"`
	Name           *string    `json:"name"`
	Status         *string    `json:"status"`
	ParentID       *string    `json:"parentId"`
	WorkflowURL    string     `json:"workflowUrl"`
	InspectURL     string     `json:"inspectUrl"`
	WorkflowLoaded bool       `json:"workflowLoaded"`
	DetailsLoaded  bool       `json:"detailsLoaded"`
	State          string     `json:"state"`
	Error          string     `json:"error"`
	ReadAt         string     `json:"readAt"`
	Steps          []FlowStep `json:"steps"`
	Offset         int        `json:"offset"`
	NextOffset     *int       `json:"nextOffset"`
	HasMore        bool       `json:"hasMore"`
	Limited        bool       `json:"limited"`
}

type FlowStep struct {
	ID              *int    `json:"id"`
	Name            string  `json:"name"`
	HasError        bool    `json:"hasError"`
	ErrorKnown      bool    `json:"errorKnown"`
	ChildWorkflowID *string `json:"childWorkflowId"`
	Relationship    string  `json:"relationship"`
	InspectURL      string  `json:"inspectUrl"`
	StartedAt       *string `json:"startedAt"`
	CompletedAt     *string `json:"completedAt"`
}

func NewFlow(app, id string, offset int) Flow {
	u := WorkflowURL(app, id)
	return Flow{ID: id, WorkflowURL: u, InspectURL: u + "/inspect", Steps: []FlowStep{}, Offset: offset}
}

func (f *Flow) SetWorkflow(w protocol.WorkflowsOutput) {
	f.WorkflowLoaded = true
	f.Name, f.Status, f.ParentID = w.WorkflowName, w.Status, w.ParentWorkflowID
}

func RecordedFlowStep(app, workflowID string, offset int, step protocol.WorkflowSteps) FlowStep {
	// The SDK stringifies recorded exceptions; ValueError() is an empty
	// string on the wire and still proves that this operation failed.
	hasError := step.Error != nil
	s := FlowStep{
		Name: step.FunctionName, HasError: hasError,
		// Null output and error may be a metadata-only SDK response, including
		// a successful launch record. Keep its outcome unknown.
		ErrorKnown: hasError || step.Output != nil, ChildWorkflowID: step.ChildWorkflowID,
		StartedAt: step.StartedAtEpochMS, CompletedAt: step.CompletedAtEpochMS,
	}
	if step.HasFunctionID() {
		id := step.FunctionID
		s.ID = &id
		q := url.Values{"step": {strconv.Itoa(id)}, "offset": {strconv.Itoa(offset)}}
		s.InspectURL = WorkflowURL(app, workflowID) + "/inspect?" + q.Encode()
	}
	if step.ChildWorkflowID != nil && *step.ChildWorkflowID != "" {
		s.Relationship = "reference"
		// Reviewed Python 2.31/3.x launch records use the workflow's registered
		// name; getResult records a returned result or error. Other SDK-internal
		// operations and unnamed records retain a conservative reference.
		if step.FunctionName == "DBOS.getResult" {
			s.Relationship = "return"
		} else if step.FunctionName != "" && !strings.HasPrefix(step.FunctionName, "DBOS.") {
			s.Relationship = "invocation"
		}
	}
	return s
}
