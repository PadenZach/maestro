package api

import (
	"fmt"
	"net/http"

	"github.com/PadenZach/maestro/internal/protocol"
)

type Schedule struct {
	ScheduleID        string  `json:"scheduleId"`
	ScheduleName      string  `json:"scheduleName"`
	WorkflowName      string  `json:"workflowName"`
	WorkflowClass     *string `json:"workflowClass"`
	CronExpression    string  `json:"cronExpression"`
	Status            string  `json:"status"`
	Context           *string `json:"context"`
	LastFiredAt       *string `json:"lastFiredAt" format:"date-time"`
	AutomaticBackfill bool    `json:"automaticBackfill"`
	CronTimezone      *string `json:"cronTimezone"`
	ApplicationName   *string `json:"applicationName"`
}

type scheduleListQuery struct {
	Status             string `json:"status,omitempty"`
	WorkflowName       string `json:"workflowName,omitempty"`
	ScheduleNamePrefix string `json:"scheduleNamePrefix,omitempty"`
	LoadContext        bool   `json:"loadContext,omitempty"`
}

// scheduleResponse maps the released SDK wire record to the pinned official
// Schedule schema. queue_name is intentionally absent from that HTTP schema.
func scheduleResponse(schedule protocol.ScheduleOutput) (*Schedule, error) {
	if err := schedule.Validate(); err != nil {
		return nil, err
	}
	return &Schedule{
		ScheduleID: schedule.ScheduleID, ScheduleName: schedule.ScheduleName,
		WorkflowName: schedule.WorkflowName, WorkflowClass: schedule.WorkflowClassName,
		CronExpression: schedule.Schedule, Status: schedule.Status,
		Context: schedule.Context, LastFiredAt: schedule.LastFiredAt,
		AutomaticBackfill: schedule.AutomaticBackfill, CronTimezone: schedule.CronTimezone,
		ApplicationName: schedule.ApplicationName,
	}, nil
}

func scheduleQuery(r *http.Request) (protocol.ListSchedulesBody, error) {
	query, err := parseUTF8Query(r.URL.RawQuery)
	if err != nil {
		return protocol.ListSchedulesBody{}, fmt.Errorf("malformed query")
	}
	var body protocol.ListSchedulesBody
	for name, values := range query {
		if len(values) != 1 {
			return body, fmt.Errorf("duplicate query %q", name)
		}
		value := values[0]
		switch name {
		case "status":
			body.Status = []string{value}
		case "workflowName":
			body.WorkflowName = []string{value}
		case "scheduleNamePrefix":
			body.ScheduleNamePrefix = []string{value}
		case "loadContext":
			if value != "true" && value != "false" {
				return body, fmt.Errorf("loadContext must be boolean")
			}
			loadContext := value == "true"
			body.LoadContext = &loadContext
		default:
			return body, fmt.Errorf("unsupported query %q", name)
		}
	}
	return body, nil
}

func (s *handler) listSchedules(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrganization(w, r) {
		return
	}
	body, err := scheduleQuery(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	var response protocol.ListSchedulesResponse
	if err := s.hub.Call(r.Context(), r.PathValue("app"), protocol.ListSchedulesRequest(body), &response); err != nil {
		writeFailure(w, err)
		return
	}
	if response.Output == nil {
		writeProblem(w, http.StatusBadGateway, "schedule list unavailable")
		return
	}
	rows := make([]*Schedule, 0, len(response.Output))
	for _, schedule := range response.Output {
		row, err := scheduleResponse(schedule)
		if err != nil {
			writeFailure(w, err)
			return
		}
		rows = append(rows, row)
	}
	writeJSON(w, http.StatusOK, rows)
}

func (s *handler) getSchedule(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrganization(w, r) || !noQuery(w, r) {
		return
	}
	var response protocol.GetScheduleResponse
	if err := s.hub.Call(r.Context(), r.PathValue("app"), protocol.GetScheduleRequest(r.PathValue("name")), &response); err != nil {
		writeFailure(w, err)
		return
	}
	if response.Output == nil {
		writeProblem(w, http.StatusNotFound, "schedule not found")
		return
	}
	row, err := scheduleResponse(*response.Output)
	if err != nil {
		writeFailure(w, err)
		return
	}
	writeJSON(w, http.StatusOK, row)
}
