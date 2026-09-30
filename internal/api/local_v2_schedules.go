package api

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
	"unicode/utf8"

	"github.com/zpaden/maestro/internal/protocol"
)

// localV2Schedule maps the released SDK wire record to the pinned official
// Schedule schema. queue_name is intentionally absent from that HTTP schema.
func localV2Schedule(schedule protocol.ScheduleOutput) (map[string]any, error) {
	if !schedule.HasRequiredFields() {
		return nil, fmt.Errorf("schedule missing required SDK fields")
	}
	if schedule.LastFiredAt != nil {
		if _, err := time.Parse(time.RFC3339Nano, *schedule.LastFiredAt); err != nil {
			return nil, fmt.Errorf("schedule last_fired_at must be RFC3339: %w", err)
		}
	}
	return map[string]any{
		"scheduleId": schedule.ScheduleID, "scheduleName": schedule.ScheduleName,
		"workflowName": schedule.WorkflowName, "workflowClass": schedule.WorkflowClassName,
		"cronExpression": schedule.Schedule, "status": schedule.Status,
		"context": schedule.Context, "lastFiredAt": schedule.LastFiredAt,
		"automaticBackfill": schedule.AutomaticBackfill, "cronTimezone": schedule.CronTimezone,
		"applicationName": schedule.ApplicationName,
	}, nil
}

func localV2ScheduleQuery(r *http.Request) (protocol.ListSchedulesBody, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return protocol.ListSchedulesBody{}, fmt.Errorf("malformed query")
	}
	for name, values := range query {
		if !utf8.ValidString(name) {
			return protocol.ListSchedulesBody{}, fmt.Errorf("malformed query")
		}
		for _, value := range values {
			if !utf8.ValidString(value) {
				return protocol.ListSchedulesBody{}, fmt.Errorf("malformed query")
			}
		}
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

func (s *Server) localV2Schedules(w http.ResponseWriter, r *http.Request) {
	if !localV2Allowed(w, r) {
		return
	}
	body, err := localV2ScheduleQuery(r)
	if err != nil {
		localV2Problem(w, http.StatusBadRequest, err.Error())
		return
	}
	var response protocol.ListSchedulesResponse
	if err := s.dispatch(r.Context(), r.PathValue("app"), protocol.ListSchedulesRequest(body), &response); err != nil {
		localV2Failure(w, err)
		return
	}
	if response.Output == nil {
		localV2Problem(w, http.StatusBadGateway, "schedule list unavailable")
		return
	}
	rows := make([]map[string]any, 0, len(response.Output))
	for _, schedule := range response.Output {
		row, err := localV2Schedule(schedule)
		if err != nil {
			localV2Failure(w, err)
			return
		}
		rows = append(rows, row)
	}
	writeJSON(w, http.StatusOK, rows)
}

func (s *Server) localV2GetSchedule(w http.ResponseWriter, r *http.Request) {
	if !localV2Allowed(w, r) || !localV2NoQuery(w, r) {
		return
	}
	var response protocol.GetScheduleResponse
	if err := s.dispatch(r.Context(), r.PathValue("app"), protocol.GetScheduleRequest(r.PathValue("name")), &response); err != nil {
		localV2Failure(w, err)
		return
	}
	if response.Output == nil {
		localV2Problem(w, http.StatusNotFound, "schedule not found")
		return
	}
	row, err := localV2Schedule(*response.Output)
	if err != nil {
		localV2Failure(w, err)
		return
	}
	writeJSON(w, http.StatusOK, row)
}
