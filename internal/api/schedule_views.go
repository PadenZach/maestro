package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/zpaden/maestro/internal/protocol"
)

type scheduleFilterState struct {
	Status             string
	WorkflowName       string
	ScheduleNamePrefix string
}

type schedulesData struct {
	App       string
	Filter    scheduleFilterState
	Schedules []protocol.ScheduleOutput
}

type scheduleDetailData struct {
	App      string
	Schedule *protocol.ScheduleOutput
}

func (d schedulesData) SchedulesURL() string {
	return applicationPath(d.App) + "/schedules"
}

// ScheduleDetailURL keeps exact schedule names in the query so browser path
// normalization cannot change dot, slash, or other path-significant names.
func (d schedulesData) ScheduleDetailURL(name string) string {
	query := url.Values{"name": []string{name}}
	return applicationPath(d.App) + "/schedule?" + query.Encode()
}

func parseScheduleListQuery(r *http.Request) (scheduleFilterState, protocol.ListSchedulesBody, error) {
	query, err := parseUTF8Query(r.URL.RawQuery)
	if err != nil {
		return scheduleFilterState{}, protocol.ListSchedulesBody{}, errors.New("malformed query")
	}
	var filter scheduleFilterState
	loadContext := false
	body := protocol.ListSchedulesBody{LoadContext: &loadContext}
	for name, values := range query {
		if len(values) != 1 {
			return filter, body, fmt.Errorf("duplicate query %q", name)
		}
		value := values[0]
		switch name {
		case "status":
			filter.Status = value
			if value != "" {
				body.Status = []string{value}
			}
		case "workflowName":
			filter.WorkflowName = value
			if value != "" {
				body.WorkflowName = []string{value}
			}
		case "scheduleNamePrefix":
			filter.ScheduleNamePrefix = value
			if value != "" {
				body.ScheduleNamePrefix = []string{value}
			}
		default:
			return filter, body, fmt.Errorf("unsupported query %q", name)
		}
	}
	return filter, body, nil
}

func parseScheduleNameQuery(r *http.Request) (string, error) {
	query, err := parseUTF8Query(r.URL.RawQuery)
	if err != nil {
		return "", errors.New("schedule detail requires exactly one nonempty name query parameter")
	}
	names, hasName := query["name"]
	if len(query) != 1 || !hasName || len(names) != 1 || names[0] == "" {
		return "", errors.New("schedule detail requires exactly one nonempty name query parameter")
	}
	return names[0], nil
}

func validateScheduleForConsole(schedule protocol.ScheduleOutput) error {
	_, err := localV2Schedule(schedule)
	return err
}

func (s *Server) handleSchedules(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	filter, body, err := parseScheduleListQuery(r)
	if err != nil {
		s.renderStatusError(w, http.StatusBadRequest, schedulesCrumbs(app), fmt.Errorf("invalid schedule list query: %w", err))
		return
	}

	var response protocol.ListSchedulesResponse
	if err := s.dispatch(r.Context(), app, protocol.ListSchedulesRequest(body), &response); err != nil {
		s.renderErrorPage(w, schedulesCrumbs(app), err)
		return
	}
	if response.Output == nil {
		s.renderStatusError(w, http.StatusBadGateway, schedulesCrumbs(app), errors.New("schedule list unavailable"))
		return
	}
	for _, schedule := range response.Output {
		if err := validateScheduleForConsole(schedule); err != nil {
			s.renderStatusError(w, http.StatusBadGateway, schedulesCrumbs(app), err)
			return
		}
	}

	s.web.Page(w, "schedules", page{
		Title:         app + " · Schedules",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(false),
		Crumbs:        schedulesCrumbs(app),
		Data: schedulesData{
			App:       app,
			Filter:    filter,
			Schedules: response.Output,
		},
	})
}

func (s *Server) handleScheduleDetail(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	name, err := parseScheduleNameQuery(r)
	if err != nil {
		s.renderStatusError(w, http.StatusBadRequest, schedulesCrumbs(app), err)
		return
	}
	crumbs := scheduleDetailCrumbs(app, name)

	var response protocol.GetScheduleResponse
	if err := s.dispatch(r.Context(), app, protocol.GetScheduleRequest(name), &response); err != nil {
		s.renderErrorPage(w, crumbs, err)
		return
	}
	if response.Output == nil {
		s.renderStatusError(w, http.StatusNotFound, crumbs, fmt.Errorf("schedule %q not found", name))
		return
	}
	if err := validateScheduleForConsole(*response.Output); err != nil {
		s.renderStatusError(w, http.StatusBadGateway, crumbs, err)
		return
	}

	s.web.Page(w, "schedule_detail", page{
		Title:         name + " · Schedule",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(false),
		Crumbs:        crumbs,
		Data:          scheduleDetailData{App: app, Schedule: response.Output},
	})
}

func schedulesCrumbs(app string) []crumb {
	appPath := applicationPath(app)
	return []crumb{
		{Label: "Home", Href: "/"},
		{Label: app, Href: appPath},
		{Label: "Schedules"},
	}
}

func scheduleDetailCrumbs(app, name string) []crumb {
	appPath := applicationPath(app)
	return []crumb{
		{Label: "Home", Href: "/"},
		{Label: app, Href: appPath},
		{Label: "Schedules", Href: appPath + "/schedules"},
		{Label: name},
	}
}
