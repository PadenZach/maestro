package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/protocol"
	"github.com/zpaden/maestro/internal/web"
)

// defaultPageSize bounds the workflow list; the list view never loads
// input/output blobs (those lazy-load on the detail page).
const defaultPageSize = 25

// knownStatuses populates the workflow-list status filter.
var knownStatuses = []string{
	"ENQUEUED", "PENDING", "SUCCESS", "ERROR",
	"CANCELLED", "MAX_RECOVERY_ATTEMPTS_EXCEEDED", "DELAYED",
}

// --- view models (fields exported so html/template can read them) -----------

type crumb struct{ Label, Href string }

// page carries shared navigation and a connection-status snapshot for HTML views.
type page struct {
	Title         string
	AppsAvailable int
	Status        uiStatus
	Crumbs        []crumb
	Data          any
}

type appsData struct{ Apps []appSummary }

type appSummary struct {
	Name      string
	Available bool
	Executors []hub.ExecutorView
}

type filterState struct {
	// Status supports the single-status JSON API; Console uses Statuses.
	Status          string
	Statuses        []string
	Name            string
	IDPrefix        string
	Queue           string
	StartTime       string
	EndTime         string
	IncludeChildren bool
	// Nil preserves the JSON API's existing all-executions behavior.
	HasParent *bool
}

func (f filterState) statusValues() []string {
	if len(f.Statuses) > 0 {
		return f.Statuses
	}
	if f.Status != "" {
		return []string{f.Status}
	}
	return nil
}

func (f filterState) HasStatus(status string) bool {
	for _, value := range f.statusValues() {
		if value == status {
			return true
		}
	}
	return false
}

func (f filterState) Active() bool {
	return len(f.statusValues()) > 0 || f.Name != "" || f.IDPrefix != "" || f.Queue != "" || f.StartTime != "" || f.EndTime != ""
}

func (f filterState) StatusLabel() string {
	statuses := f.statusValues()
	if len(statuses) == 0 {
		return "All statuses"
	}
	if len(statuses) == 1 {
		return statuses[0]
	}
	return fmt.Sprintf("%d statuses", len(statuses))
}

type workflowsData struct {
	App               string
	Statuses          []string
	Filter            filterState
	Rows              workflowRows
	AggregatesEnabled bool
}

type workflowRows struct {
	App        string
	Workflows  []protocol.WorkflowsOutput
	RangeLabel string
	PrevURL    string
	NextURL    string
	RefreshURL string
	Filter     filterState
}

type detailData struct {
	Live               detailLive
	Events             []protocol.EventOutput
	Notifications      []protocol.NotificationOutput
	Streams            []protocol.StreamEntryOutput
	EventsError        string
	NotificationsError string
	StreamsError       string
}

type detailLive struct {
	App           string
	WF            *protocol.WorkflowsOutput
	Timeline      web.Timeline
	IsRunning     bool
	Flash         string
	TimelineError string
}

type queuesData struct {
	App    string
	Queues []protocol.QueueOutput
}

type blobData struct {
	Title, Content string
	Available      bool
	Missing        bool
}

// --- dispatcher helper ------------------------------------------------------

// responder is satisfied by every typed response via the embedded BaseResponse,
// letting dispatch surface executor-side error_message uniformly.
type responder interface{ Err() error }

// dispatch sends req to a healthy executor of app, decodes the reply into out,
// and returns any transport or executor-side error.
func (s *Server) dispatch(ctx context.Context, app string, req protocol.Request, out responder) error {
	raw, err := s.hub.Request(ctx, app, req)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if err := out.Err(); err != nil {
		return err
	}
	// A 3.1 metadata-only refusal normally carries error_message, but a
	// BaseResponse-only reply must not become an empty successful read panel.
	key := ""
	switch out.(type) {
	case *protocol.ListWorkflowsResponse, *protocol.GetWorkflowResponse,
		*protocol.ListStepsResponse, *protocol.ListQueuesResponse, *protocol.GetQueueResponse,
		*protocol.ListSchedulesResponse, *protocol.GetScheduleResponse:
		key = "output"
	case *protocol.GetWorkflowEventsResponse:
		key = "events"
	case *protocol.GetWorkflowNotificationsResponse:
		key = "notifications"
	case *protocol.GetWorkflowStreamsResponse:
		key = "streams"
	}
	if key != "" {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
		if _, ok := fields[key]; !ok {
			return errors.New("executor response data unavailable")
		}
	}
	return nil
}

// appsAvailable preserves the distinct ready-app count used by page models.
func (s *Server) appsAvailable() int {
	seen := map[string]struct{}{}
	for _, e := range s.hub.Executors() {
		seen[e.App] = struct{}{}
	}
	return len(seen)
}

// renderErrorPage shows a full-page error (e.g. application unavailable).
func (s *Server) renderErrorPage(w http.ResponseWriter, crumbs []crumb, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, hub.ErrAppUnavailable) {
		status = http.StatusServiceUnavailable
	}
	s.renderStatusError(w, status, crumbs, err)
}

func (s *Server) renderStatusError(w http.ResponseWriter, status int, crumbs []crumb, err error) {
	w.WriteHeader(status)
	s.web.Page(w, "error", page{
		Title:         "Error",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(status >= http.StatusInternalServerError),
		Crumbs:        crumbs,
		Data:          errorData{Message: htmlErrorText(err)},
	})
}

type errorData struct{ Message string }

// partialError writes a small inline error fragment for HTMX swaps.
func partialError(w http.ResponseWriter, err error) {
	partialMessage(w, htmlErrorText(err))
}

func partialMessage(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<div class="flash err">%s</div>`, html.EscapeString(message))
}

// --- HTML handlers ----------------------------------------------------------

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	byApp := map[string][]hub.ExecutorView{}
	for _, e := range s.hub.Executors() {
		byApp[e.App] = append(byApp[e.App], e)
	}
	names := make([]string, 0, len(byApp))
	for name := range byApp {
		names = append(names, name)
	}
	sort.Strings(names)
	apps := make([]appSummary, 0, len(names))
	for _, name := range names {
		apps = append(apps, appSummary{Name: name, Available: true, Executors: byApp[name]})
	}
	s.web.Page(w, "apps", page{
		Title:         "Applications",
		AppsAvailable: len(names),
		Status:        s.statusForPage(false),
		Crumbs:        []crumb{{Label: "Home"}},
		Data:          appsData{Apps: apps},
	})
}

func (s *Server) handleWorkflows(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	f, err := parseFilter(r)
	if err != nil {
		s.renderStatusError(w, http.StatusBadRequest, workflowsCrumbs(app), err)
		return
	}
	f.Name = strings.TrimSpace(f.Name)
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	rows, err := s.fetchConsoleRows(r.Context(), app, f, offset)
	if err != nil {
		s.renderErrorPage(w, workflowsCrumbs(app), err)
		return
	}
	s.web.Page(w, "workflows", page{
		Title:         app + " · Workflows",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(false),
		Crumbs:        workflowsCrumbs(app),
		Data: workflowsData{
			App:               app,
			Statuses:          workflowFilterStatuses(f),
			Filter:            f,
			Rows:              rows,
			AggregatesEnabled: s.cfg.EnableAggregates,
		},
	})
}

func (s *Server) handleWorkflowRows(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	f, err := parseFilter(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		partialError(w, err)
		return
	}
	f.Name = strings.TrimSpace(f.Name)
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	rows, err := s.fetchConsoleRows(r.Context(), app, f, offset)
	if err != nil {
		partialError(w, err)
		return
	}
	// History entries must be reloadable full pages, including the current offset.
	w.Header().Set("HX-Push-Url", workflowsURL(app, f, offset))
	s.web.Partial(w, "workflow_rows", rows)
}

// fetchRows runs LIST_WORKFLOWS for the given filter/offset and builds the table
// view model, requesting one extra row to detect a next page.
func (s *Server) fetchRows(ctx context.Context, app string, f filterState, offset int) (workflowRows, error) {
	limit := defaultPageSize
	over := limit + 1
	body := protocol.ListWorkflowsBody{
		SortDesc: true,
		Limit:    &over,
		Offset:   &offset,
	}
	body.Status = f.statusValues()
	body.StartTime, body.EndTime = f.StartTime, f.EndTime
	body.HasParent = f.HasParent
	if f.Name != "" {
		body.WorkflowName = []string{f.Name}
	}
	if f.IDPrefix != "" {
		body.WorkflowIDPrefix = []string{f.IDPrefix}
	}
	if f.Queue != "" {
		body.QueueName = []string{f.Queue}
	}

	wfs, err := s.readWorkflows(ctx, app, body)
	if err != nil {
		return workflowRows{}, err
	}
	return paginateRows(app, f, offset, wfs), nil
}

// paginateRows shares range labels and navigation between exact SDK queries
// and Console substring search. Callers supply at most one extra result.
func paginateRows(app string, f filterState, offset int, workflows []protocol.WorkflowsOutput) workflowRows {
	hasNext := len(workflows) > defaultPageSize
	if hasNext {
		workflows = workflows[:defaultPageSize]
	}
	rows := workflowRows{App: app, Workflows: workflows, RangeLabel: "No results", RefreshURL: rowsURL(app, f, offset), Filter: f}
	if len(workflows) > 0 {
		rows.RangeLabel = fmt.Sprintf("%d–%d", offset+1, offset+len(workflows))
	}
	if offset > 0 {
		rows.PrevURL = rowsURL(app, f, max(0, offset-defaultPageSize))
	}
	if hasNext {
		rows.NextURL = rowsURL(app, f, offset+defaultPageSize)
	}
	return rows
}

func (s *Server) handleWorkflowDetail(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")

	live, err := s.buildDetailLive(r.Context(), app, id, "")
	if err != nil {
		s.renderErrorPage(w, detailCrumbs(app, id), err)
		return
	}

	data := s.readWorkflowRelated(r.Context(), app, id)
	data.Live = live

	s.web.Page(w, "workflow_detail", page{
		Title:         id + " · Workflow",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(data.EventsError != "" || data.NotificationsError != "" || data.StreamsError != ""),
		Crumbs:        detailCrumbs(app, id),
		Data:          data,
	})
}

func (s *Server) handleWorkflowLive(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	live, err := s.buildDetailLive(r.Context(), app, id, "")
	if err != nil {
		partialError(w, err)
		return
	}
	s.web.Partial(w, "detail_live", live)
}

// buildDetailLive fetches the workflow header + steps and assembles the pollable
// live region (status, actions, gantt timeline).
func (s *Server) buildDetailLive(ctx context.Context, app, id, flash string) (detailLive, error) {
	ctx, cancel := context.WithTimeout(ctx, flowReadTimeout)
	defer cancel()
	wf, err := s.readWorkflow(ctx, app, id, false, false)
	if err != nil {
		return detailLive{}, err
	}
	if wf == nil {
		return detailLive{}, fmt.Errorf("workflow %q not found", id)
	}

	steps, more, err := s.readTimelinePage(ctx, app, id, 0)
	tl := web.BuildTimeline(app, id, steps)
	setTimelinePage(&tl, 0, more, "", nil)
	timelineError := ""
	if err != nil {
		timelineError = htmlErrorText(err)
	}
	return detailLive{
		App:           app,
		WF:            wf,
		Timeline:      tl,
		TimelineError: timelineError,
		IsRunning:     isRunning(wf.Status),
		Flash:         flash,
	}, nil
}

func (s *Server) handleWorkflowTimeline(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	query := r.URL.Query()
	offset, offsetErr := flowOffset(query)
	if offsetErr != nil {
		partialError(w, offsetErr)
		return
	}
	if len(query["ancestor"]) > 8 {
		partialMessage(w, "Timeline depth limit reached. Open this workflow to continue.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), flowReadTimeout)
	defer cancel()
	starts, hasStart := query["window_start"]
	ends, hasEnd := query["window_end"]
	var start, end int64
	if hasStart || hasEnd {
		if len(starts) != 1 || len(ends) != 1 {
			partialError(w, fmt.Errorf("invalid timeline window"))
			return
		}
		var startErr, endErr error
		start, startErr = strconv.ParseInt(starts[0], 10, 64)
		end, endErr = strconv.ParseInt(ends[0], 10, 64)
		if startErr != nil || endErr != nil || start < 0 || end < start {
			partialError(w, fmt.Errorf("invalid timeline window"))
			return
		}
	}
	wf, err := s.readWorkflow(ctx, app, id, false, false)
	if err != nil {
		partialError(w, err)
		return
	}
	if wf == nil {
		partialError(w, fmt.Errorf("child workflow %q not found", id))
		return
	}
	ancestors := query["ancestor"]
	for _, ancestor := range ancestors {
		if ancestor == id {
			partialMessage(w, fmt.Sprintf("Workflow relationship cycle at %q", id))
			return
		}
	}
	steps, more, err := s.readTimelinePage(ctx, app, id, offset)
	if err != nil {
		partialError(w, err)
		return
	}
	var tl web.Timeline
	if hasStart {
		tl = web.BuildTimelineInWindow(app, id, steps, start, end)
	} else {
		tl = web.BuildTimeline(app, id, steps)
	}
	tl.ChildStatus = deref(wf.Status)
	web.SetTimelineBranch(&tl, query.Get("branch"), ancestors)
	setTimelinePage(&tl, offset, more, query.Get("branch"), ancestors)
	s.web.Partial(w, "timeline_rows", tl)
}

func (s *Server) handleWorkflowBlob(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	kind := r.URL.Query().Get("kind")

	loadInput := kind == "input"
	loadOutput := kind == "output"
	wf, err := s.readWorkflow(r.Context(), app, id, loadInput, loadOutput)
	if err != nil {
		partialError(w, err)
		return
	}

	if wf == nil {
		partialError(w, fmt.Errorf("workflow %q not found", id))
		return
	}
	data := blobData{Title: "Output"}
	if loadInput {
		data.Title = "Input"
		data.Content = deref(wf.Input)
		data.Available = wf.Input != nil
		data.Missing = !wf.FieldPresent("Input")
	} else {
		data.Content = deref(wf.Output)
		data.Available = wf.Output != nil
		data.Missing = !wf.FieldPresent("Output")
	}
	s.web.Partial(w, "blob", data)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	s.manage(w, r, protocol.CancelRequest(r.PathValue("id"), false), "Cancel failed: ")
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	s.manage(w, r, protocol.ResumeRequest(r.PathValue("id"), nil), "Resume failed: ")
}

// manage runs a mutating command then re-renders the live region, surfacing any
// failure as an inline flash rather than tearing down the page.
func (s *Server) manage(w http.ResponseWriter, r *http.Request, req protocol.Request, failPrefix string) {
	app := r.PathValue("app")
	id := r.PathValue("id")

	flash := ""
	var resp protocol.SuccessResponse
	if err := s.dispatch(r.Context(), app, req, &resp); err != nil {
		flash = failPrefix + htmlErrorText(err)
	}
	live, err := s.buildDetailLive(r.Context(), app, id, flash)
	if err != nil {
		partialError(w, err)
		return
	}
	s.web.Partial(w, "detail_live", live)
}

func (s *Server) handleQueues(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	var resp protocol.ListQueuesResponse
	if err := s.dispatch(r.Context(), app, protocol.ListQueuesRequest(), &resp); err != nil {
		s.renderErrorPage(w, queuesCrumbs(app), err)
		return
	}
	s.web.Page(w, "queues", page{
		Title:         app + " · Queues",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(false),
		Crumbs:        queuesCrumbs(app),
		Data:          queuesData{App: app, Queues: resp.Output},
	})
}

// --- small helpers ----------------------------------------------------------

func parseFilter(r *http.Request) (filterState, error) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return filterState{}, fmt.Errorf("invalid workflow filters: %w", err)
	}
	for name, values := range q {
		switch name {
		case "status":
		case "name", "id_prefix", "queue", "start_time", "end_time", "offset", "children":
			if len(values) != 1 {
				return filterState{}, fmt.Errorf("duplicate workflow filter %q", name)
			}
		default:
			return filterState{}, fmt.Errorf("unsupported workflow filter %q", name)
		}
	}
	f := filterState{
		Name:      q.Get("name"),
		IDPrefix:  q.Get("id_prefix"),
		Queue:     q.Get("queue"),
		StartTime: q.Get("start_time"),
		EndTime:   q.Get("end_time"),
	}
	if values, exists := q["children"]; exists {
		if values[0] != "true" && values[0] != "false" {
			return filterState{}, errors.New("children must be true or false")
		}
		f.IncludeChildren = values[0] == "true"
	}
	if !f.IncludeChildren {
		no := false
		f.HasParent = &no
	}
	for _, status := range q["status"] {
		if status != "" {
			f.Statuses = append(f.Statuses, status)
		}
	}
	var start, end time.Time
	for name, raw := range map[string]string{"start_time": f.StartTime, "end_time": f.EndTime} {
		if raw == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return filterState{}, fmt.Errorf("%s must be an RFC3339 timestamp in UTC", name)
		}
		_, zone := parsed.Zone()
		if zone != 0 || parsed.Nanosecond()%int(time.Millisecond) != 0 {
			return filterState{}, fmt.Errorf("%s must use UTC with at most millisecond precision", name)
		}
		if name == "start_time" {
			start = parsed
		} else {
			end = parsed
		}
	}
	if !start.IsZero() && !end.IsZero() && start.After(end) {
		return filterState{}, errors.New("start_time must be at or before end_time")
	}
	if values, exists := q["offset"]; exists && values[0] != "" {
		offset, err := strconv.Atoi(values[0])
		if err != nil || offset < 0 {
			return filterState{}, errors.New("offset must be a nonnegative integer")
		}
	}
	return f, nil
}

func workflowFilterStatuses(f filterState) []string {
	statuses := append([]string(nil), knownStatuses...)
	for _, status := range f.statusValues() {
		if !slices.Contains(statuses, status) {
			statuses = append(statuses, status)
		}
	}
	return statuses
}

func workflowFilterQuery(f filterState, offset int) string {
	v := url.Values{}
	for _, status := range f.statusValues() {
		v.Add("status", status)
	}
	if f.Name != "" {
		v.Set("name", f.Name)
	}
	if f.IDPrefix != "" {
		v.Set("id_prefix", f.IDPrefix)
	}
	if f.Queue != "" {
		v.Set("queue", f.Queue)
	}
	if f.StartTime != "" {
		v.Set("start_time", f.StartTime)
	}
	if f.EndTime != "" {
		v.Set("end_time", f.EndTime)
	}
	if f.HasParent != nil || f.IncludeChildren {
		v.Set("children", strconv.FormatBool(f.IncludeChildren))
	}
	v.Set("offset", strconv.Itoa(offset))
	return v.Encode()
}

func rowsURL(app string, f filterState, offset int) string {
	return applicationPath(app) + "/workflows/rows?" + workflowFilterQuery(f, offset)
}

func workflowsURL(app string, f filterState, offset int) string {
	return applicationPath(app) + "/workflows?" + workflowFilterQuery(f, offset)
}

func isRunning(status *string) bool {
	if status == nil {
		return false
	}
	switch *status {
	case "PENDING", "ENQUEUED", "DELAYED":
		return true
	}
	return false
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func workflowsCrumbs(app string) []crumb {
	return []crumb{{Label: "Home", Href: "/"}, {Label: app, Href: applicationPath(app)}, {Label: "Workflows"}}
}

func detailCrumbs(app, id string) []crumb {
	return []crumb{
		{Label: "Home", Href: "/"},
		{Label: app, Href: applicationPath(app)},
		{Label: "Workflows", Href: applicationPath(app) + "/workflows"},
		{Label: id},
	}
}

func queuesCrumbs(app string) []crumb {
	return []crumb{{Label: "Home", Href: "/"}, {Label: app, Href: applicationPath(app)}, {Label: "Queues"}}
}
