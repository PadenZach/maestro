package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

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
	Status   string
	Name     string
	IDPrefix string
	Queue    string
}

type workflowsData struct {
	App      string
	Statuses []string
	Filter   filterState
	Rows     workflowRows
}

type workflowRows struct {
	App        string
	Workflows  []protocol.WorkflowsOutput
	RangeLabel string
	PrevURL    string
	NextURL    string
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
	App       string
	WF        *protocol.WorkflowsOutput
	Timeline  web.Timeline
	IsRunning bool
	Flash     string
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
	w.WriteHeader(status)
	s.web.Page(w, "error", page{
		Title:         "Error",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(true),
		Crumbs:        crumbs,
		Data:          errorData{Message: htmlErrorText(err)},
	})
}

type errorData struct{ Message string }

// partialError writes a small inline error fragment for HTMX swaps.
func partialError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<div class="flash err">%s</div>`, htmlEscape(htmlErrorText(err)))
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
	f := parseFilter(r)
	f.Name = strings.TrimSpace(f.Name)
	rows, err := s.fetchConsoleRows(r.Context(), app, f, 0)
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
			App:      app,
			Statuses: knownStatuses,
			Filter:   f,
			Rows:     rows,
		},
	})
}

func (s *Server) handleWorkflowRows(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	f := parseFilter(r)
	f.Name = strings.TrimSpace(f.Name)
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	rows, err := s.fetchConsoleRows(r.Context(), app, f, offset)
	if err != nil {
		partialError(w, err)
		return
	}
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
	if f.Status != "" {
		body.Status = []string{f.Status}
	}
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
	hasNext := len(wfs) > limit
	if hasNext {
		wfs = wfs[:limit]
	}
	rows := workflowRows{App: app, Workflows: wfs}
	if len(wfs) == 0 {
		rows.RangeLabel = "No results"
	} else {
		rows.RangeLabel = fmt.Sprintf("%d–%d", offset+1, offset+len(wfs))
	}
	if offset > 0 {
		prev := offset - limit
		if prev < 0 {
			prev = 0
		}
		rows.PrevURL = rowsURL(app, f, prev)
	}
	if hasNext {
		rows.NextURL = rowsURL(app, f, offset+limit)
	}
	return rows, nil
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
	wf, err := s.readWorkflow(ctx, app, id, true, true)
	if err != nil {
		return detailLive{}, err
	}
	if wf == nil {
		return detailLive{}, fmt.Errorf("workflow %q not found", id)
	}

	steps, err := s.readSteps(ctx, app, id, true, nil, nil)
	if err != nil {
		return detailLive{}, err
	}
	return detailLive{
		App:       app,
		WF:        wf,
		Timeline:  web.BuildTimeline(app, id, steps),
		IsRunning: isRunning(wf.Status),
		Flash:     flash,
	}, nil
}

func (s *Server) handleWorkflowTimeline(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	wf, err := s.readWorkflow(r.Context(), app, id, false, false)
	if err != nil {
		partialError(w, err)
		return
	}
	if wf == nil {
		partialError(w, fmt.Errorf("child workflow %q not found", id))
		return
	}
	ancestors := r.URL.Query()["ancestor"]
	for _, ancestor := range ancestors {
		if ancestor == id {
			partialError(w, fmt.Errorf("Workflow relationship cycle at %q", id))
			return
		}
	}
	steps, err := s.readSteps(r.Context(), app, id, true, nil, nil)
	if err != nil {
		partialError(w, err)
		return
	}
	tl := web.BuildTimeline(app, id, steps)
	tl.ChildStatus = deref(wf.Status)
	web.SetTimelineBranch(&tl, r.URL.Query().Get("branch"), ancestors)
	s.web.Partial(w, "timeline", tl)
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
	}
	if loadInput {
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

// --- JSON handlers ----------------------------------------------------------

func (s *Server) handleAPIApps(w http.ResponseWriter, r *http.Request) {
	byApp := map[string]int{}
	for _, e := range s.hub.Executors() {
		byApp[e.App]++
	}
	type appJSON struct {
		Name      string `json:"name"`
		Executors int    `json:"executors"`
	}
	out := make([]appJSON, 0, len(byApp))
	for name, n := range byApp {
		out = append(out, appJSON{Name: name, Executors: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleAPIWorkflows(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	f := parseFilter(r)
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	rows, err := s.fetchRows(r.Context(), app, f, offset)
	if err != nil {
		s.apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rows.Workflows)
}

func (s *Server) handleAPIWorkflow(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	wf, err := s.readWorkflow(r.Context(), app, id, true, true)
	if err != nil {
		s.apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, wf)
}

func (s *Server) handleAPISteps(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	steps, err := s.readSteps(r.Context(), app, id, true, nil, nil)
	if err != nil {
		s.apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, steps)
}

func (s *Server) handleAPIEvents(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	var resp protocol.GetWorkflowEventsResponse
	if err := s.dispatch(r.Context(), app, protocol.GetWorkflowEventsRequest(id), &resp); err != nil {
		s.apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp.Events)
}

func (s *Server) handleAPINotifications(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	var resp protocol.GetWorkflowNotificationsResponse
	if err := s.dispatch(r.Context(), app, protocol.GetWorkflowNotificationsRequest(id), &resp); err != nil {
		s.apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp.Notifications)
}

func (s *Server) handleAPIStreams(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	var resp protocol.GetWorkflowStreamsResponse
	if err := s.dispatch(r.Context(), app, protocol.GetWorkflowStreamsRequest(id), &resp); err != nil {
		s.apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp.Streams)
}

func (s *Server) handleAPIQueues(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	var resp protocol.ListQueuesResponse
	if err := s.dispatch(r.Context(), app, protocol.ListQueuesRequest(), &resp); err != nil {
		s.apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp.Output)
}

func (s *Server) handleAPIQueue(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	name := r.PathValue("name")
	var resp protocol.GetQueueResponse
	if err := s.dispatch(r.Context(), app, protocol.GetQueueRequest(name), &resp); err != nil {
		s.apiError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp.Output)
}

func (s *Server) apiError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, hub.ErrAppUnavailable) {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

// --- small helpers ----------------------------------------------------------

func parseFilter(r *http.Request) filterState {
	q := r.URL.Query()
	return filterState{
		Status:   q.Get("status"),
		Name:     q.Get("name"),
		IDPrefix: q.Get("id_prefix"),
		Queue:    q.Get("queue"),
	}
}

func rowsURL(app string, f filterState, offset int) string {
	v := url.Values{}
	if f.Status != "" {
		v.Set("status", f.Status)
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
	v.Set("offset", strconv.Itoa(offset))
	return "/apps/" + url.PathEscape(app) + "/workflows/rows?" + v.Encode()
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

func htmlEscape(s string) string {
	r := make([]byte, 0, len(s))
	for _, c := range []byte(s) {
		switch c {
		case '<':
			r = append(r, "&lt;"...)
		case '>':
			r = append(r, "&gt;"...)
		case '&':
			r = append(r, "&amp;"...)
		default:
			r = append(r, c)
		}
	}
	return string(r)
}

func workflowsCrumbs(app string) []crumb {
	return []crumb{{Label: "Home", Href: "/"}, {Label: app, Href: "/apps/" + app + "/workflows"}, {Label: "Workflows"}}
}

func detailCrumbs(app, id string) []crumb {
	return []crumb{
		{Label: "Home", Href: "/"},
		{Label: app, Href: "/apps/" + app + "/workflows"},
		{Label: "Workflows", Href: "/apps/" + app + "/workflows"},
		{Label: id},
	}
}

func queuesCrumbs(app string) []crumb {
	return []crumb{{Label: "Home", Href: "/"}, {Label: app, Href: "/apps/" + app + "/workflows"}, {Label: "Queues"}}
}
