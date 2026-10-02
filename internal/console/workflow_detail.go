package console

import (
	"context"
	"fmt"
	"net/http"

	"github.com/PadenZach/maestro/internal/protocol"
)

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
	Timeline      Timeline
	IsRunning     bool
	Flash         string
	TimelineError string
}

type blobData struct {
	Title, Content string
	Available      bool
	Missing        bool
}

func (s *handler) handleWorkflowDetail(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")

	live, err := s.buildDetailLive(r.Context(), app, id, "")
	if err != nil {
		s.renderErrorPage(w, detailCrumbs(app, id), err)
		return
	}

	data := s.readWorkflowRelated(r.Context(), app, id)
	data.Live = live

	s.web.page(w, "workflow_detail", page{
		Title:         id + " · Workflow",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(data.EventsError != "" || data.NotificationsError != "" || data.StreamsError != ""),
		Crumbs:        detailCrumbs(app, id),
		Data:          data,
	})
}

func (s *handler) handleWorkflowLive(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	live, err := s.buildDetailLive(r.Context(), app, id, "")
	if err != nil {
		partialError(w, err)
		return
	}
	s.web.partial(w, "detail_live", live)
}

// buildDetailLive fetches the workflow header + steps and assembles the pollable
// live region (status, actions, gantt timeline).
func (s *handler) buildDetailLive(ctx context.Context, app, id, flash string) (detailLive, error) {
	ctx, cancel := context.WithTimeout(ctx, flowReadTimeout)
	defer cancel()
	wf, err := s.hub.Workflow(ctx, app, id, false, false)
	if err != nil {
		return detailLive{}, err
	}
	if wf == nil {
		return detailLive{}, fmt.Errorf("workflow %q not found", id)
	}

	steps, more, err := s.readTimelinePage(ctx, app, id, 0)
	tl := BuildTimeline(app, id, steps)
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

func (s *handler) handleWorkflowBlob(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	id := r.PathValue("id")
	kind := r.URL.Query().Get("kind")

	loadInput := kind == "input"
	loadOutput := kind == "output"
	wf, err := s.hub.Workflow(r.Context(), app, id, loadInput, loadOutput)
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
	s.web.partial(w, "blob", data)
}

func (s *handler) handleCancel(w http.ResponseWriter, r *http.Request) {
	s.manage(w, r, protocol.CancelRequest(r.PathValue("id"), false), "Cancel failed: ")
}

func (s *handler) handleResume(w http.ResponseWriter, r *http.Request) {
	s.manage(w, r, protocol.ResumeRequest(r.PathValue("id"), nil), "Resume failed: ")
}

// manage runs a mutating command then re-renders the live region, surfacing any
// failure as an inline flash rather than tearing down the page.
func (s *handler) manage(w http.ResponseWriter, r *http.Request, req protocol.Request, failPrefix string) {
	app := r.PathValue("app")
	id := r.PathValue("id")

	flash := ""
	var resp protocol.SuccessResponse
	if err := s.hub.Call(r.Context(), app, req, &resp); err != nil {
		flash = failPrefix + htmlErrorText(err)
	}
	live, err := s.buildDetailLive(r.Context(), app, id, flash)
	if err != nil {
		partialError(w, err)
		return
	}
	s.web.partial(w, "detail_live", live)
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

func detailCrumbs(app, id string) []crumb {
	return []crumb{
		{Label: "Home", Href: "/"},
		{Label: app, Href: applicationPath(app)},
		{Label: "Workflows", Href: applicationPath(app) + "/workflows"},
		{Label: id},
	}
}
