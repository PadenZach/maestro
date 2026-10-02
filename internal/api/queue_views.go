package api

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/zpaden/maestro/internal/protocol"
)

type queueDetailData struct {
	App   string
	Queue *protocol.QueueOutput
}

// QueueDetailURL returns a path-segment-safe link for a queue list row.
func (d queuesData) QueueDetailURL(name string) string {
	return queueDetailURL(d.App, name)
}

// QueuedWorkflowsURL keeps the queue-to-workflow navigation while encoding the
// queue name as a query value rather than interpolating it into markup.
func (d queuesData) QueuedWorkflowsURL(name string) string {
	return queuedWorkflowsURL(d.App, name)
}

func (d queueDetailData) QueuedWorkflowsURL() string {
	return queuedWorkflowsURL(d.App, d.Queue.Name)
}

func queueDetailURL(app, name string) string {
	appPath := "/apps/" + url.PathEscape(app)
	if name == "." || name == ".." {
		query := url.Values{"name": []string{name}}
		return appPath + "/queue?" + query.Encode()
	}
	return appPath + "/queues/" + url.PathEscape(name)
}

func queuedWorkflowsURL(app, name string) string {
	query := url.Values{"queue": []string{name}, "children": []string{"true"}}
	return "/apps/" + url.PathEscape(app) + "/workflows?" + query.Encode()
}

func (s *Server) handleQueueDetail(w http.ResponseWriter, r *http.Request) {
	s.renderQueueDetail(w, r, r.PathValue("app"), r.PathValue("name"))
}

func (s *Server) handleQueueDetailAlias(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	query, err := url.ParseQuery(r.URL.RawQuery)
	names, hasName := query["name"]
	if err != nil || len(query) != 1 || !hasName || len(names) != 1 || names[0] == "" {
		s.renderStatusError(w, http.StatusBadRequest, queuesCrumbs(app), fmt.Errorf("queue detail requires exactly one nonempty name query parameter"))
		return
	}
	s.renderQueueDetail(w, r, app, names[0])
}

func (s *Server) renderQueueDetail(w http.ResponseWriter, r *http.Request, app, name string) {
	crumbs := queueDetailCrumbs(app, name)

	var resp protocol.GetQueueResponse
	if err := s.dispatch(r.Context(), app, protocol.GetQueueRequest(name), &resp); err != nil {
		s.renderErrorPage(w, crumbs, err)
		return
	}
	if resp.Output == nil {
		s.renderStatusError(w, http.StatusNotFound, crumbs, fmt.Errorf("queue %q not found", name))
		return
	}
	if !resp.Output.HasRequiredFields() {
		s.renderStatusError(w, http.StatusBadGateway, crumbs, fmt.Errorf("queue %q response missing required fields", name))
		return
	}

	s.web.Page(w, "queue_detail", page{
		Title:         name + " · Queue",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(false),
		Crumbs:        crumbs,
		Data:          queueDetailData{App: app, Queue: resp.Output},
	})
}

func queueDetailCrumbs(app, name string) []crumb {
	appPath := "/apps/" + url.PathEscape(app)
	return []crumb{
		{Label: "Home", Href: "/"},
		{Label: app, Href: appPath},
		{Label: "Queues", Href: appPath + "/queues"},
		{Label: name},
	}
}
