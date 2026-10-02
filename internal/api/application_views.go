package api

import (
	"net/http"
	"time"

	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/web"
)

type applicationData struct {
	App               string
	Available         bool
	Executors         []hub.ExecutorView
	Window            overviewWindow
	AggregatesEnabled bool
}

func (d applicationData) WorkflowsURL() string {
	return applicationPath(d.App) + "/workflows"
}

func (d applicationData) QueuesURL() string {
	return applicationPath(d.App) + "/queues"
}

func (d applicationData) SchedulesURL() string {
	return applicationPath(d.App) + "/schedules"
}

func (d applicationData) AggregatesURL() string {
	return applicationPath(d.App) + "/aggregates/workflows"
}

func (a appSummary) ApplicationURL() string {
	return applicationPath(a.Name)
}

func applicationPath(app string) string {
	return web.ApplicationURL(app)
}

// handleApplication renders the overview shell for a single application without
// dispatching reads to its executors.
func (s *Server) handleApplication(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	window, err := newOverviewWindow(r.URL.Query().Get("range"), time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	executors := make([]hub.ExecutorView, 0)
	for _, executor := range s.hub.Executors() {
		if executor.App == app {
			executors = append(executors, executor)
		}
	}

	s.web.Page(w, "application", page{
		Title:         app + " · Application",
		AppsAvailable: s.appsAvailable(),
		Status:        s.statusForPage(false),
		Crumbs: []crumb{
			{Label: "Home", Href: "/"},
			{Label: app},
		},
		Data: applicationData{
			App:               app,
			Window:            window,
			Available:         len(executors) > 0,
			Executors:         executors,
			AggregatesEnabled: s.cfg.EnableAggregates,
		},
	})
}

func (d applicationData) OverviewURL() string { return applicationPath(d.App) + "/overview/" }
