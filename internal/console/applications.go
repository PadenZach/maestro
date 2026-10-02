package console

import (
	"net/http"
	"sort"
	"time"

	"github.com/PadenZach/maestro/internal/hub"
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
	return ApplicationURL(app)
}

// handleApplication renders the overview shell for a single application without
// dispatching reads to its executors.
func (s *handler) handleApplication(w http.ResponseWriter, r *http.Request) {
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

	s.web.page(w, "application", page{
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

type appsData struct{ Apps []appSummary }

type appSummary struct {
	Name      string
	Available bool
	Executors []hub.ExecutorView
}

// appsAvailable preserves the distinct ready-app count used by page models.
func (s *handler) appsAvailable() int {
	seen := map[string]struct{}{}
	for _, e := range s.hub.Executors() {
		seen[e.App] = struct{}{}
	}
	return len(seen)
}

func (s *handler) handleHome(w http.ResponseWriter, r *http.Request) {
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
	s.web.page(w, "apps", page{
		Title:         "Applications",
		AppsAvailable: len(names),
		Status:        s.statusForPage(false),
		Crumbs:        []crumb{{Label: "Home"}},
		Data:          appsData{Apps: apps},
	})
}
