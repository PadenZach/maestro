package console

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/PadenZach/maestro/internal/protocol"
)

// defaultPageSize bounds the workflow list; the list view never loads
// input/output blobs (those lazy-load on the detail page).
const defaultPageSize = 25

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

func (s *handler) handleWorkflows(w http.ResponseWriter, r *http.Request) {
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
	s.web.page(w, "workflows", page{
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

func (s *handler) handleWorkflowRows(w http.ResponseWriter, r *http.Request) {
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
	location := workflowsURL(app, f, offset)
	if current, err := url.Parse(r.Header.Get("HX-Current-URL")); err == nil && current.RequestURI() == location {
		// Refreshing the current page must not add a duplicate history entry.
		w.Header().Set("HX-Push-Url", "false")
	} else {
		w.Header().Set("HX-Push-Url", location)
	}
	s.web.partial(w, "workflow_rows", rows)
}

// fetchRows runs LIST_WORKFLOWS for the given filter/offset and builds the table
// view model, requesting one extra row to detect a next page.
func (s *handler) fetchRows(ctx context.Context, app string, f filterState, offset int) (workflowRows, error) {
	limit := defaultPageSize
	over := limit + 1
	body := protocol.ListWorkflowsBody{
		SortDesc: true,
		Limit:    &over,
		Offset:   &offset,
	}
	body.Status = f.Statuses
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

	wfs, err := s.hub.Workflows(ctx, app, body)
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

func rowsURL(app string, f filterState, offset int) string {
	return applicationPath(app) + "/workflows/rows?" + workflowFilterQuery(f, offset)
}

func workflowsURL(app string, f filterState, offset int) string {
	return applicationPath(app) + "/workflows?" + workflowFilterQuery(f, offset)
}

func workflowsCrumbs(app string) []crumb {
	return []crumb{{Label: "Home", Href: "/"}, {Label: app, Href: applicationPath(app)}, {Label: "Workflows"}}
}
