package console

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"io/fs"
	"net/http"

	"github.com/PadenZach/maestro/internal/hub"
)

// all: is required so partial files (templates/_*.html) are embedded — embed
// skips names beginning with "_" or "." otherwise.
//
//go:embed all:templates static
var embedded embed.FS

// pageFiles maps a logical page name to its template file. Each becomes its own
// parsed set so multiple {{define "content"}} blocks don't collide.
var pageFiles = map[string]string{
	"apps":            "templates/apps.html",
	"application":     "templates/application.html",
	"aggregates":      "templates/aggregates.html",
	"workflows":       "templates/workflows.html",
	"workflow_detail": "templates/workflow_detail.html",
	"queues":          "templates/queues.html",
	"queue_detail":    "templates/queue_detail.html",
	"schedules":       "templates/schedules.html",
	"schedule_detail": "templates/schedule_detail.html",
	"error":           "templates/error.html",
}

const partialGlob = "templates/_*.html"

// renderer holds the parsed template sets.
type renderer struct {
	pages    map[string]*template.Template
	partials *template.Template
}

// newRenderer parses all embedded templates. Call once at startup; a parse error here is
// a programming error (templates are compiled into the binary).
func newRenderer() (*renderer, error) {
	funcs := funcMap()
	funcs["maestroVersion"] = maestroVersion
	funcs["utcDateInput"] = UTCDateInput
	funcs["workflowCreatedUTC"] = WorkflowCreatedUTC
	r := &renderer{pages: make(map[string]*template.Template)}
	for name, file := range pageFiles {
		t, err := template.New(name).Funcs(funcs).ParseFS(embedded, "templates/layout.html", partialGlob, file)
		if err != nil {
			return nil, fmt.Errorf("parse page %q: %w", name, err)
		}
		r.pages[name] = t
	}
	p, err := template.New("partials").Funcs(funcs).ParseFS(embedded, partialGlob)
	if err != nil {
		return nil, fmt.Errorf("parse partials: %w", err)
	}
	r.partials = p
	return r, nil
}

// page renders a full page (layout + content) for the given page name.
func (r *renderer) page(w http.ResponseWriter, name string, data any) {
	t, ok := r.pages[name]
	if !ok {
		http.Error(w, "unknown page: "+name, http.StatusInternalServerError)
		return
	}
	r.execute(w, t, "layout", data)
}

// partial renders a single named fragment (an HTMX swap target) from the
// partials set.
func (r *renderer) partial(w http.ResponseWriter, name string, data any) {
	r.execute(w, r.partials, name, data)
}

func (r *renderer) execute(w http.ResponseWriter, t *template.Template, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, name, data); err != nil {
		// Headers may already be flushed; log-shaped errors are the caller's job.
		http.Error(w, "template error: "+err.Error(), http.StatusInternalServerError)
	}
}

// staticHandler serves the embedded /static assets (htmx.min.js, app.css).
func staticHandler() http.Handler {
	sub, err := fs.Sub(embedded, "static")
	if err != nil {
		panic(err) // embedded path is a compile-time constant
	}
	return http.FileServerFS(sub)
}

type crumb struct{ Label, Href string }

// page carries shared navigation and a connection-status snapshot for HTML views.
type page struct {
	Title         string
	AppsAvailable int
	Status        uiStatus
	Crumbs        []crumb
	Data          any
}

// renderErrorPage shows a full-page error (e.g. application unavailable).
func (s *handler) renderErrorPage(w http.ResponseWriter, crumbs []crumb, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, hub.ErrAppUnavailable) {
		status = http.StatusServiceUnavailable
	}
	s.renderStatusError(w, status, crumbs, err)
}

func (s *handler) renderStatusError(w http.ResponseWriter, status int, crumbs []crumb, err error) {
	w.WriteHeader(status)
	s.web.page(w, "error", page{
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
