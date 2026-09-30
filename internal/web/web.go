// Package web is maestro's embedded DBOS Console: Go html/template pages plus a
// vendored copy of HTMX and a hand-written dark stylesheet, all baked into the
// binary via embed.FS so the console ships as a single artifact with no build
// step and works offline.
//
// Rendering model: a shared layout.html (define "layout") wraps each page's
// {{define "content"}}. Because html/template keeps one namespace per set, each
// page is parsed into its OWN template set (layout + shared partials + that
// page). HTMX fragment responses are served from a separate partials set, keyed
// by the partial's define name.
package web

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
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
	"workflows":       "templates/workflows.html",
	"workflow_detail": "templates/workflow_detail.html",
	"queues":          "templates/queues.html",
	"queue_detail":    "templates/queue_detail.html",
	"error":           "templates/error.html",
}

const partialGlob = "templates/_*.html"

// Renderer holds the parsed template sets.
type Renderer struct {
	pages    map[string]*template.Template
	partials *template.Template
}

// New parses all embedded templates. Call once at startup; a parse error here is
// a programming error (templates are compiled into the binary).
func New() (*Renderer, error) {
	funcs := funcMap()
	r := &Renderer{pages: make(map[string]*template.Template)}
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

// Page renders a full page (layout + content) for the given page name.
func (r *Renderer) Page(w http.ResponseWriter, name string, data any) {
	t, ok := r.pages[name]
	if !ok {
		http.Error(w, "unknown page: "+name, http.StatusInternalServerError)
		return
	}
	r.execute(w, t, "layout", data)
}

// Partial renders a single named fragment (an HTMX swap target) from the
// partials set.
func (r *Renderer) Partial(w http.ResponseWriter, name string, data any) {
	r.execute(w, r.partials, name, data)
}

func (r *Renderer) execute(w http.ResponseWriter, t *template.Template, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, name, data); err != nil {
		// Headers may already be flushed; log-shaped errors are the caller's job.
		http.Error(w, "template error: "+err.Error(), http.StatusInternalServerError)
	}
}

// StaticHandler serves the embedded /static assets (htmx.min.js, app.css).
func StaticHandler() http.Handler {
	sub, err := fs.Sub(embedded, "static")
	if err != nil {
		panic(err) // embedded path is a compile-time constant
	}
	return http.FileServerFS(sub)
}

// Assets exposes the embedded static FS (used by tests).
func Assets() fs.FS {
	sub, _ := fs.Sub(embedded, "static")
	return sub
}
