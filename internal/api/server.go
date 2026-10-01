// Package api serves conductor's HTTP surface: the executor WebSocket endpoint,
// a health check, the JSON API, and the embedded DBOS Console — HTMX pages
// for browsing workflows, steps, queues, and for basic management.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/zpaden/maestro/internal/config"
	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/web"
)

// Server wires the HTTP routes onto the hub.
type Server struct {
	cfg config.Config
	hub *hub.Hub
	log *slog.Logger
	web *web.Renderer
	mux *http.ServeMux
}

// New builds the HTTP server and registers routes. A template parse error is a
// programming error (templates are compiled into the binary), so it panics.
func New(cfg config.Config, h *hub.Hub, log *slog.Logger) *Server {
	if cfg.OrgName == "" {
		cfg.OrgName = "local"
	}
	r, err := web.New()
	if err != nil {
		panic(err)
	}
	s := &Server{cfg: cfg, hub: h, log: log, web: r, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Handler returns the root http.Handler.
func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.executorRoutes()
	s.consoleRoutes()
	api := s.httpAPI()
	s.jsonRoutes(api)
	s.conductorRoutes(api)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
