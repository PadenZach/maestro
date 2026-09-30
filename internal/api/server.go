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
	// Transport and health.
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /websocket/{app_name}/{conductor_key}", s.handleWS)

	// Static assets for the console.
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", web.StaticHandler()))

	// Console (HTML, HTMX-enhanced).
	s.mux.HandleFunc("GET /{$}", s.handleHome)
	s.mux.HandleFunc("GET /apps/{app}", s.handleApplication)
	s.mux.HandleFunc("GET /apps/{app}/{$}", s.handleApplication)
	s.mux.HandleFunc("GET /apps/{app}/workflows", s.handleWorkflows)
	s.mux.HandleFunc("GET /apps/{app}/workflows/rows", s.handleWorkflowRows)
	s.mux.HandleFunc("GET /apps/{app}/workflows/{id}", s.handleWorkflowDetail)
	s.mux.HandleFunc("GET /apps/{app}/workflows/{id}/live", s.handleWorkflowLive)
	s.mux.HandleFunc("GET /apps/{app}/workflows/{id}/timeline", s.handleWorkflowTimeline)
	s.mux.HandleFunc("GET /apps/{app}/workflows/{id}/blob", s.handleWorkflowBlob)
	s.mux.HandleFunc("POST /apps/{app}/workflows/{id}/cancel", s.handleCancel)
	s.mux.HandleFunc("POST /apps/{app}/workflows/{id}/resume", s.handleResume)
	s.mux.HandleFunc("GET /apps/{app}/queues", s.handleQueues)
	s.mux.HandleFunc("GET /apps/{app}/queues/{name}", s.handleQueueDetail)
	s.mux.HandleFunc("GET /apps/{app}/queue", s.handleQueueDetailAlias)
	s.mux.HandleFunc("GET /apps/{app}/schedules", s.handleSchedules)
	s.mux.HandleFunc("GET /apps/{app}/schedule", s.handleScheduleDetail)

	// The official HTTP subset is an explicit loopback-only test adapter.
	if s.cfg.LocalHTTPV2 {
		s.mux.HandleFunc("GET /v2/orgs/{org}/apps/{app}/schedules", s.localV2Schedules)
		s.mux.HandleFunc("GET /v2/orgs/{org}/apps/{app}/schedules/{name}", s.localV2GetSchedule)
		s.mux.HandleFunc("GET /v2/orgs/{org}/apps/{app}/queues", s.localV2Queues)
		s.mux.HandleFunc("GET /v2/orgs/{org}/apps/{app}/queues/{name}", s.localV2GetQueue)
		s.mux.HandleFunc("POST /v2/orgs/{org}/apps/{app}/workflows/search", s.localV2Search)
		s.mux.HandleFunc("GET /v2/orgs/{org}/apps/{app}/workflows/{id}", s.localV2Get)
		s.mux.HandleFunc("GET /v2/orgs/{org}/apps/{app}/workflows/{id}/steps", s.localV2Steps)
	}

	// JSON API (mirror of the reads, for tests/SDKs).
	s.mux.HandleFunc("GET /api/executors", s.handleExecutors)
	s.mux.HandleFunc("GET /api/apps", s.handleAPIApps)
	s.mux.HandleFunc("GET /api/{app}/workflows", s.handleAPIWorkflows)
	s.mux.HandleFunc("GET /api/{app}/workflows/{id}", s.handleAPIWorkflow)
	s.mux.HandleFunc("GET /api/{app}/workflows/{id}/steps", s.handleAPISteps)
	s.mux.HandleFunc("GET /api/{app}/workflows/{id}/events", s.handleAPIEvents)
	s.mux.HandleFunc("GET /api/{app}/workflows/{id}/notifications", s.handleAPINotifications)
	s.mux.HandleFunc("GET /api/{app}/workflows/{id}/streams", s.handleAPIStreams)
	s.mux.HandleFunc("GET /api/{app}/queues", s.handleAPIQueues)
	s.mux.HandleFunc("GET /api/{app}/queues/{name}", s.handleAPIQueue)
}

// handleHealth mirrors the shape the DBOS client expects from /conductor.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

// handleWS authenticates the conductor key and hands the connection to the hub.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app_name")
	key := r.PathValue("conductor_key")
	if key != s.cfg.ConductorKey {
		http.Error(w, "invalid conductor key", http.StatusUnauthorized)
		return
	}
	if err := s.hub.Accept(w, r, app); err != nil {
		s.log.Debug("websocket session ended", "app", app, "err", err)
	}
}

func (s *Server) handleExecutors(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.hub.Executors())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
