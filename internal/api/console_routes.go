package api

import (
	"github.com/zpaden/maestro/internal/web"
	"net/http"
)

// consoleRoutes registers HTML pages and HTMX fragments independently of JSON documentation.
func (s *Server) consoleRoutes() {
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
	s.mux.HandleFunc("GET /apps/{app}/workflows/{id}/inspect", s.handleWorkflowInspection)
	s.mux.HandleFunc("GET /apps/{app}/workflows/{id}/blob", s.handleWorkflowBlob)
	s.mux.HandleFunc("POST /apps/{app}/workflows/{id}/cancel", s.handleCancel)
	s.mux.HandleFunc("POST /apps/{app}/workflows/{id}/resume", s.handleResume)
	s.mux.HandleFunc("GET /apps/{app}/queues", s.handleQueues)
	s.mux.HandleFunc("GET /apps/{app}/queues/{name}", s.handleQueueDetail)
	s.mux.HandleFunc("GET /apps/{app}/queue", s.handleQueueDetailAlias)
	s.mux.HandleFunc("GET /apps/{app}/schedules", s.handleSchedules)
	s.mux.HandleFunc("GET /apps/{app}/schedule", s.handleScheduleDetail)

}
