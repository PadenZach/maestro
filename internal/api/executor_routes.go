package api

import "net/http"

// executorRoutes accepts SDK connections; message schemas belong to the SDK protocol.
func (s *Server) executorRoutes() {
	s.mux.HandleFunc("GET /websocket/{app_name}/{conductor_key}", s.handleWS)
}

// handleWS hands the connection to the hub. Authentication belongs to the gateway;
// the SDK's conductor_key URL segment is retained for compatibility and ignored.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app_name")
	if err := s.hub.Accept(w, r, app); err != nil {
		s.log.Debug("websocket session ended", "app", app, "err", err)
	}
}
