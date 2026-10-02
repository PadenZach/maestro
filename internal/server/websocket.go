package server

import (
	"log/slog"
	"net/http"

	"github.com/PadenZach/maestro/internal/hub"
)

func registerWebSocket(mux *http.ServeMux, h *hub.Hub, log *slog.Logger) {
	// The SDK includes a key segment; authentication belongs to the gateway.
	mux.HandleFunc("GET /websocket/{app_name}/{conductor_key}", func(w http.ResponseWriter, r *http.Request) {
		if err := h.Accept(w, r, r.PathValue("app_name")); err != nil {
			log.Debug("websocket session ended", "app", r.PathValue("app_name"), "err", err)
		}
	})
}
