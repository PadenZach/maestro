// Package server composes the HTTP API, Console and executor endpoint.
package server

import (
	"log/slog"
	"net/http"

	"github.com/PadenZach/maestro/internal/api"
	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/console"
	"github.com/PadenZach/maestro/internal/hub"
)

func New(cfg config.Config, h *hub.Hub, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	registerWebSocket(mux, h, log)
	api.Register(mux, cfg, h)
	console.Register(mux, cfg, h)
	return mux
}
