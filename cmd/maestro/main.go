// Command maestro is the Go port of the DBOS Conductor control plane.
// Its WebSocket hub completes the EXECUTOR_INFO handshake with DBOS executors
// and lists connected executors under GET /api/executors.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zpaden/maestro/internal/api"
	"github.com/zpaden/maestro/internal/config"
	"github.com/zpaden/maestro/internal/hub"
)

func serve(ctx context.Context, listener net.Listener, server *http.Server, h *hub.Hub) error {
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-served:
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Shutdown closes the HTTP listener but waits for active HTTP handlers.
	// Tear down upgraded sockets concurrently so handlers blocked on executor
	// RPCs can finish before the shared shutdown deadline expires.
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Shutdown(shutdownCtx) }()
	hubErr := h.Shutdown(shutdownCtx)
	httpErr := <-httpDone
	if serveErr == nil {
		serveErr = <-served
	}
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, httpErr, hubErr)
}

// validateHTTPListener checks the bound socket rather than trusting a hostname
// that might resolve to a different address at listen time.
func validateHTTPListener(restrictToLoopback bool, addr net.Addr) error {
	if !restrictToLoopback {
		return nil
	}
	tcp, ok := addr.(*net.TCPAddr)
	if !ok || tcp.IP == nil || !tcp.IP.IsLoopback() {
		return fmt.Errorf("non-loopback listeners require --allow-remote")
	}
	return nil
}

func main() {
	cfg := config.Load()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(log)

	h := hub.New(log, cfg.RequestTimeout)
	srv := api.New(cfg, h, log)

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Error("http listen failed", "err", err)
		os.Exit(1)
	}
	if err := validateHTTPListener(!cfg.AllowRemote, listener.Addr()); err != nil {
		_ = listener.Close()
		log.Error("invalid HTTP listener configuration", "err", err)
		os.Exit(1)
	}
	log.Info("maestro listening", "addr", listener.Addr().String())
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, listener, &http.Server{Handler: srv.Handler()}, h); err != nil {
		log.Error("server shutdown failed", "err", err)
		os.Exit(1)
	}
}
