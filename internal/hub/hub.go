// Package hub maintains the pool of live executor WebSocket connections and the
// per-connection RPC machinery. It is the transport core conductor is built on:
// HTTP/API and background loops ask the hub for a connection to an app and issue
// request/response calls over it.
package hub

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/zpaden/maestro/internal/protocol"
)

// ErrAppUnavailable is returned by Request when no executor of the named app is
// connected. The application is "available" iff ≥1 live socket (spec §5.1).
var ErrAppUnavailable = errors.New("conductor: application unavailable")

// Hub indexes live executor connections by application name.
type Hub struct {
	log            *slog.Logger
	requestTimeout time.Duration
	mu             sync.RWMutex
	apps           map[string]map[*Conn]struct{}
}

// New constructs an empty hub. requestTimeout bounds a single executor
// round-trip issued by Request; a non-positive value falls back to 30s.
func New(log *slog.Logger, requestTimeout time.Duration) *Hub {
	if requestTimeout <= 0 {
		requestTimeout = 30 * time.Second
	}
	return &Hub{
		log:            log,
		requestTimeout: requestTimeout,
		apps:           make(map[string]map[*Conn]struct{}),
	}
}

// Accept upgrades an executor's HTTP request to a WebSocket, runs the
// EXECUTOR_INFO handshake, registers the executor, and then blocks until the
// connection closes (keeping the HTTP handler alive for the socket's lifetime).
// The caller must have already authenticated the conductor key.
func (h *Hub) Accept(w http.ResponseWriter, r *http.Request, app string) error {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Executors are server processes (no browser Origin); accept any.
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		return err
	}
	// Conductor can push large frames (e.g. exported workflows); the Python
	// client sets max_size=None, so disable the inbound read limit.
	ws.SetReadLimit(-1)

	c := newConn(h, ws, app)
	c.start()

	hctx, cancel := context.WithTimeout(c.ctx, handshakeTimeout)
	defer cancel()
	if err := c.handshake(hctx); err != nil {
		h.log.Warn("executor handshake failed", "app", app, "err", err)
		c.close(err)
		return err
	}

	h.register(c)
	defer h.deregister(c)
	h.log.Info("executor connected",
		"app", app,
		"executor_id", c.executor.ID,
		"version", c.executor.Version,
		"language", c.executor.Language,
	)

	<-c.ctx.Done()
	return nil
}

func (h *Hub) register(c *Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.apps[c.app] == nil {
		h.apps[c.app] = make(map[*Conn]struct{})
	}
	h.apps[c.app][c] = struct{}{}
}

func (h *Hub) deregister(c *Conn) {
	h.mu.Lock()
	conns := h.apps[c.app]
	if conns != nil {
		delete(conns, c)
		if len(conns) == 0 {
			delete(h.apps, c.app)
		}
	}
	h.mu.Unlock()

	id := ""
	if c.executor != nil {
		id = c.executor.ID
	}
	h.log.Info("executor disconnected", "app", c.app, "executor_id", id)
}

// Pick returns a live connection for the named app. Selection is trivial today
// (first available); the dispatcher (Request) layers retry on top.
func (h *Hub) Pick(app string) (*Conn, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.apps[app] {
		return c, true
	}
	return nil, false
}

// conns snapshots the live connections for an app so the dispatcher can iterate
// candidates without holding the hub lock during a round-trip.
func (h *Hub) conns(app string) []*Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	set := h.apps[app]
	out := make([]*Conn, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	return out
}

// Request is the dispatcher: it sends a server-initiated request to a healthy
// executor of app and returns the raw response bytes for the caller to decode
// (spec §3.5). If no executor is connected it returns ErrAppUnavailable. Each
// attempt is bounded by the hub's requestTimeout; if the chosen socket dies
// mid-request it retries another executor of the same app. A caller-context
// cancellation or a per-request timeout surfaces immediately rather than
// retrying.
func (h *Hub) Request(ctx context.Context, app string, req protocol.Request) ([]byte, error) {
	candidates := h.conns(app)
	if len(candidates) == 0 {
		return nil, ErrAppUnavailable
	}
	var lastErr error
	for _, c := range candidates {
		rctx, cancel := context.WithTimeout(ctx, h.requestTimeout)
		resp, err := c.roundtrip(rctx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		lastErr = err
		// Retry on a different executor only when this socket dropped. Timeouts
		// and caller cancellations are surfaced so the Console sees them.
		if errors.Is(err, errConnClosed) {
			continue
		}
		return nil, err
	}
	return nil, lastErr
}

// Executors returns a snapshot of every connected executor for the JSON API.
func (h *Hub) Executors() []ExecutorView {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]ExecutorView, 0)
	for app, conns := range h.apps {
		for c := range conns {
			c.mu.Lock()
			ex := c.executor
			c.mu.Unlock()
			if ex == nil {
				continue
			}
			out = append(out, ex.view(app))
		}
	}
	return out
}
