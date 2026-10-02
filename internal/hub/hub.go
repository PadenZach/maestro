// Package hub maintains the pool of live executor WebSocket connections and the
// per-connection RPC machinery. It is the transport core conductor is built on:
// HTTP/API and background loops ask the hub for a connection to an app and issue
// request/response calls over it.
package hub

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/zpaden/maestro/internal/protocol"
)

// ErrAppUnavailable is returned by Request when no executor of the named app is
// connected. An application is available while it has at least one live socket.
var ErrAppUnavailable = errors.New("conductor: application unavailable")

var ErrHubClosed = errors.New("conductor: hub closed")

// Hub indexes live executor connections by application name.
type Hub struct {
	log            *slog.Logger
	requestTimeout time.Duration
	mu             sync.RWMutex
	apps           map[string]map[*Conn]struct{}
	active         map[*Conn]struct{} // includes sockets still handshaking
	sessions       int                // handlers admitted before upgrading; guarded with mu, never a WaitGroup Add/Wait race
	stopping       bool
	stopped        chan struct{}
	keepaliveTicks func(context.Context) <-chan time.Time
	ping           func(context.Context, *websocket.Conn) error
	disconnected   map[executorKey]DisconnectedExecutor
	changes        chan struct{}
}

// Shutdown rejects new sessions, closes all hijacked sockets (which HTTP
// Shutdown does not own), and waits for handlers and their transport workers.
func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	if !h.stopping {
		h.stopping = true
		if h.sessions == 0 {
			close(h.stopped)
		}
	}
	conns := make([]*Conn, 0, len(h.active))
	for c := range h.active {
		conns = append(conns, c)
	}
	h.mu.Unlock()
	for _, c := range conns {
		c.close(ErrHubClosed)
	}
	select {
	case <-h.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
		active:         make(map[*Conn]struct{}),
		stopped:        make(chan struct{}),
		disconnected:   make(map[executorKey]DisconnectedExecutor),
		changes:        make(chan struct{}, 1),
		ping:           func(ctx context.Context, c *websocket.Conn) error { return c.Ping(ctx) },
	}
}

// Accept upgrades an executor's HTTP request to a WebSocket, runs the
// EXECUTOR_INFO handshake, registers the executor, and then blocks until the
// connection closes (keeping the HTTP handler alive for the socket's lifetime).
// Authentication is delegated to the external gateway; the SDK URL key is ignored.
func (h *Hub) Accept(w http.ResponseWriter, r *http.Request, app string) error {
	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return ErrHubClosed
	}
	h.sessions++
	h.mu.Unlock()
	defer h.sessionDone()
	// Absent Origin is valid for server-process executors. The default
	// same-origin check still rejects cross-origin browser WebSockets.
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return err
	}
	// Conductor can push large frames (e.g. exported workflows); the Python
	// client sets max_size=None, so disable the inbound read limit.
	ws.SetReadLimit(-1)

	c := newConn(h, ws, app)
	h.mu.Lock()
	if h.stopping {
		h.mu.Unlock()
		_ = ws.CloseNow()
		return ErrHubClosed
	}
	h.active[c] = struct{}{}
	h.mu.Unlock()
	c.start()
	defer func() {
		c.close(errConnClosed)
		<-c.workersDone
		h.mu.Lock()
		delete(h.active, c)
		h.mu.Unlock()
	}()

	hctx, cancel := context.WithTimeout(c.ctx, handshakeTimeout)
	defer cancel()
	if err := c.handshake(hctx); err != nil {
		h.log.Warn("executor handshake failed", "app", app, "err", err)
		c.close(err)
		return err
	}

	if !h.register(c) {
		return ErrHubClosed
	}
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

func (h *Hub) sessionDone() {
	h.mu.Lock()
	h.sessions--
	if h.stopping && h.sessions == 0 {
		close(h.stopped)
	}
	h.mu.Unlock()
}

func (h *Hub) register(c *Conn) bool {
	h.mu.Lock()
	if h.stopping || c.ctx.Err() != nil {
		h.mu.Unlock()
		return false
	}
	if h.apps[c.app] == nil {
		h.apps[c.app] = make(map[*Conn]struct{})
	}
	var stale []*Conn
	for prior := range h.apps[c.app] {
		if prior.executor.ID == c.executor.ID {
			delete(h.apps[c.app], prior)
			stale = append(stale, prior)
		}
	}
	h.apps[c.app][c] = struct{}{}
	h.notifyChange()
	h.mu.Unlock()
	for _, prior := range stale {
		prior.close(errConnClosed)
	}
	return true
}

func (h *Hub) deregister(c *Conn) {
	h.mu.Lock()
	conns := h.apps[c.app]
	if _, registered := conns[c]; registered {
		delete(conns, c)
		key := executorKey{c.app, c.executor.ID, c.executor.Version}
		h.disconnected[key] = DisconnectedExecutor{c.executor.view(c.app), time.Now()}
		h.notifyChange()
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
// executor of app and returns the raw response bytes for the caller to decode.
// If no executor is connected it returns ErrAppUnavailable. Each
// attempt is bounded by the hub's requestTimeout; if the chosen socket dies
// mid-request it retries another executor only for documented pure reads. A caller-context
// cancellation or a per-request timeout surfaces immediately rather than
// retrying.
func (h *Hub) Request(ctx context.Context, app string, req protocol.Request) ([]byte, error) {
	return h.request(ctx, app, nil, req)
}

// RequestVersion restricts dispatch and any pure-read retries to an exact
// application version. Recovery uses this because the SDK recovers its own version.
func (h *Hub) RequestVersion(ctx context.Context, app, version string, req protocol.Request) ([]byte, error) {
	return h.request(ctx, app, &version, req)
}

func (h *Hub) request(ctx context.Context, app string, version *string, req protocol.Request) ([]byte, error) {
	h.mu.RLock()
	stopping := h.stopping
	h.mu.RUnlock()
	if stopping {
		return nil, ErrHubClosed
	}
	candidates := h.conns(app)
	if version != nil {
		matched := candidates[:0]
		for _, c := range candidates {
			if c.executor.Version == *version {
				matched = append(matched, c)
			}
		}
		candidates = matched
	}
	if len(candidates) == 0 {
		return nil, ErrAppUnavailable
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	// The open Request map also accepts MessageType values, which encode as
	// strings but must be normalized for response correlation and read retries.
	// Never modify the caller's map (it may be shared by concurrent requests).
	if typ, ok := req["type"].(protocol.MessageType); ok {
		copy := maps.Clone(req)
		copy["type"] = string(typ)
		req = copy
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
		if errors.Is(err, errConnClosed) && retryableRead(req) {
			continue
		}
		return nil, err
	}
	return nil, lastErr
}

// Only commands documented as pure reads may be replayed after an ambiguous
// disconnect. Unknown commands and mutations default to no retry; a request ID
// correlates replies but does not deduplicate executor-side effects.
func retryableRead(req protocol.Request) bool {
	typ, _ := req["type"].(string)
	switch protocol.MessageType(typ) {
	case protocol.MsgListWorkflows, protocol.MsgListQueuedWorkflows,
		protocol.MsgGetWorkflow, protocol.MsgListSteps,
		protocol.MsgGetWorkflowEvents, protocol.MsgGetWorkflowNotifications,
		protocol.MsgGetWorkflowStreams, protocol.MsgListQueues, protocol.MsgGetQueue,
		protocol.MsgExistPendingWorkflows, protocol.MsgGetWorkflowAggregates,
		protocol.MsgGetStepAggregates, protocol.MsgGetMetrics,
		protocol.MsgListSchedules, protocol.MsgGetSchedule,
		protocol.MsgListApplicationVersions:
		return true
	default:
		return false
	}
}

// Executors returns a snapshot of every connected executor for the JSON API.
func (h *Hub) Executors() []ExecutorView {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.executorsLocked()
}

func (h *Hub) executorsLocked() []ExecutorView {
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

type executorKey struct{ app, id, version string }

// DisconnectedExecutor is a transient notification consumed by recovery.
type DisconnectedExecutor struct {
	ExecutorView
	DisconnectedAt time.Time
}

// RecoverySnapshot atomically snapshots live executors and drains disconnect
// notifications. Stale sockets replaced by a reconnect never emit a disconnect.
func (h *Hub) RecoverySnapshot() ([]ExecutorView, []DisconnectedExecutor) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Changes after this snapshot remain queued until the next pass. The
	// coordinator can then defer dispatch until it has seen their timestamps.
	select {
	case <-h.changes:
	default:
	}
	disconnected := make([]DisconnectedExecutor, 0, len(h.disconnected))
	for _, executor := range h.disconnected {
		disconnected = append(disconnected, executor)
	}
	clear(h.disconnected)
	return h.executorsLocked(), disconnected
}

// Changes wakes the single recovery loop. Coalesced notifications are safe:
// the snapshot retains disconnects until the loop consumes them.
func (h *Hub) Changes() <-chan struct{} { return h.changes }

func (h *Hub) notifyChange() {
	select {
	case h.changes <- struct{}{}:
	default:
	}
}
