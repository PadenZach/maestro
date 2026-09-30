package hub

import "errors"

// IsExecutorConnectionClosed reports whether err has the hub's private
// connection-closed identity. Matching by text would rewrite opaque SDK errors.
func IsExecutorConnectionClosed(err error) bool {
	return errors.Is(err, errConnClosed)
}

// ConnectionStatus is a point-in-time view of executor connection readiness.
// Active includes sockets still completing executor_info; Ready includes only
// registered sockets that can receive requests.
type ConnectionStatus struct {
	Active  int
	Ready   int
	Pending int
}

// ConnectionStatus snapshots active and registered sockets under one read lock.
func (h *Hub) ConnectionStatus() ConnectionStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()

	status := ConnectionStatus{Active: len(h.active)}
	for _, conns := range h.apps {
		status.Ready += len(conns)
	}
	status.Pending = status.Active - status.Ready
	if status.Pending < 0 {
		status.Pending = 0
	}
	return status
}
