package console

import (
	"errors"

	"github.com/PadenZach/maestro/internal/hub"
)

// uiStatus describes this rendered page, without retaining health history.
type uiStatus struct {
	State string
	Label string
}

func (s *handler) statusForPage(degraded bool) uiStatus {
	connections := s.hub.ConnectionStatus()
	switch {
	case connections.Active == 0:
		return uiStatus{State: "red", Label: "Application status: no connections"}
	case degraded:
		return uiStatus{State: "amber", Label: "Application status: degraded"}
	case connections.Pending > 0 || connections.Ready == 0:
		return uiStatus{State: "amber", Label: "Application status: connection pending"}
	default:
		return uiStatus{State: "green", Label: "Application status: connected and ready"}
	}
}

// htmlErrorText changes only errors with maestro-owned hub identities.
// Executor-provided error_message values remain opaque and untouched.
func htmlErrorText(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, hub.ErrAppUnavailable):
		return "maestro: application unavailable"
	case errors.Is(err, hub.ErrHubClosed):
		return "maestro: hub closed"
	case hub.IsExecutorConnectionClosed(err):
		return "maestro: executor connection closed"
	default:
		return err.Error()
	}
}
