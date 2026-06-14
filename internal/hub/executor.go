package hub

import "time"

// Executor is the identity of a single connected DBOS application process,
// learned from its EXECUTOR_INFO response. executor_id is regenerated on every
// process restart, so it is treated as ephemeral — liveness is keyed by the
// live socket, not by this id.
type Executor struct {
	ID          string
	Version     string
	Hostname    string
	Language    string
	DBOSVersion string
	Metadata    map[string]any
	ConnectedAt time.Time
}

// ExecutorView is the JSON-serializable snapshot returned by GET /api/executors.
type ExecutorView struct {
	ExecutorID  string         `json:"executor_id"`
	App         string         `json:"app"`
	Version     string         `json:"application_version"`
	Hostname    string         `json:"hostname,omitempty"`
	Language    string         `json:"language,omitempty"`
	DBOSVersion string         `json:"dbos_version,omitempty"`
	Metadata    map[string]any `json:"executor_metadata,omitempty"`
	ConnectedAt time.Time      `json:"connected_at"`
}

func (e *Executor) view(app string) ExecutorView {
	return ExecutorView{
		ExecutorID:  e.ID,
		App:         app,
		Version:     e.Version,
		Hostname:    e.Hostname,
		Language:    e.Language,
		DBOSVersion: e.DBOSVersion,
		Metadata:    e.Metadata,
		ConnectedAt: e.ConnectedAt,
	}
}
