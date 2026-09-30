// Package reference embeds the pinned upstream HTTP contract used by the
// optional Conductor-compatible routes and their documentation.
package reference

import _ "embed"

// ConductorOpenAPI is the unmodified snapshot recorded in upstream-lock.json.
//
//go:embed conductor-openapi-2026-09-25.json
var ConductorOpenAPI []byte
