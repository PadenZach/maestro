// Package maestro exposes the project's version, embedded in every build.
package maestro

import (
	_ "embed"
	"strings"
)

//go:embed VERSION
var version string

// Version returns the release version recorded in VERSION.
func Version() string {
	return strings.TrimSpace(version)
}
