package web

import (
	"runtime/debug"
	"strings"

	"github.com/zpaden/maestro"
)

// Revision and Modified are set with -ldflags -X when building without Git
// metadata, as in Docker. Otherwise Go's VCS build settings supply them.
var Revision, Modified string

func maestroVersion() string {
	info, _ := debug.ReadBuildInfo()
	return buildVersion(maestro.Version(), Revision, Modified, info)
}

func buildVersion(version, revision, modified string, info *debug.BuildInfo) string {
	if info != nil {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if revision == "" {
					revision = setting.Value
				}
			case "vcs.modified":
				if modified == "" {
					modified = setting.Value
				}
			}
		}
	}
	if revision == "" {
		revision = "unknown"
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	separator := "+"
	if strings.Contains(version, "+") {
		separator = "."
	}
	version += separator + revision
	if modified == "true" {
		version += ".dirty"
	}
	return version
}
