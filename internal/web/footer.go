package web

import "runtime/debug"

// Version may be set at build time with -ldflags -X.
var Version string

func maestroVersion() string {
	info, _ := debug.ReadBuildInfo()
	return buildVersion(Version, info)
}

func buildVersion(override string, info *debug.BuildInfo) string {
	if override != "" {
		return override
	}
	if info == nil {
		return "dev"
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	revision, modified := "", false
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if revision == "" {
		return "dev"
	}
	if len(revision) > 12 {
		revision = revision[:12]
	}
	version := "dev+" + revision
	if modified {
		version += ".dirty"
	}
	return version
}
