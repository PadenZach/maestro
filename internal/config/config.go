// Package config holds the conductor server configuration.
package config

import (
	"flag"
	"os"
	"time"
)

// Config is the fully-resolved server configuration.
type Config struct {
	// ListenAddr is the HTTP listen address (the WebSocket endpoint, health
	// check, and JSON API are all served here). Default ":8090" mirrors the
	// port the DBOS docs use for self-hosted Conductor.
	ListenAddr string
	// ConductorKey is ignored. Retained for compatibility with existing launchers;
	// the gateway owns authentication for executor and HTTP connections.
	ConductorKey string
	// RequestTimeout bounds a single server→executor round-trip issued by the
	// dispatcher (LIST_WORKFLOWS, GET_WORKFLOW, ...). If an executor does not
	// answer within this window the dispatcher gives up on that socket (and may
	// retry another executor of the same app).
	RequestTimeout time.Duration
	// LocalHTTPV2 exposes only a read-only, unauthenticated loopback test adapter.
	LocalHTTPV2 bool
}

// Load resolves configuration from environment variables, then lets command
// line flags override. Call once at startup.
func Load() Config {
	cfg := Config{
		ListenAddr:     envOr("CONDUCTOR_LISTEN_ADDR", ":8090"),
		ConductorKey:   envOr("CONDUCTOR_DEV_KEY", "dev-key"),
		RequestTimeout: 30 * time.Second,
	}
	flag.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "HTTP listen address")
	flag.StringVar(&cfg.ConductorKey, "key", cfg.ConductorKey, "ignored compatibility option; authentication belongs to the gateway")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "per-request executor round-trip timeout")
	flag.BoolVar(&cfg.LocalHTTPV2, "local-http-v2", false, "enable read-only local-org HTTP v2 adapter (loopback listener only)")
	flag.Parse()
	return cfg
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
