// Package config holds the conductor server configuration. In M1 this is
// intentionally tiny: a listen address and a single static "conductor key"
// used to authenticate executor WebSocket connections. M2 replaces the static
// key with a Postgres-backed app/key registry.
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
	// ConductorKey is the single accepted API key for executor connections in
	// M1. Executors connect to /websocket/{app_name}/{conductor_key}; we accept
	// the upgrade only when {conductor_key} matches this value.
	ConductorKey string
	// RequestTimeout bounds a single server→executor round-trip issued by the
	// dispatcher (LIST_WORKFLOWS, GET_WORKFLOW, ...). If an executor does not
	// answer within this window the dispatcher gives up on that socket (and may
	// retry another executor of the same app).
	RequestTimeout time.Duration
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
	flag.StringVar(&cfg.ConductorKey, "key", cfg.ConductorKey, "accepted conductor API key (dev/static)")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "per-request executor round-trip timeout")
	flag.Parse()
	return cfg
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
