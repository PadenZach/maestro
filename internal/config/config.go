// Package config holds the conductor server configuration.
package config

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"
)

// Config is the fully-resolved server configuration.
type Config struct {
	// ListenAddr is the HTTP listen address (the WebSocket endpoint, health
	// check, and JSON API are all served here). Defaults to loopback port 8090.
	ListenAddr string
	// ConductorKey is ignored. Retained for compatibility with existing launchers;
	// the gateway owns authentication for executor and HTTP connections.
	ConductorKey string
	// RequestTimeout bounds a single server→executor round-trip issued by the
	// dispatcher (LIST_WORKFLOWS, GET_WORKFLOW, ...). If an executor does not
	// answer within this window the dispatcher gives up on that socket (and may
	// retry another executor of the same app).
	RequestTimeout time.Duration
	// OrgName is the single organization served by this deployment.
	OrgName string
	// AllowRemote lifts the listener and Conductor client loopback restriction for
	// deployments behind a gateway. It does not provide authentication.
	AllowRemote bool
	// EnableAggregates exposes the advanced aggregate viewer and query API.
	// Fixed application overview queries remain available independently.
	EnableAggregates bool
}

// Load resolves configuration from environment variables, then lets command
// line flags override. Call once at startup.
func Load() Config {
	cfg := Config{
		ListenAddr:     envOr("CONDUCTOR_LISTEN_ADDR", "127.0.0.1:8090"),
		ConductorKey:   envOr("CONDUCTOR_DEV_KEY", "dev-key"),
		RequestTimeout: 30 * time.Second,
		OrgName:        envOr("CONDUCTOR_ORG_NAME", "local"),
	}
	flags := flag.NewFlagSet("maestro", flag.ExitOnError)
	flags.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "HTTP listen address")
	flags.StringVar(&cfg.ConductorKey, "key", cfg.ConductorKey, "ignored compatibility option; authentication belongs to the gateway")
	flags.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "per-request executor round-trip timeout")
	flags.StringVar(&cfg.OrgName, "org", cfg.OrgName, "single organization identifier (3-30 lowercase letters, digits or underscores)")
	flags.BoolVar(&cfg.AllowRemote, "allow-remote", envBool("CONDUCTOR_ALLOW_REMOTE"), "allow non-loopback access behind an external gateway")
	flags.BoolVar(&cfg.EnableAggregates, "enable-aggregates", envBool("CONDUCTOR_ENABLE_AGGREGATES"), "enable advanced aggregate queries and viewer (disabled by default)")
	_ = flags.Parse(os.Args[1:])
	if !regexp.MustCompile(`^[a-z0-9_]{3,30}$`).MatchString(cfg.OrgName) {
		fmt.Fprintln(os.Stderr, "--org must contain 3-30 lowercase letters, digits or underscores")
		os.Exit(2)
	}
	return cfg
}

func envBool(key string) bool {
	raw, present := os.LookupEnv(key)
	if !present {
		return false
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		fmt.Fprintln(os.Stderr, key+" must be boolean")
		os.Exit(2)
	}
	return value
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
