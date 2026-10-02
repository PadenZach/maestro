// Package config holds the Maestro server configuration.
package config

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/PadenZach/maestro"
)

// Config is the fully-resolved server configuration.
type Config struct {
	// ListenAddr is the HTTP listen address (the WebSocket endpoint, health
	// check, and JSON API are all served here). Defaults to loopback port 8090.
	ListenAddr string
	// RequestTimeout bounds a single server→executor round-trip issued by the
	// dispatcher (LIST_WORKFLOWS, GET_WORKFLOW, ...). If an executor does not
	// answer within this window the dispatcher gives up on that socket (and may
	// retry another executor of the same app).
	RequestTimeout time.Duration
	// RecoveryTimeout is how long an absent executor must wait before recovery.
	RecoveryTimeout time.Duration
	// OrgName is the single organization served by this deployment.
	OrgName string
	// EnableAggregates exposes the advanced aggregate viewer and query API.
	// Fixed application overview queries remain available independently.
	EnableAggregates bool
}

// Load resolves configuration from environment variables, then lets command
// line flags override. Call once at startup.
func Load() Config {
	cfg := Config{
		ListenAddr:      envOr("MAESTRO_LISTEN_ADDR", "127.0.0.1:8090"),
		RequestTimeout:  30 * time.Second,
		RecoveryTimeout: envDuration("MAESTRO_RECOVERY_TIMEOUT", time.Minute),
		OrgName:         envOr("MAESTRO_ORG_NAME", "local"),
	}
	flags := flag.NewFlagSet("maestro", flag.ExitOnError)
	version := flags.Bool("version", false, "print version and exit")
	flags.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "HTTP listen address")
	flags.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "per-request executor round-trip timeout")
	flags.DurationVar(&cfg.RecoveryTimeout, "recovery-timeout", cfg.RecoveryTimeout, "executor disconnect timeout before workflow recovery")
	flags.StringVar(&cfg.OrgName, "org", cfg.OrgName, "single organization identifier (3-30 lowercase letters, digits or underscores)")
	flags.BoolVar(&cfg.EnableAggregates, "enable-aggregates", envBool("MAESTRO_ENABLE_AGGREGATES"), "enable advanced aggregate queries and viewer (disabled by default)")
	_ = flags.Parse(os.Args[1:])
	if *version {
		fmt.Fprintln(os.Stdout, "maestro", maestro.Version())
		os.Exit(0)
	}
	if cfg.RequestTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "--request-timeout must be positive")
		os.Exit(2)
	}
	if cfg.RecoveryTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "--recovery-timeout must be positive")
		os.Exit(2)
	}
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

func envDuration(key string, fallback time.Duration) time.Duration {
	raw, present := os.LookupEnv(key)
	if !present {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		fmt.Fprintln(os.Stderr, key+" must be a positive duration")
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
