package config_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/PadenZach/maestro"
	"github.com/PadenZach/maestro/internal/config"
)

func TestConfigurationProcess(t *testing.T) {
	if os.Getenv("MAESTRO_CONFIG_TEST") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"maestro"}, os.Args[i+1:]...)
			break
		}
	}
	_ = json.NewEncoder(os.Stdout).Encode(config.Load())
	os.Exit(0)
}

func TestDeploymentConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     []string
		args    []string
		want    map[string]any
		output  string
		invalid bool
	}{
		{name: "defaults", want: map[string]any{"ListenAddr": "127.0.0.1:8090", "OrgName": "local", "EnableAggregates": false, "RequestTimeout": float64(30 * time.Second)}},
		{name: "version", args: []string{"--version"}, output: "maestro " + maestro.Version() + "\n"},
		{name: "request timeout flag", args: []string{"--request-timeout=5s"}, want: map[string]any{"RequestTimeout": float64(5 * time.Second)}},
		{name: "zero request timeout", args: []string{"--request-timeout=0s"}, output: "--request-timeout must be positive\n", invalid: true},
		{name: "negative request timeout", args: []string{"--request-timeout=-1s"}, output: "--request-timeout must be positive\n", invalid: true},
		{name: "recovery default", want: map[string]any{"RecoveryTimeout": float64(time.Minute)}},
		{name: "recovery environment", env: []string{"MAESTRO_RECOVERY_TIMEOUT=90s"}, want: map[string]any{"RecoveryTimeout": float64(90 * time.Second)}},
		{name: "recovery flag overrides environment", env: []string{"MAESTRO_RECOVERY_TIMEOUT=90s"}, args: []string{"--recovery-timeout=2m"}, want: map[string]any{"RecoveryTimeout": float64(2 * time.Minute)}},
		{name: "invalid recovery environment", env: []string{"MAESTRO_RECOVERY_TIMEOUT=perhaps"}, invalid: true},
		{name: "zero recovery timeout", args: []string{"--recovery-timeout=0s"}, invalid: true},
		{name: "negative recovery timeout", args: []string{"--recovery-timeout=-1s"}, invalid: true},
		{name: "aggregate flag", args: []string{"--enable-aggregates"}, want: map[string]any{"EnableAggregates": true}},
		{name: "aggregate environment", env: []string{"MAESTRO_ENABLE_AGGREGATES=true"}, want: map[string]any{"EnableAggregates": true}},
		{name: "aggregate flag overrides environment", env: []string{"MAESTRO_ENABLE_AGGREGATES=true"}, args: []string{"--enable-aggregates=false"}, want: map[string]any{"EnableAggregates": false}},
		{name: "invalid aggregate boolean", env: []string{"MAESTRO_ENABLE_AGGREGATES=perhaps"}, invalid: true},
		{name: "environment", env: []string{"MAESTRO_ORG_NAME=acme", "MAESTRO_LISTEN_ADDR=0.0.0.0:8090"}, want: map[string]any{"OrgName": "acme", "ListenAddr": "0.0.0.0:8090"}},
		{name: "flags override environment", env: []string{"MAESTRO_ORG_NAME=acme", "MAESTRO_LISTEN_ADDR=127.0.0.1:8090"}, args: []string{"--org=team", "--listen=:9000"}, want: map[string]any{"OrgName": "team", "ListenAddr": ":9000"}},
		{name: "empty org", args: []string{"--org="}, invalid: true},
		{name: "org path", args: []string{"--org=../team"}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestConfigurationProcess$", "--"}, tc.args...)...)
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "MAESTRO_") {
					cmd.Env = append(cmd.Env, value)
				}
			}
			cmd.Env = append(cmd.Env, "MAESTRO_CONFIG_TEST=1")
			cmd.Env = append(cmd.Env, tc.env...)
			output, err := cmd.CombinedOutput()
			if tc.output != "" && string(output) != tc.output {
				t.Fatalf("output = %q, want %q", output, tc.output)
			}
			if tc.invalid {
				if err == nil || cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 2 {
					t.Fatalf("invalid configuration must exit 2: %v: %s", err, output)
				}
				return
			}
			if err != nil {
				t.Fatalf("configuration failed: %v: %s", err, output)
			}
			if tc.output != "" {
				return
			}
			var got map[string]any
			if err := json.Unmarshal(output, &got); err != nil {
				t.Fatal(err)
			}
			for field, want := range tc.want {
				if got[field] != want {
					t.Errorf("%s = %v, want %v", field, got[field], want)
				}
			}
		})
	}
}
