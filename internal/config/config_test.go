package config_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/zpaden/maestro/internal/config"
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

// TODO Foundation and CONTRACTS deployment assumptions: one configurable org,
// loopback by default, explicit gateway deployment override, flags over env.
func TestDeploymentConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name    string
		env     []string
		args    []string
		want    map[string]any
		invalid bool
	}{
		{name: "defaults", want: map[string]any{"ListenAddr": "127.0.0.1:8090", "OrgName": "local", "AllowRemote": false, "EnableAggregates": false}},
		{name: "recovery default", want: map[string]any{"RecoveryTimeout": float64(time.Minute)}},
		{name: "recovery environment", env: []string{"CONDUCTOR_RECOVERY_TIMEOUT=90s"}, want: map[string]any{"RecoveryTimeout": float64(90 * time.Second)}},
		{name: "recovery flag overrides environment", env: []string{"CONDUCTOR_RECOVERY_TIMEOUT=90s"}, args: []string{"--recovery-timeout=2m"}, want: map[string]any{"RecoveryTimeout": float64(2 * time.Minute)}},
		{name: "invalid recovery environment", env: []string{"CONDUCTOR_RECOVERY_TIMEOUT=perhaps"}, invalid: true},
		{name: "zero recovery timeout", args: []string{"--recovery-timeout=0s"}, invalid: true},
		{name: "negative recovery timeout", args: []string{"--recovery-timeout=-1s"}, invalid: true},
		{name: "aggregate flag", args: []string{"--enable-aggregates"}, want: map[string]any{"EnableAggregates": true}},
		{name: "aggregate environment", env: []string{"CONDUCTOR_ENABLE_AGGREGATES=true"}, want: map[string]any{"EnableAggregates": true}},
		{name: "aggregate flag overrides environment", env: []string{"CONDUCTOR_ENABLE_AGGREGATES=true"}, args: []string{"--enable-aggregates=false"}, want: map[string]any{"EnableAggregates": false}},
		{name: "invalid aggregate boolean", env: []string{"CONDUCTOR_ENABLE_AGGREGATES=perhaps"}, invalid: true},
		{name: "environment", env: []string{"CONDUCTOR_ORG_NAME=acme", "CONDUCTOR_ALLOW_REMOTE=true"}, want: map[string]any{"OrgName": "acme", "AllowRemote": true}},
		{name: "flags override environment", env: []string{"CONDUCTOR_ORG_NAME=acme", "CONDUCTOR_ALLOW_REMOTE=true"}, args: []string{"--org=team", "--allow-remote=false"}, want: map[string]any{"OrgName": "team", "AllowRemote": false}},
		{name: "remote flag", args: []string{"--org=team", "--allow-remote"}, want: map[string]any{"OrgName": "team", "AllowRemote": true}},
		{name: "invalid remote boolean", env: []string{"CONDUCTOR_ALLOW_REMOTE=perhaps"}, invalid: true},
		{name: "empty org", args: []string{"--org="}, invalid: true},
		{name: "org path", args: []string{"--org=../team"}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestConfigurationProcess$", "--"}, tc.args...)...)
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "CONDUCTOR_") && !strings.HasPrefix(value, "MAESTRO_CONFIG_TEST=") {
					cmd.Env = append(cmd.Env, value)
				}
			}
			cmd.Env = append(cmd.Env, "MAESTRO_CONFIG_TEST=1")
			cmd.Env = append(cmd.Env, tc.env...)
			output, err := cmd.CombinedOutput()
			if tc.invalid {
				if err == nil {
					t.Fatalf("invalid configuration accepted: %s", output)
				}
				return
			}
			if err != nil {
				t.Fatalf("configuration failed: %v: %s", err, output)
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
