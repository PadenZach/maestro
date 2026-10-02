//go:build dbosctl

package api_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/protocol"
	"github.com/PadenZach/maestro/internal/testserver"
)

func runDBOSCTL(t *testing.T, url string, args ...string) (int, string) {
	t.Helper()
	binary := os.Getenv("DBOSCTL_BIN")
	if binary == "" {
		t.Fatal("explicit dbosctl gate requires DBOSCTL_BIN (missing binary is a failure)")
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := []string{"workflow"}
	base = append(base, args...)
	base = append(base, "--url", url, "--org", "local", "--app", "fixture-app", "-o", "json")
	cmd := exec.CommandContext(ctx, binary, base...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + dir, "XDG_CONFIG_HOME=" + filepath.Join(dir, "config"), "XDG_CACHE_HOME=" + filepath.Join(dir, "cache"), "NO_PROXY=*", "HTTP_PROXY=", "HTTPS_PROXY=", "ALL_PROXY="}
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("CLI timed out: %v", ctx.Err())
	}
	if err == nil {
		return 0, string(out)
	}
	if ex, ok := err.(*exec.ExitError); ok {
		return ex.ExitCode(), string(out)
	}
	t.Fatal(err)
	return -1, ""
}

func TestDBOSCTLReadMatrix(t *testing.T) {
	ts, fe := fixture(t)
	code, out := runDBOSCTL(t, ts.URL, "list", "--limit", "2")
	if code != 0 {
		t.Fatalf("list: %d %s", code, out)
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(out), &rows); err != nil || len(rows) != 2 || rows[0]["workflowId"] != "wf-1" {
		t.Fatalf("list JSON: %s %v", out, err)
	}
	code, out = runDBOSCTL(t, ts.URL, "list", "--limit", "0", "--status", "SUCCESS", "--name", "job", "--app-version", "v1", "--queue", "q", "--user", "alice", "--id", "wf-1", "--offset", "0", "--desc", "--queued", "--since", "2024-07-02T00:00:00Z", "--until", "2024-07-04T00:00:00Z")
	if code != 0 || strings.TrimSpace(out) != "[]" {
		t.Fatalf("CLI filters/zero: %d %s", code, out)
	}
	wire := fe.Body(t, protocol.MsgListWorkflows)
	if wire["start_time"] != "2024-07-02T00:00:00Z" || wire["end_time"] != "2024-07-04T00:00:00Z" || wire["sort_desc"] != true || wire["queues_only"] != true || wire["limit"] != float64(0) {
		t.Fatalf("CLI filters wire: %v", wire)
	}
	code, out = runDBOSCTL(t, ts.URL, "list")
	if code != 0 || json.Unmarshal([]byte(out), &rows) != nil || len(rows) != 30 || rows[29]["workflowId"] != "wf-30" {
		t.Fatalf("omitted limit: %d %s", code, out)
	}
	code, out = runDBOSCTL(t, ts.URL, "get", "wf-1")
	var wf map[string]any
	if code != 0 || json.Unmarshal([]byte(out), &wf) != nil || wf["workflowId"] != "wf-1" || wf["createdAt"] != "2024-07-03T09:46:40.123Z" {
		t.Fatalf("get: %d %s", code, out)
	}
	code, out = runDBOSCTL(t, ts.URL, "steps", "wf-1")
	var steps []map[string]any
	if code != 0 || json.Unmarshal([]byte(out), &steps) != nil || len(steps) != 1 || steps[0]["stepId"] != float64(0) {
		t.Fatalf("steps: %d %s", code, out)
	}
	for _, id := range []string{"missing"} {
		code, out = runDBOSCTL(t, ts.URL, "get", id)
		if code != 4 || !strings.Contains(out, "not found") {
			t.Fatalf("404: %d %s", code, out)
		}
	}
	code, out = runDBOSCTL(t, ts.URL, "list", "--limit", "-1")
	if code != 1 || !strings.Contains(out, "limit") {
		t.Fatalf("invalid limit: %d %s", code, out)
	}
	code, out = runDBOSCTL(t, ts.URL, "list", "--offset", "-1")
	if code != 1 || !strings.Contains(out, "offset") {
		t.Fatalf("invalid offset: %d %s", code, out)
	}
	code, out = runDBOSCTL(t, ts.URL, "list", "--since", "not-a-date")
	if code == 0 {
		t.Fatalf("bad date: %s", out)
	}
}

func TestDBOSCTLUnavailableAndRefusal(t *testing.T) {
	ts, _ := testserver.New(t, config.Config{EnableAggregates: true})
	code, out := runDBOSCTL(t, ts.URL, "get", "wf-1")
	if code != 1 || !strings.Contains(out, "unavailable") {
		t.Fatalf("503: %d %s", code, out)
	}
	ts, h := testserver.New(t, config.Config{EnableAggregates: true})
	testserver.Connect(t, ts, "fixture-app", "testkey", "exec-1", map[protocol.MessageType]testserver.Responder{protocol.MsgGetWorkflow: func(map[string]any) map[string]any {
		return map[string]any{"error_message": "private executor refusal"}
	}})
	testserver.Wait(t, func() bool { return len(h.Executors()) == 1 })
	code, out = runDBOSCTL(t, ts.URL, "get", "wf-1")
	if code != 1 || !strings.Contains(out, "private executor refusal") {
		t.Fatalf("502: %d %s", code, out)
	}
}
