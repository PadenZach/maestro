package api_test

import (
	"html"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/zpaden/maestro/internal/protocol"
)

// Console navigation must retain the decoded application identity even on error
// pages. This exercises the application routes in docs/CONTRACTS.md rather than
// checking a URL builder in isolation.
func TestConsoleBreadcrumbsPreserveApplicationName(t *testing.T) {
	ts, _ := newTestServer(t)
	app := "team/東京?#&"
	appPath := "/apps/" + url.PathEscape(app)
	for _, suffix := range []string{"/workflows", "/workflows/missing", "/queues"} {
		t.Run(suffix, func(t *testing.T) {
			code, body := getBody(t, ts.URL+appPath+suffix)
			if code != 503 {
				t.Fatalf("offline application status = %d, want 503", code)
			}
			links := regexp.MustCompile(`href="([^"]*)"`).FindAllStringSubmatch(body, -1)
			found := false
			for _, link := range links {
				href := html.UnescapeString(link[1])
				if href != appPath {
					continue
				}
				found = true
				code, followed := getBody(t, ts.URL+href)
				if code != 200 || !strings.Contains(followed, "No connected executors for this application") {
					t.Fatalf("breadcrumb lost application identity: %d", code)
				}
			}
			if !found {
				t.Fatalf("missing path-escaped application breadcrumb %q", appPath)
			}
		})
	}
}

func TestConsoleWorkflowLinksPreserveIdentifiers(t *testing.T) {
	ts, h := newTestServer(t)
	app, id := "team/東京?x#y", "run/one?x#y&z"
	wf := sampleWorkflow(id, "PENDING")
	dialFake(t, ts, url.PathEscape(app), "testkey", "one", map[protocol.MessageType]respondFn{
		protocol.MsgListWorkflows: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{wf}}
		},
		protocol.MsgGetWorkflow: func(map[string]any) map[string]any { return map[string]any{"output": wf} },
		protocol.MsgListSteps: func(map[string]any) map[string]any {
			return map[string]any{"output": []any{}}
		},
	})
	waitFor(t, func() bool { return len(h.Executors()) == 1 })
	appPath := "/apps/" + url.PathEscape(app)
	workflowPath := appPath + "/workflows/" + url.PathEscape(id)
	for _, tc := range []struct {
		path       string
		attributes []string
	}{
		{appPath + "/workflows", []string{`href="` + workflowPath + `"`, `data-workflow-url="` + workflowPath + `"`, `hx-get="` + appPath + `/workflows/rows"`}},
		{workflowPath, []string{`hx-get="` + workflowPath + `/live"`, `hx-post="` + workflowPath + `/cancel"`, `hx-post="` + workflowPath + `/resume"`, `hx-get="` + workflowPath + `/blob?kind=input"`, `hx-get="` + workflowPath + `/blob?kind=output"`}},
	} {
		code, body := getBody(t, ts.URL+tc.path)
		if code != 200 {
			t.Fatalf("page status = %d", code)
		}
		decoded := html.UnescapeString(body)
		for _, attribute := range tc.attributes {
			if !strings.Contains(decoded, attribute) {
				t.Errorf("missing encoded control %s", attribute)
			}
		}
	}
}
