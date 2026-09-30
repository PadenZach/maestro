package web

import (
	"net/http/httptest"
	"runtime/debug"
	"strings"
	"testing"
)

// The owner requested a global Maestro footer with its own build version,
// MIT attribution and an explicit DBOS non-affiliation notice.
func TestPageHasMaestroFooter(t *testing.T) {
	r, err := New()
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range []string{"apps", "error"} {
		t.Run(page, func(t *testing.T) {
			w := httptest.NewRecorder()
			r.Page(w, page, map[string]any{"Title": "Test", "Status": map[string]string{"Label": "Online", "State": "online"}})
			body := w.Body.String()
			for _, want := range []string{`<footer class="site-footer">`, `Maestro`, `href="https://opensource.org/license/mit"`, `MIT License`, `aria-label="Maestro version"`, maestroVersion(), `Not affiliated with DBOS, Inc. in any way.`, `/static/footer.css`} {
				if !strings.Contains(body, want) {
					t.Errorf("page lacks footer content %q", want)
				}
			}
			if strings.Index(body, "<footer") < strings.Index(body, "</main>") {
				t.Error("footer must follow page content")
			}
		})
	}
}

func TestMaestroBuildVersion(t *testing.T) {
	for _, tc := range []struct {
		name, override string
		info           *debug.BuildInfo
		want           string
	}{
		{"explicit release", "v1.2.3", nil, "v1.2.3"},
		{"module release", "", &debug.BuildInfo{Main: debug.Module{Version: "v2.0.1"}}, "v2.0.1"},
		{"revision", "", &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abcdef1234567890"}}}, "dev+abcdef123456"},
		{"modified revision", "", &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abcdef1234567890"}, {Key: "vcs.modified", Value: "true"}}}, "dev+abcdef123456.dirty"},
		{"short revision", "", &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}}}, "dev+abc"},
		{"unavailable", "", nil, "dev"},
		{"unversioned", "", &debug.BuildInfo{}, "dev"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildVersion(tc.override, tc.info); got != tc.want {
				t.Errorf("build version = %q, want %q", got, tc.want)
			}
		})
	}
}
