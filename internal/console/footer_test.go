package console

import (
	"html"
	"net/http/httptest"
	"runtime/debug"
	"strings"
	"testing"
)

// The owner requested a global Maestro footer with its own build version,
// MIT attribution and an explicit DBOS non-affiliation notice.
func TestPageHasMaestroFooter(t *testing.T) {
	r, err := newRenderer()
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range []string{"apps", "error"} {
		t.Run(page, func(t *testing.T) {
			w := httptest.NewRecorder()
			r.page(w, page, map[string]any{"Title": "Test", "Status": map[string]string{"Label": "Online", "State": "online"}})
			body := html.UnescapeString(w.Body.String())
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
		name, version, revision, modified string
		info                              *debug.BuildInfo
		want                              string
	}{
		{"container release", "1.2.3", "abcdef1234567890", "false", nil, "1.2.3+abcdef123456"},
		{"container dirty", "1.2.3", "abcdef1234567890", "true", nil, "1.2.3+abcdef123456.dirty"},
		{"revision", "0.1.0", "", "", &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abcdef1234567890"}}}, "0.1.0+abcdef123456"},
		{"modified revision", "0.1.0", "", "", &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abcdef1234567890"}, {Key: "vcs.modified", Value: "true"}}}, "0.1.0+abcdef123456.dirty"},
		{"explicit metadata wins", "0.1.0", "123456abcdef", "false", &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "other"}, {Key: "vcs.modified", Value: "true"}}}, "0.1.0+123456abcdef"},
		{"short revision", "0.1.0", "abc", "false", nil, "0.1.0+abc"},
		{"prerelease", "0.2.0-rc.1", "abcdef123456", "false", nil, "0.2.0-rc.1+abcdef123456"},
		{"existing metadata", "0.2.0+custom", "abcdef123456", "false", nil, "0.2.0+custom.abcdef123456"},
		{"unavailable", "0.1.0", "", "", nil, "0.1.0+unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildVersion(tc.version, tc.revision, tc.modified, tc.info); got != tc.want {
				t.Errorf("build version = %q, want %q", got, tc.want)
			}
		})
	}
}
