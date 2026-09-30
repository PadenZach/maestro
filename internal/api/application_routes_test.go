package api_test

import (
	"strings"
	"testing"
)

func TestApplicationLandingRoutesExposeExistingViews(t *testing.T) {
	ts, _ := newTestServer(t)
	for _, path := range []string{"/apps/maestro-poc", "/apps/maestro-poc/"} {
		t.Run(path, func(t *testing.T) {
			code, body := getBody(t, ts.URL+path)
			if code != 200 {
				t.Fatalf("application landing status = %d, want 200", code)
			}
			for _, href := range []string{"/apps/maestro-poc/workflows", "/apps/maestro-poc/queues"} {
				if !strings.Contains(body, `href="`+href+`"`) {
					t.Errorf("application landing missing link %s", href)
				}
			}
			if !strings.Contains(body, `<div class="brand">maestro</div>`) {
				t.Error("application landing missing maestro branding")
			}
		})
	}
	code, _ := postBody(t, ts.URL+"/apps/maestro-poc/")
	if code != 405 {
		t.Fatalf("application landing POST status = %d, want 405", code)
	}
}
