package console_test

import (
	"strings"
	"testing"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/testserver"
)

func TestApplicationLandingRoutesExposeExistingViews(t *testing.T) {
	ts, _ := testserver.New(t, config.Config{})
	for _, path := range []string{"/apps/maestro-poc", "/apps/maestro-poc/"} {
		t.Run(path, func(t *testing.T) {
			code, body := testserver.Get(t, ts.URL+path)
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
	code, _ := testserver.Post(t, ts.URL+"/apps/maestro-poc/")
	if code != 405 {
		t.Fatalf("application landing POST status = %d, want 405", code)
	}
}
