package server_test

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/PadenZach/maestro/internal/config"
	"github.com/PadenZach/maestro/internal/hub"
	"github.com/PadenZach/maestro/internal/server"
)

func TestConductorReadsAreAlwaysAvailable(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := server.New(config.Config{}, hub.New(log, time.Second), log)
	r := httptest.NewRequest("GET", "/v2/orgs/local/apps/fixture-app/queues", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("Conductor route must exist without an enable flag: status=%d want=503", w.Code)
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/openapi.json", nil))
	if !strings.Contains(w.Body.String(), `"listQueues"`) {
		t.Fatal("default API documentation omits Conductor reads")
	}
}

func TestAPIConfiguredOrganizationAndRemoteAccess(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := server.New(config.Config{OrgName: "acme"}, hub.New(log, time.Second), log)
	for _, tc := range []struct {
		org, remote string
		want        int
	}{
		{"acme", "127.0.0.1:1234", 503},
		{"local", "127.0.0.1:1234", 404},
		{"acme", "198.51.100.2:1234", 503},
		{"other", "198.51.100.2:1234", 404},
	} {
		r := httptest.NewRequest("GET", "/v2/orgs/"+tc.org+"/apps/fixture-app/queues", nil)
		r.RemoteAddr = tc.remote
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Errorf("org=%s remote=%s: status=%d want=%d body=%s", tc.org, tc.remote, w.Code, tc.want, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/openapi.json", nil))
	if !strings.Contains(w.Body.String(), "acme") {
		t.Error("API description omits configured organization")
	}
}
