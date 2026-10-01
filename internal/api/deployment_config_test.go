package api_test

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zpaden/maestro/internal/api"
	"github.com/zpaden/maestro/internal/config"
	"github.com/zpaden/maestro/internal/hub"
)

func TestConductorReadsAreAlwaysAvailable(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := api.New(config.Config{}, hub.New(log, time.Second), log)
	r := httptest.NewRequest("GET", "/v2/orgs/local/apps/fixture-app/queues", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("Conductor route must exist without an enable flag: status=%d want=503", w.Code)
	}
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/openapi.json", nil))
	if !strings.Contains(w.Body.String(), `"listQueues"`) {
		t.Fatal("default API documentation omits Conductor reads")
	}
}

func TestHTTPV2ConfiguredOrganizationAndRemoteAccess(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, allowRemote := range []bool{false, true} {
		cfg := config.Config{OrgName: "acme", AllowRemote: allowRemote}
		s := api.New(cfg, hub.New(log, time.Second), log)
		for _, tc := range []struct {
			org, remote string
			want        int
		}{
			{"acme", "127.0.0.1:1234", 503},
			{"local", "127.0.0.1:1234", 404},
			{"acme", "198.51.100.2:1234", map[bool]int{true: 503, false: 403}[allowRemote]},
			{"other", "198.51.100.2:1234", map[bool]int{true: 404, false: 403}[allowRemote]},
		} {
			r := httptest.NewRequest("GET", "/v2/orgs/"+tc.org+"/apps/fixture-app/queues", nil)
			r.RemoteAddr = tc.remote
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Errorf("remote override=%t org=%s remote=%s: status=%d want=%d body=%s", allowRemote, tc.org, tc.remote, w.Code, tc.want, w.Body.String())
			}
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/openapi.json", nil))
		if !strings.Contains(w.Body.String(), "acme") {
			t.Error("API description omits configured organization")
		}
		if allowRemote && strings.Contains(w.Body.String(), "loopback-only test adapter") {
			t.Error("remote API description still claims loopback only")
		}
	}
}
