package api

import (
	"context"
	"encoding/json"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/zpaden/maestro/internal/config"
	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/protocol"
	"github.com/zpaden/maestro/internal/web"
)

func applicationViewServer(t *testing.T) (*Server, *hub.Hub) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New(log, time.Second)
	renderer, err := web.New()
	if err != nil {
		t.Fatal(err)
	}
	return &Server{
		cfg: config.Config{},
		hub: h,
		log: log,
		web: renderer,
	}, h
}

func renderApplication(t *testing.T, s *Server, app string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetPathValue("app", app)
	recorder := httptest.NewRecorder()
	s.handleApplication(recorder, req)
	return recorder
}

func applicationLink(t *testing.T, body, class string) string {
	t.Helper()
	re := regexp.MustCompile(`<a class="[^"]*` + regexp.QuoteMeta(class) + `[^"]*" href="([^"]+)"`)
	match := re.FindStringSubmatch(body)
	if match == nil {
		t.Fatalf("missing %s link: %s", class, body)
	}
	return html.UnescapeString(match[1])
}

func TestApplicationLandingRendersSafeNavigationAndOfflineState(t *testing.T) {
	const app = `team/alpha <unsafe>&?`
	s, _ := applicationViewServer(t)

	response := renderApplication(t, s, app)
	body := response.Body.String()
	if response.Code != http.StatusOK {
		t.Fatalf("application landing status = %d, want 200: %s", response.Code, body)
	}
	for _, want := range []string{
		`<title>team/alpha &lt;unsafe&gt;&amp;? · Application · maestro</title>`,
		`<div class="brand">maestro</div>`,
		`<h1 class="page-title">team/alpha &lt;unsafe&gt;&amp;?</h1>`,
		`No connected executors for this application.`,
		`class="status-dot status-red"`,
		`aria-label="Application status: no connections"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("application landing missing %q: %s", want, body)
		}
	}
	if strings.Contains(body, "<unsafe>") {
		t.Fatalf("application display name was not HTML escaped: %s", body)
	}

	const escapedAppPath = "/apps/team%2Falpha%20%3Cunsafe%3E&%3F"
	if got := applicationLink(t, body, "application-workflows-link"); got != escapedAppPath+"/workflows" {
		t.Errorf("workflows link = %q, want %q", got, escapedAppPath+"/workflows")
	}
	if got := applicationLink(t, body, "application-queues-link"); got != escapedAppPath+"/queues" {
		t.Errorf("queues link = %q, want %q", got, escapedAppPath+"/queues")
	}
}

func connectApplicationExecutor(t *testing.T, target, app, executorID string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(target, "http")+"/websocket/"+app+"/testkey", nil)
	if err != nil {
		t.Fatalf("dial executor for %q: %v", app, err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })

	_, request, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read executor_info for %q: %v", app, err)
	}
	envelope, err := protocol.DecodeEnvelope(request)
	if err != nil {
		t.Fatalf("decode executor_info for %q: %v", app, err)
	}
	language := "python"
	response, err := json.Marshal(protocol.ExecutorInfoResponse{
		Type:               protocol.MsgExecutorInfo,
		RequestID:          envelope.RequestID,
		ExecutorID:         executorID,
		ApplicationVersion: "v1",
		Language:           &language,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, response); err != nil {
		t.Fatalf("write executor_info for %q: %v", app, err)
	}
	return conn
}

func waitForApplicationExecutors(t *testing.T, h *hub.Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(h.Executors()) != want {
		if time.Now().After(deadline) {
			t.Fatalf("connected executors = %d, want %d", len(h.Executors()), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestApplicationLandingShowsOnlySelectedApplicationExecutors(t *testing.T) {
	s, h := applicationViewServer(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /websocket/{app_name}/{conductor_key}", s.handleWS)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	connectApplicationExecutor(t, ts.URL, "selected-app", "selected-executor")
	connectApplicationExecutor(t, ts.URL, "other-app", "other-executor")
	waitForApplicationExecutors(t, h, 2)

	response := renderApplication(t, s, "selected-app")
	body := response.Body.String()
	if response.Code != http.StatusOK {
		t.Fatalf("application landing status = %d, want 200: %s", response.Code, body)
	}
	for _, want := range []string{
		`class="badge ok">Available</span>`,
		`1 connected executor`,
		`class="status-dot status-green"`,
		`aria-label="Application status: connected and ready"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("connected application landing missing %q: %s", want, body)
		}
	}
	if strings.Contains(body, "2 connected executors") {
		t.Fatalf("application landing used executors from another application: %s", body)
	}
}

func TestHomeApplicationCardLinksToEscapedLandingPath(t *testing.T) {
	renderer, err := web.New()
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	renderer.Page(recorder, "apps", page{
		Title:  "Applications",
		Status: uiStatus{State: "green", Label: "Application status: connected and ready"},
		Data: appsData{Apps: []appSummary{{
			Name:      `team/alpha & beta`,
			Available: true,
		}}},
	})

	body := recorder.Body.String()
	if got, want := applicationLink(t, body, "app-name"), "/apps/team%2Falpha%20&%20beta"; got != want {
		t.Fatalf("home application card href = %q, want landing %q", got, want)
	}
	if strings.Contains(applicationLink(t, body, "app-name"), "/workflows") {
		t.Fatalf("home application card still skips the landing page: %s", body)
	}
}
