package api

import (
	"errors"
	"net/http"
	"strings"
)

// Gate every user-composed aggregate query before parsing or executor dispatch.
// The overview uses its own fixed, bounded query builders and bypasses this gate.
func (s *Server) aggregateHandler(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.EnableAggregates {
			next(w, r)
			return
		}
		const message = "Advanced aggregate queries are disabled."
		if strings.HasPrefix(r.URL.Path, "/v2/") {
			localV2Problem(w, http.StatusNotFound, message)
			return
		}
		s.renderStatusError(w, http.StatusNotFound, []crumb{{Label: "Home", Href: "/"}, {Label: r.PathValue("app"), Href: applicationPath(r.PathValue("app"))}}, errors.New("advanced aggregate queries are disabled"))
	}
}
