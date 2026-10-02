package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/PadenZach/maestro/internal/hub"
)

type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Problem{Type: "about:blank", Title: http.StatusText(status), Status: status, Detail: detail})
}

func writeFailure(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, hub.ErrAppUnavailable) {
		status = http.StatusServiceUnavailable
	}
	writeProblem(w, status, err.Error())
}
