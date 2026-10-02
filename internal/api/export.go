package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"unicode/utf8"

	"github.com/PadenZach/maestro/internal/protocol"
)

type ExportWorkflowOutputBody struct {
	SerializedWorkflow string `json:"serializedWorkflow"`
}

type exportQuery struct {
	ExportChildren bool `json:"exportChildren,omitempty"`
}

func parseExportQuery(r *http.Request) (bool, error) {
	query, err := parseUTF8Query(r.URL.RawQuery)
	if err != nil {
		return false, errors.New("malformed query")
	}
	exportChildren := false
	for name, values := range query {
		if name != "exportChildren" {
			return false, fmt.Errorf("unsupported query %q", name)
		}
		if len(values) != 1 {
			return false, errors.New("duplicate query \"exportChildren\"")
		}
		if values[0] != "true" && values[0] != "false" {
			return false, errors.New("exportChildren must be boolean")
		}
		exportChildren = values[0] == "true"
	}
	// The SDK wire field is required. The optional HTTP boolean has the natural
	// false value when omitted; this does not enable recursive child export.
	return exportChildren, nil
}

func exportValue(raw []byte) (string, error) {
	fields, err := protocol.DecodeObject(raw, "export response")
	if err != nil {
		return "", err
	}
	var base protocol.BaseResponse
	if err := json.Unmarshal(raw, &base); err != nil {
		return "", fmt.Errorf("decode response: %w", err)
	}
	if err := base.Err(); err != nil {
		return "", err
	}
	return requiredString(fields, "serialized_workflow", "export response")
}

func (s *handler) exportWorkflow(w http.ResponseWriter, r *http.Request) {
	if !s.allowOrganization(w, r) {
		return
	}
	app, workflowID := r.PathValue("app"), r.PathValue("id")
	if err := validateApp(app); err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	if workflowID == "" || !utf8.ValidString(workflowID) {
		writeProblem(w, http.StatusBadRequest, "invalid workflow ID")
		return
	}
	exportChildren, err := parseExportQuery(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, err.Error())
		return
	}
	// Python export raises for an absent workflow, but reports it through the
	// generic executor error channel. The official resource route distinguishes
	// that case with the same raw, blob-free existence boundary as related reads.
	existenceRaw, err := s.hub.Request(r.Context(), app, protocol.GetWorkflowRequest(workflowID, false, false))
	if err != nil {
		writeFailure(w, err)
		return
	}
	exists, err := workflowExists(existenceRaw, workflowID)
	if err != nil {
		writeFailure(w, err)
		return
	}
	if !exists {
		writeProblem(w, http.StatusNotFound, "workflow not found")
		return
	}
	raw, err := s.hub.Request(r.Context(), app, protocol.ExportWorkflowRequest(workflowID, exportChildren))
	if err != nil {
		writeFailure(w, err)
		return
	}
	serialized, err := exportValue(raw)
	if err != nil {
		writeFailure(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ExportWorkflowOutputBody{SerializedWorkflow: serialized})
}
