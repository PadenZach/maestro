package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/zpaden/maestro/internal/hub"
	"github.com/zpaden/maestro/internal/protocol"
)

type aggregateControl struct {
	Name, Label, Kind, Value string
}

type aggregateSection struct {
	Label    string
	Controls []aggregateControl
}

func (section aggregateSection) Expanded() bool {
	if section.Label != "Filters" {
		return true
	}
	for _, control := range section.Controls {
		if control.Value != "" {
			return true
		}
	}
	return false
}

type aggregateRow struct {
	Group string
	Cells []string
}

type aggregatesData struct {
	App, Kind, Title, Error string
	Sections                []aggregateSection
	Columns                 []string
	Rows                    []aggregateRow
}

func (d aggregatesData) URL() string { return applicationPath(d.App) + "/aggregates/" + d.Kind }

// Labels and input kinds belong to the Console; the shared HTTP parsers remain
// responsible for field types and the SDK's exact nullable/omitted wire values.
func aggregateSections(kind string) []aggregateSection {
	groups := []aggregateControl{{Name: "groupByStatus", Label: "Status", Kind: "boolean"}}
	measures := []aggregateControl{{Name: "selectCount", Label: "Count", Kind: "boolean"}}
	filters := []aggregateControl{{Name: "status", Label: "Statuses", Kind: "list"}, {Name: "workflowIdPrefix", Label: "Workflow ID prefixes", Kind: "list"}, {Name: "completedAfter", Label: "Completed after", Kind: "date"}, {Name: "completedBefore", Label: "Completed before", Kind: "date"}}
	if kind == "workflows" {
		groups = append(groups, []aggregateControl{
			{Name: "groupByWorkflowName", Label: "Workflow name", Kind: "boolean"},
			{Name: "groupByQueueName", Label: "Queue", Kind: "boolean"},
			{Name: "groupByExecutorId", Label: "Executor", Kind: "boolean"},
			{Name: "groupByAppVersion", Label: "Application version", Kind: "boolean"},
			{Name: "groupByApplicationName", Label: "Application name", Kind: "boolean"},
		}...)
		measures = append(measures, []aggregateControl{
			{Name: "selectMinCreatedAt", Label: "Earliest created", Kind: "boolean"},
			{Name: "selectMaxQueueWaitMs", Label: "Max queue wait", Kind: "boolean"},
			{Name: "selectMaxTotalLatencyMs", Label: "Max total latency", Kind: "boolean"},
		}...)
		filters = append(filters, []aggregateControl{
			{Name: "workflowName", Label: "Workflow names", Kind: "list"},
			{Name: "workflowIds", Label: "Workflow IDs", Kind: "list"},
			{Name: "queueName", Label: "Queue names", Kind: "list"},
			{Name: "appVersion", Label: "Application versions", Kind: "list"},
			{Name: "executorId", Label: "Executor IDs", Kind: "list"},
			{Name: "user", Label: "Users", Kind: "list"},
			{Name: "scheduleName", Label: "Schedule names", Kind: "list"},
			{Name: "forkedFrom", Label: "Forked from workflow IDs", Kind: "list"},
			{Name: "parentWorkflowId", Label: "Parent workflow IDs", Kind: "list"},
			{Name: "startTime", Label: "Created after", Kind: "date"},
			{Name: "endTime", Label: "Created before", Kind: "date"},
			{Name: "dequeuedAfter", Label: "Dequeued after", Kind: "date"},
			{Name: "dequeuedBefore", Label: "Dequeued before", Kind: "date"},
			{Name: "wasForkedFrom", Label: "Has been forked", Kind: "boolean"},
			{Name: "hasParent", Label: "Has parent", Kind: "boolean"},
			{Name: "attributes", Label: "Attributes (JSON object)", Kind: "object"},
		}...)
	} else {
		groups = append(groups, aggregateControl{Name: "groupByFunctionName", Label: "Step name", Kind: "boolean"})
		measures = append(measures, aggregateControl{Name: "selectMaxDurationMs", Label: "Max duration", Kind: "boolean"})
		filters = append(filters, aggregateControl{Name: "stepName", Label: "Step names", Kind: "list"})
	}
	groups = append(groups, aggregateControl{Name: "timeBucketSizeMs", Label: "Time bucket size (ms)", Kind: "integer"})
	return []aggregateSection{{"Group by", groups}, {"Measures", measures}, {"Filters", filters}}
}

func parseAggregateControls(r *http.Request, sections []aggregateSection) (map[string]json.RawMessage, error) {
	query, err := localV2WorkflowQueryValues(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	controls := map[string]*aggregateControl{}
	for i := range sections {
		for j := range sections[i].Controls {
			control := &sections[i].Controls[j]
			controls[control.Name] = control
		}
	}
	if len(query) == 0 {
		query.Set("groupByStatus", "true")
		query.Set("selectCount", "true")
	}
	fields := map[string]json.RawMessage{}
	for name, values := range query {
		control, ok := controls[name]
		if !ok {
			return nil, fmt.Errorf("unsupported aggregate filter %q", name)
		}
		if len(values) != 1 {
			return nil, fmt.Errorf("duplicate aggregate filter %q", name)
		}
		value := values[0]
		control.Value = value
		if value == "" {
			continue
		}
		switch control.Kind {
		case "boolean", "integer", "object":
			fields[name] = json.RawMessage(value)
		case "list":
			// Textareas submit CRLF; preserve each exact value, including spaces.
			fields[name], _ = json.Marshal(strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n"))
		default:
			fields[name], _ = json.Marshal(value)
		}
	}
	return fields, nil
}

func (s *Server) handleAggregates(w http.ResponseWriter, r *http.Request) {
	app, kind := r.PathValue("app"), r.PathValue("kind")
	if kind != "workflows" && kind != "steps" {
		http.NotFound(w, r)
		return
	}
	data := aggregatesData{App: app, Kind: kind, Title: "Workflow aggregates", Sections: aggregateSections(kind), Columns: []string{"Count", "Earliest created", "Max queue wait (ms)", "Max total latency (ms)"}}
	if kind == "steps" {
		data.Title = "Step aggregates"
		data.Columns = []string{"Count", "Max duration (ms)"}
	}
	fields, err := parseAggregateControls(r, data.Sections)
	var request protocol.Request
	mapper := localV2WorkflowAggregateRecord
	if err == nil {
		if kind == "workflows" {
			var body protocol.WorkflowAggregatesBody
			body, err = localV2WorkflowAggregateBody(fields)
			request = protocol.GetWorkflowAggregatesRequest(body)
		} else {
			var body protocol.StepAggregatesBody
			body, err = localV2StepAggregateBody(fields)
			request = protocol.GetStepAggregatesRequest(body)
			mapper = localV2StepAggregateRecord
		}
	}
	status := http.StatusOK
	if err != nil {
		status = http.StatusBadRequest
	} else {
		var records []aggregateResult
		records, err = s.readAggregates(r.Context(), app, request, mapper)
		if err != nil {
			status = http.StatusBadGateway
			if errors.Is(err, hub.ErrAppUnavailable) {
				status = http.StatusServiceUnavailable
			}
		} else {
			for _, record := range records {
				data.Rows = append(data.Rows, record.consoleRow())
			}
		}
	}
	if err != nil {
		data.Error = htmlErrorText(err)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if r.Header.Get("HX-Request") == "true" {
		s.web.Partial(w, "aggregate_view", data)
		return
	}
	s.web.Page(w, "aggregates", page{Title: app + " · " + data.Title, AppsAvailable: s.appsAvailable(), Status: s.statusForPage(status >= 500), Crumbs: []crumb{{Label: "Home", Href: "/"}, {Label: app, Href: applicationPath(app)}, {Label: data.Title}}, Data: data})
}

func aggregateCell[T any](value *T) string {
	if value == nil {
		return "Not selected"
	}
	return fmt.Sprint(*value)
}

func aggregateConsoleRow(group map[string]*string, cells ...string) aggregateRow {
	var text strings.Builder
	encoder := json.NewEncoder(&text)
	encoder.SetEscapeHTML(false) // html/template escapes the visible text.
	_ = encoder.Encode(group)
	return aggregateRow{Group: strings.TrimSuffix(text.String(), "\n"), Cells: cells}
}

func (a *WorkflowAggregate) consoleRow() aggregateRow {
	return aggregateConsoleRow(a.Group, aggregateCell(a.Count), aggregateCell(a.MinCreatedAt), aggregateCell(a.MaxQueueWaitMS), aggregateCell(a.MaxTotalLatencyMS))
}

func (a *StepAggregate) consoleRow() aggregateRow {
	return aggregateConsoleRow(a.Group, aggregateCell(a.Count), aggregateCell(a.MaxDurationMS))
}
