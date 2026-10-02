package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/PadenZach/maestro/internal/hub"
	"github.com/PadenZach/maestro/internal/protocol"
)

type aggregateControl struct {
	Name, Label, Kind, Value string
	WireName                 string
	Options                  []aggregateOption
}

type aggregateOption struct {
	Value    string
	Selected bool
}

type aggregateSection struct {
	Label    string
	Controls []aggregateControl
}

type aggregateGroupValue struct{ Label, Value string }

type aggregateRow struct {
	Group []aggregateGroupValue
	Cells []string
}

type aggregatesData struct {
	App, Kind, Title, Error string
	Sections                []aggregateSection
	Columns                 []string
	Rows                    []aggregateRow
}

func (c aggregateControl) DateInput() string {
	stamp, err := time.Parse(time.RFC3339Nano, c.Value)
	if err != nil {
		return c.Value
	}
	return stamp.UTC().Format("2006-01-02T15:04:05.000")
}

func (d aggregatesData) AdvancedOpen() bool {
	for _, section := range d.AdvancedSections() {
		for _, control := range section.Controls {
			if control.Value != "" {
				return true
			}
		}
	}
	return false
}

func (d aggregatesData) Basics() []aggregateControl {
	return []aggregateControl{d.Sections[0].Controls[0], d.Sections[1].Controls[0]}
}

func (d aggregatesData) AdvancedSections() []aggregateSection {
	out := make([]aggregateSection, len(d.Sections))
	copy(out, d.Sections)
	out[0].Controls = out[0].Controls[1:]
	out[1].Controls = out[1].Controls[1:]
	return out
}

func (d aggregatesData) URL() string { return applicationPath(d.App) + "/aggregates/" + d.Kind }

// Controls retain their public URL names and map directly to SDK request fields.
func aggregateSections(kind string) []aggregateSection {
	groups := []aggregateControl{{Name: "groupByStatus", WireName: "group_by_status", Label: "Status", Kind: "boolean"}}
	measures := []aggregateControl{{Name: "selectCount", WireName: "select_count", Label: "Count", Kind: "boolean"}}
	filters := []aggregateControl{{Name: "status", Label: "Statuses", Kind: "statuses"}, {Name: "workflowIdPrefix", WireName: "workflow_id_prefix", Label: "Workflow ID prefixes", Kind: "list"}, {Name: "completedAfter", WireName: "completed_after", Label: "Completed after", Kind: "date"}, {Name: "completedBefore", WireName: "completed_before", Label: "Completed before", Kind: "date"}}
	if kind == "workflows" {
		groups = append(groups, []aggregateControl{
			{Name: "groupByWorkflowName", WireName: "group_by_name", Label: "Workflow name", Kind: "boolean"},
			{Name: "groupByQueueName", WireName: "group_by_queue_name", Label: "Queue", Kind: "boolean"},
			{Name: "groupByExecutorId", WireName: "group_by_executor_id", Label: "Executor", Kind: "boolean"},
			{Name: "groupByAppVersion", WireName: "group_by_application_version", Label: "Application version", Kind: "boolean"},
			{Name: "groupByApplicationName", WireName: "group_by_application_name", Label: "Application name", Kind: "boolean"},
		}...)
		measures = append(measures, []aggregateControl{
			{Name: "selectMinCreatedAt", WireName: "select_min_created_at", Label: "Earliest created", Kind: "boolean"},
			{Name: "selectMaxQueueWaitMs", WireName: "select_max_queue_wait_ms", Label: "Max queue wait", Kind: "boolean"},
			{Name: "selectMaxTotalLatencyMs", WireName: "select_max_total_latency_ms", Label: "Max total latency", Kind: "boolean"},
		}...)
		filters = append(filters, []aggregateControl{
			{Name: "workflowName", WireName: "name", Label: "Workflow names", Kind: "list"},
			{Name: "workflowIds", WireName: "workflow_ids", Label: "Workflow IDs", Kind: "list"},
			{Name: "queueName", WireName: "queue_name", Label: "Queue names", Kind: "list"},
			{Name: "appVersion", WireName: "app_version", Label: "Application versions", Kind: "list"},
			{Name: "executorId", WireName: "executor_id", Label: "Executor IDs", Kind: "list"},
			{Name: "user", Label: "Users", Kind: "list"},
			{Name: "scheduleName", WireName: "schedule_name", Label: "Schedule names", Kind: "list"},
			{Name: "forkedFrom", WireName: "forked_from", Label: "Forked from workflow IDs", Kind: "list"},
			{Name: "parentWorkflowId", WireName: "parent_workflow_id", Label: "Parent workflow IDs", Kind: "list"},
			{Name: "startTime", WireName: "start_time", Label: "Created after", Kind: "date"},
			{Name: "endTime", WireName: "end_time", Label: "Created before", Kind: "date"},
			{Name: "dequeuedAfter", WireName: "dequeued_after", Label: "Dequeued after", Kind: "date"},
			{Name: "dequeuedBefore", WireName: "dequeued_before", Label: "Dequeued before", Kind: "date"},
			{Name: "wasForkedFrom", WireName: "was_forked_from", Label: "Has been forked", Kind: "tristate"},
			{Name: "hasParent", WireName: "has_parent", Label: "Has parent", Kind: "tristate"},
			{Name: "attributes", Label: "Attributes (JSON object)", Kind: "object"},
		}...)
	} else {
		groups = append(groups, aggregateControl{Name: "groupByFunctionName", WireName: "group_by_function_name", Label: "Step name", Kind: "boolean"})
		measures = append(measures, aggregateControl{Name: "selectMaxDurationMs", WireName: "select_max_duration_ms", Label: "Max duration", Kind: "boolean"})
		filters = append(filters, aggregateControl{Name: "stepName", WireName: "function_name", Label: "Step names", Kind: "list"})
	}
	groups = append(groups, aggregateControl{Name: "timeBucketSeconds", WireName: "time_bucket_size_ms", Label: "Time bucket (seconds)", Kind: "seconds"})
	statuses := []string{"SUCCESS", "ERROR"}
	if kind == "workflows" {
		statuses = []string{"SUCCESS", "ERROR", "MAX_RECOVERY_ATTEMPTS_EXCEEDED", "PENDING", "ENQUEUED", "DELAYED", "CANCELLED"}
	}
	for _, status := range statuses {
		filters[0].Options = append(filters[0].Options, aggregateOption{Value: status})
	}
	return []aggregateSection{{"Group by", groups}, {"Measures", measures}, {"Filters", filters}}
}

// Console presentation converts seconds to milliseconds exactly; JSON APIs
// retain their original wire units and validation.
func aggregateSecondsMS(value string) (int64, error) {
	parts := strings.Split(value, ".")
	if len(parts) == 2 && parts[0] == "" {
		parts[0] = "0"
	}
	if len(parts) > 2 || parts[0] == "" {
		return 0, errors.New("timeBucketSeconds must be positive seconds with millisecond precision")
	}
	for _, part := range parts {
		if part == "" {
			return 0, errors.New("timeBucketSeconds must be positive seconds with millisecond precision")
		}
		for _, digit := range part {
			if digit < '0' || digit > '9' {
				return 0, errors.New("timeBucketSeconds must be positive seconds with millisecond precision")
			}
		}
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > 3 {
		if strings.Trim(fraction[3:], "0") != "" {
			return 0, errors.New("timeBucketSeconds must have millisecond precision")
		}
		fraction = fraction[:3]
	}
	fraction += strings.Repeat("0", 3-len(fraction))
	millis, _ := strconv.ParseInt(fraction, 10, 64)
	seconds, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || seconds > (math.MaxInt64-millis)/1000 {
		return 0, errors.New("timeBucketSeconds is out of range")
	}
	result := seconds*1000 + millis
	if result < 1 {
		return 0, errors.New("timeBucketSeconds must be at least 0.001 seconds")
	}
	return result, nil
}

func aggregateSecondsText(ms int64) string {
	whole := strconv.FormatInt(ms/1000, 10)
	fraction := strings.TrimRight(fmt.Sprintf("%03d", ms%1000), "0")
	if fraction != "" {
		whole += "." + fraction
	}
	return whole
}

func parseAggregateControls(r *http.Request, sections []aggregateSection) (map[string]any, error) {
	query, err := parseUTF8Query(r.URL.RawQuery)
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
	if _, seconds := query["timeBucketSeconds"]; seconds {
		if _, millis := query["timeBucketSizeMs"]; millis {
			return nil, errors.New("choose timeBucketSeconds or timeBucketSizeMs, not both")
		}
	}
	fields := map[string]any{}
	for name, values := range query {
		if name == "timeBucketSizeMs" {
			if len(values) != 1 {
				return nil, fmt.Errorf("duplicate aggregate filter %q", name)
			}
			ms, err := strconv.ParseInt(values[0], 10, 64)
			if err != nil || ms < 1 {
				return nil, errors.New("timeBucketSizeMs must be a positive int64")
			}
			fields["time_bucket_size_ms"] = ms
			controls["timeBucketSeconds"].Value = aggregateSecondsText(ms)
			continue
		}
		control, ok := controls[name]
		if !ok {
			return nil, fmt.Errorf("unsupported aggregate filter %q", name)
		}
		if len(values) != 1 && control.Kind != "statuses" {
			return nil, fmt.Errorf("duplicate aggregate filter %q", name)
		}
		value := values[0]
		control.Value = value
		if value == "" && len(values) == 1 {
			continue
		}
		wireName := control.WireName
		if wireName == "" {
			wireName = name
		}
		switch control.Kind {
		case "boolean", "tristate":
			var parsed bool
			if bytes.Equal(bytes.TrimSpace([]byte(value)), []byte("null")) || json.Unmarshal([]byte(value), &parsed) != nil {
				return nil, fmt.Errorf("%s must be boolean", name)
			}
			fields[wireName] = parsed
		case "object":
			decoder := json.NewDecoder(strings.NewReader(value))
			decoder.UseNumber()
			var parsed map[string]any
			if err := decoder.Decode(&parsed); err != nil || parsed == nil {
				return nil, errors.New("attributes must be an object")
			}
			var trailing any
			if err := decoder.Decode(&trailing); err != io.EOF {
				return nil, errors.New("attributes must be an object")
			}
			fields[wireName] = parsed
		case "seconds":
			ms, err := aggregateSecondsMS(value)
			if err != nil {
				return nil, err
			}
			fields[wireName] = ms
		case "statuses":
			statuses := []string{}
			for _, value := range values {
				statuses = append(statuses, strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n")...)
			}
			fields[wireName] = statuses
			for _, status := range statuses {
				found := false
				for i := range control.Options {
					if control.Options[i].Value == status {
						control.Options[i].Selected = true
						found = true
					}
				}
				if !found {
					control.Options = append(control.Options, aggregateOption{Value: status, Selected: true})
				}
			}
		case "date":
			date := value
			if _, err := time.Parse(time.RFC3339Nano, date); err != nil {
				var stamp time.Time
				for _, format := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02T15:04"} {
					stamp, err = time.Parse(format, value)
					if err == nil {
						break
					}
				}
				if err != nil {
					return nil, fmt.Errorf("%s must be a UTC date and time", name)
				}
				date = stamp.UTC().Format(time.RFC3339Nano)
			}
			fields[wireName] = date
		case "list":
			fields[wireName] = strings.Split(strings.ReplaceAll(value, "\r\n", "\n"), "\n")
		default:
			fields[wireName] = value
		}
	}
	return fields, nil
}

func (s *handler) handleAggregates(w http.ResponseWriter, r *http.Request) {
	app, kind := r.PathValue("app"), r.PathValue("kind")
	if !s.cfg.EnableAggregates {
		s.renderStatusError(w, http.StatusNotFound, []crumb{{Label: "Home", Href: "/"}, {Label: app, Href: applicationPath(app)}}, errors.New("advanced aggregate queries are disabled"))
		return
	}
	if kind != "workflows" && kind != "steps" {
		http.NotFound(w, r)
		return
	}
	data := aggregatesData{App: app, Kind: kind, Title: "Workflow aggregates", Sections: aggregateSections(kind)}
	if kind == "steps" {
		data.Title = "Step aggregates"
	}
	fields, err := parseAggregateControls(r, data.Sections)
	data.Columns = nil
	for _, index := range aggregateSelectedMeasures(data.Sections) {
		data.Columns = append(data.Columns, data.Sections[1].Controls[index].Label)
	}
	request := protocol.NewRequest(protocol.MsgGetWorkflowAggregates)
	if kind == "steps" {
		request = protocol.NewRequest(protocol.MsgGetStepAggregates)
	}
	request["body"] = fields
	if err == nil && len(aggregateSelectedMeasures(data.Sections)) == 0 {
		err = errors.New("select at least one measure")
	}
	if err == nil {
		grouped := false
		for _, control := range data.Sections[0].Controls {
			if control.Value == "true" || control.Kind == "seconds" && control.Value != "" {
				grouped = true
			}
		}
		if !grouped {
			err = errors.New("select a grouping or time bucket")
		}
	}
	status := http.StatusOK
	if err != nil {
		status = http.StatusBadRequest
	} else {
		var rows []aggregateRow
		rows, err = s.readAggregateRows(r.Context(), app, kind, request)
		if err != nil {
			status = http.StatusBadGateway
			if errors.Is(err, hub.ErrAppUnavailable) {
				status = http.StatusServiceUnavailable
			}
		} else {
			for _, row := range rows {
				selected := []string{}
				for _, index := range aggregateSelectedMeasures(data.Sections) {
					selected = append(selected, row.Cells[index])
				}
				row.Cells = selected
				data.Rows = append(data.Rows, row)
			}
		}
	}
	if err != nil {
		data.Error = htmlErrorText(err)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if r.Header.Get("HX-Request") == "true" {
		s.web.partial(w, "aggregate_view", data)
		return
	}
	s.web.page(w, "aggregates", page{Title: app + " · " + data.Title, AppsAvailable: s.appsAvailable(), Status: s.statusForPage(status >= 500), Crumbs: []crumb{{Label: "Home", Href: "/"}, {Label: app, Href: applicationPath(app)}, {Label: data.Title}}, Data: data})
}

func aggregateSelectedMeasures(sections []aggregateSection) []int {
	indices := []int{}
	for i, control := range sections[1].Controls {
		if control.Value == "true" {
			indices = append(indices, i)
		}
	}
	return indices
}

func aggregateCell[T any](value *T) string {
	if value == nil {
		return "Unknown"
	}
	return fmt.Sprint(*value)
}

func aggregateDate(value *string) string {
	if value == nil {
		return "Unknown"
	}
	stamp, err := time.Parse(time.RFC3339Nano, *value)
	if err != nil {
		return "Unknown"
	}
	return stamp.UTC().Format("Jan 02, 2006 15:04:05.000 UTC")
}

func aggregateDuration(value *int64) string {
	if value == nil {
		return "Unknown"
	}
	sign := ""
	remainder := *value % 1000
	if *value < 0 {
		sign = "-"
		remainder = -remainder
	}
	whole := *value / 1000
	if whole < 0 {
		whole = -whole
	}
	text := strconv.FormatInt(whole, 10)
	fraction := strings.TrimRight(fmt.Sprintf("%03d", remainder), "0")
	if fraction != "" {
		text += "." + fraction
	}
	return sign + text + " s"
}

func aggregateConsoleRow(group map[string]*string, cells ...string) aggregateRow {
	labels := map[string]string{"status": "Status", "name": "Workflow", "workflow_name": "Workflow", "queue_name": "Queue", "executor_id": "Executor", "application_version": "Application version", "application_name": "Application", "function_name": "Step", "time_bucket": "Bucket (UTC)"}
	row := aggregateRow{Cells: cells}
	keys := make([]string, 0, len(group))
	for key := range group {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		label := labels[key]
		if label == "" {
			label = strings.ReplaceAll(key, "_", " ")
		}
		value := "Unknown"
		if group[key] != nil {
			value = *group[key]
			if value == "" {
				value = "Empty"
			}
			if key == "time_bucket" {
				if epoch, err := strconv.ParseInt(value, 10, 64); err == nil {
					stamp := time.UnixMilli(epoch).UTC().Format(time.RFC3339Nano)
					value = aggregateDate(&stamp)
				}
			}
		}
		row.Group = append(row.Group, aggregateGroupValue{Label: label, Value: value})
	}
	if len(row.Group) == 0 {
		row.Group = []aggregateGroupValue{{Label: "Group", Value: "All"}}
	}
	return row
}

func (s *handler) readAggregateRows(ctx context.Context, app, kind string, request protocol.Request) ([]aggregateRow, error) {
	raw, err := s.hub.Request(ctx, app, request)
	if err != nil {
		return nil, err
	}
	records, err := protocol.DecodeAggregatePayload(raw)
	if err != nil {
		return nil, err
	}
	rows := make([]aggregateRow, 0, len(records))
	for _, raw := range records {
		if kind == "workflows" {
			row, err := protocol.DecodeWorkflowAggregate(raw)
			if err != nil {
				return nil, err
			}
			var created *string
			if row.MinCreatedAt != nil {
				stamp := time.UnixMilli(*row.MinCreatedAt).UTC().Format(time.RFC3339Nano)
				created = &stamp
			}
			rows = append(rows, aggregateConsoleRow(row.Group, aggregateCell(row.Count), aggregateDate(created), aggregateDuration(row.MaxQueueWaitMS), aggregateDuration(row.MaxTotalLatencyMS)))
		} else {
			row, err := protocol.DecodeStepAggregate(raw)
			if err != nil {
				return nil, err
			}
			rows = append(rows, aggregateConsoleRow(row.Group, aggregateCell(row.Count), aggregateDuration(row.MaxDurationMS)))
		}
	}
	return rows, nil
}
