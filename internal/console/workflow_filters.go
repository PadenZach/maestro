package console

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"
)

// knownStatuses populates the workflow-list status filter.
var knownStatuses = []string{
	"ENQUEUED", "PENDING", "SUCCESS", "ERROR",
	"CANCELLED", "MAX_RECOVERY_ATTEMPTS_EXCEEDED", "DELAYED",
}

type filterState struct {
	Statuses        []string
	Name            string
	IDPrefix        string
	Queue           string
	StartTime       string
	EndTime         string
	IncludeChildren bool
	HasParent       *bool
}

func (f filterState) HasStatus(status string) bool {
	for _, value := range f.Statuses {
		if value == status {
			return true
		}
	}
	return false
}

func (f filterState) Active() bool {
	return len(f.Statuses) > 0 || f.Name != "" || f.IDPrefix != "" || f.Queue != "" || f.StartTime != "" || f.EndTime != ""
}

func (f filterState) StatusLabel() string {
	statuses := f.Statuses
	if len(statuses) == 0 {
		return "All statuses"
	}
	if len(statuses) == 1 {
		return statuses[0]
	}
	return fmt.Sprintf("%d statuses", len(statuses))
}

func parseFilter(r *http.Request) (filterState, error) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return filterState{}, fmt.Errorf("invalid workflow filters: %w", err)
	}
	for name, values := range q {
		switch name {
		case "status":
		case "name", "id_prefix", "queue", "start_time", "end_time", "offset", "children":
			if len(values) != 1 {
				return filterState{}, fmt.Errorf("duplicate workflow filter %q", name)
			}
		default:
			return filterState{}, fmt.Errorf("unsupported workflow filter %q", name)
		}
	}
	f := filterState{
		Name:      q.Get("name"),
		IDPrefix:  q.Get("id_prefix"),
		Queue:     q.Get("queue"),
		StartTime: q.Get("start_time"),
		EndTime:   q.Get("end_time"),
	}
	if values, exists := q["children"]; exists {
		if values[0] != "true" && values[0] != "false" {
			return filterState{}, errors.New("children must be true or false")
		}
		f.IncludeChildren = values[0] == "true"
	}
	if !f.IncludeChildren {
		no := false
		f.HasParent = &no
	}
	for _, status := range q["status"] {
		if status != "" {
			f.Statuses = append(f.Statuses, status)
		}
	}
	var start, end time.Time
	for name, raw := range map[string]string{"start_time": f.StartTime, "end_time": f.EndTime} {
		if raw == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return filterState{}, fmt.Errorf("%s must be an RFC3339 timestamp in UTC", name)
		}
		_, zone := parsed.Zone()
		if zone != 0 || parsed.Nanosecond()%int(time.Millisecond) != 0 {
			return filterState{}, fmt.Errorf("%s must use UTC with at most millisecond precision", name)
		}
		if name == "start_time" {
			start = parsed
		} else {
			end = parsed
		}
	}
	if !start.IsZero() && !end.IsZero() && start.After(end) {
		return filterState{}, errors.New("start_time must be at or before end_time")
	}
	if values, exists := q["offset"]; exists && values[0] != "" {
		offset, err := strconv.Atoi(values[0])
		if err != nil || offset < 0 {
			return filterState{}, errors.New("offset must be a nonnegative integer")
		}
	}
	return f, nil
}

func workflowFilterStatuses(f filterState) []string {
	statuses := append([]string(nil), knownStatuses...)
	for _, status := range f.Statuses {
		if !slices.Contains(statuses, status) {
			statuses = append(statuses, status)
		}
	}
	return statuses
}

func workflowFilterQuery(f filterState, offset int) string {
	v := url.Values{}
	for _, status := range f.Statuses {
		v.Add("status", status)
	}
	if f.Name != "" {
		v.Set("name", f.Name)
	}
	if f.IDPrefix != "" {
		v.Set("id_prefix", f.IDPrefix)
	}
	if f.Queue != "" {
		v.Set("queue", f.Queue)
	}
	if f.StartTime != "" {
		v.Set("start_time", f.StartTime)
	}
	if f.EndTime != "" {
		v.Set("end_time", f.EndTime)
	}
	if f.HasParent != nil || f.IncludeChildren {
		v.Set("children", strconv.FormatBool(f.IncludeChildren))
	}
	v.Set("offset", strconv.Itoa(offset))
	return v.Encode()
}
