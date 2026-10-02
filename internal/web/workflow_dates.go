package web

import (
	"strconv"
	"time"
)

// UTCDateInput formats a concrete bound for a native datetime-local picker.
// Its hidden RFC3339 input remains authoritative until the operator edits it.
func UTCDateInput(raw string) string {
	date, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return ""
	}
	return date.UTC().Format("2006-01-02T15:04:05.000")
}

// WorkflowCreatedUTC accepts SDK epoch milliseconds and RFC3339 timestamps.
// Unknown values retain their original evidence rather than inventing a date.
func WorkflowCreatedUTC(raw *string) string {
	if raw == nil || *raw == "" {
		return "—"
	}
	date, err := time.Parse(time.RFC3339Nano, *raw)
	if err != nil {
		milliseconds, epochErr := strconv.ParseInt(*raw, 10, 64)
		if epochErr != nil {
			return *raw
		}
		date = time.UnixMilli(milliseconds)
	}
	return date.UTC().Format("Jan 2, 2006 15:04:05.000")
}
