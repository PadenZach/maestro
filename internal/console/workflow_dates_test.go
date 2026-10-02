package console

import (
	"testing"
)

func TestWorkflowDatesPreserveUTCAndMilliseconds(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"2026-10-01T23:59:59.999Z", "2026-10-01T23:59:59.999"},
		{"2026-10-01T00:00:00.001+00:00", "2026-10-01T00:00:00.001"},
		{"", ""},
		{"invalid", ""},
	} {
		if got := UTCDateInput(tc.raw); got != tc.want {
			t.Errorf("UTCDateInput(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
	for _, tc := range []struct{ raw, want string }{
		{"1700000000123", "Nov 14, 2023 22:13:20.123"},
		{"2026-10-01T23:59:59.999Z", "Oct 1, 2026 23:59:59.999"},
		{"2026-10-01T00:00:00.001-05:00", "Oct 1, 2026 05:00:00.001"},
		{"invalid", "invalid"},
		{"", "—"},
	} {
		if got := WorkflowCreatedUTC(&tc.raw); got != tc.want {
			t.Errorf("WorkflowCreatedUTC(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
	if got := WorkflowCreatedUTC(nil); got != "—" {
		t.Errorf("nil date = %q", got)
	}
}
