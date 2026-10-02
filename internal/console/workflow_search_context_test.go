package console

import (
	"context"
	"errors"
	"testing"
)

func TestConsoleWorkflowSearchHonorsCanceledRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rows, err := (&handler{}).fetchConsoleRows(ctx, "app", filterState{Name: "gate"}, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled search error = %v, want context.Canceled", err)
	}
	if len(rows.Workflows) != 0 || rows.RangeLabel != "" {
		t.Fatalf("canceled search returned successful results: %+v", rows)
	}
}
