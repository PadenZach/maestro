package testserver

func WorkflowAggregate() map[string]any {
	return map[string]any{
		"group": map[string]any{"status": "SUCCESS", "queue_name": nil},
		"count": int64(0), "min_created_at": int64(0),
		"max_queue_wait_ms": int64(0), "max_total_latency_ms": int64(0),
	}
}

func StepAggregate() map[string]any {
	return map[string]any{
		"group": map[string]any{"function_name": "gate_step", "status": nil},
		"count": int64(0), "max_duration_ms": int64(0),
	}
}
