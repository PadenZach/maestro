"""Capture sanitized inspection-read wire examples from pinned DBOS Python 3.1.0.

Usage: capture_inspection_reads.py SDK_CONDUCTOR_DIRECTORY

SDK_CONDUCTOR_DIRECTORY must contain the released dbos/_conductor/protocol.py and
conductor.py files. The script verifies both immutable source hashes, uses the
SDK's own request/response JSON serializers, and requires no live DBOS runtime
or application database.
"""

import hashlib
import importlib.util
import json
import sys
import types
from pathlib import Path

VERSION = "3.1.0"
PINS = {
    "protocol.py": "96acef8072c6ebe87ad3e8d7ecb7f8f7b1aa423af74881b98b4c2a6cabc9eadb",
    "conductor.py": "befc49927f7845977e46e2fc45b386ceb2f189821a4930b3e93ea02dd5e44c78",
}


def load_protocol(directory: Path):
    for name, expected in PINS.items():
        actual = hashlib.sha256((directory / name).read_bytes()).hexdigest()
        if actual != expected:
            raise ValueError(f"{VERSION}: {name} SHA-256 mismatch: {actual}")

    # Execute the unmodified protocol module while stubbing unrelated SDK types.
    # None of these stubs is called by the dataclass serializers used below.
    for module_name in ("dbos", "dbos._serialization", "dbos._sys_db"):
        sys.modules[module_name] = types.ModuleType(module_name)
    serialization = sys.modules["dbos._serialization"]
    vars(serialization)["Serializer"] = type("Serializer", (), {})
    vars(serialization)["safe_deserialize_schedule_context"] = lambda *args, **kwargs: None
    sys_db = sys.modules["dbos._sys_db"]
    for name in (
        "NotificationInfo",
        "StepInfo",
        "VersionInfo",
        "WorkflowSchedule",
        "WorkflowStatus",
    ):
        setattr(sys_db, name, type(name, (), {}))

    spec = importlib.util.spec_from_file_location(
        "pinned_inspection_protocol", directory / "protocol.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load pinned protocol.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def capture(directory: Path) -> dict[str, dict[str, object]]:
    p = load_protocol(directory)

    request_cases = {
        "list_schedules": (
            p.ListSchedulesRequest,
            {"type": "list_schedules", "request_id": "req-sanitized", "body": {}},
        ),
        "list_schedules_all_fields": (
            p.ListSchedulesRequest,
            {
                "type": "list_schedules",
                "request_id": "req-sanitized",
                "body": {
                    "status": ["ACTIVE"],
                    "workflow_name": ["demo_workflow"],
                    "schedule_name_prefix": ["nightly"],
                    "application_name": ["app-sanitized"],
                    "load_context": False,
                },
            },
        ),
        "get_schedule": (
            p.GetScheduleRequest,
            {
                "type": "get_schedule",
                "request_id": "req-sanitized",
                "schedule_name": "nightly-sanitized",
            },
        ),
        "get_workflow_aggregates": (
            p.GetWorkflowAggregatesRequest,
            {
                "type": "get_workflow_aggregates",
                "request_id": "req-sanitized",
                "body": {},
            },
        ),
        "get_workflow_aggregates_all_fields": (
            p.GetWorkflowAggregatesRequest,
            {
                "type": "get_workflow_aggregates",
                "request_id": "req-sanitized",
                "body": {
                    "group_by_status": False,
                    "group_by_name": True,
                    "group_by_queue_name": False,
                    "group_by_executor_id": True,
                    "group_by_application_version": False,
                    "group_by_application_name": True,
                    "select_count": True,
                    "select_min_created_at": False,
                    "select_max_queue_wait_ms": True,
                    "select_max_total_latency_ms": False,
                    "time_bucket_size_ms": 0,
                    "status": [],
                    "start_time": "2026-01-01T00:00:00Z",
                    "end_time": "2026-01-02T00:00:00Z",
                    "completed_after": "2026-01-03T00:00:00Z",
                    "completed_before": "2026-01-04T00:00:00Z",
                    "dequeued_after": "2026-01-05T00:00:00Z",
                    "dequeued_before": "2026-01-06T00:00:00Z",
                    "name": ["demo_workflow"],
                    "app_version": ["app-v1"],
                    "executor_id": ["exec-sanitized"],
                    "queue_name": ["demo-queue"],
                    "workflow_id_prefix": ["wf-"],
                    "workflow_ids": ["wf-sanitized"],
                    "forked_from": ["source-wf"],
                    "parent_workflow_id": ["parent-wf"],
                    "user": ["user-sanitized"],
                    "schedule_name": ["nightly-sanitized"],
                    "application_name": ["app-sanitized"],
                    "was_forked_from": False,
                    "has_parent": False,
                    "attributes": {},
                },
            },
        ),
        "get_step_aggregates": (
            p.GetStepAggregatesRequest,
            {
                "type": "get_step_aggregates",
                "request_id": "req-sanitized",
                "body": {},
            },
        ),
        "get_step_aggregates_all_fields": (
            p.GetStepAggregatesRequest,
            {
                "type": "get_step_aggregates",
                "request_id": "req-sanitized",
                "body": {
                    "group_by_function_name": False,
                    "group_by_status": True,
                    "select_count": False,
                    "select_max_duration_ms": True,
                    "time_bucket_size_ms": 0,
                    "status": [],
                    "function_name": ["demo_step"],
                    "workflow_id_prefix": ["wf-"],
                    "completed_after": "2026-02-01T00:00:00Z",
                    "completed_before": "2026-02-02T00:00:00Z",
                    "application_name": ["app-sanitized"],
                },
            },
        ),
        "export_workflow": (
            p.ExportWorkflowRequest,
            {
                "type": "export_workflow",
                "request_id": "req-sanitized",
                "workflow_id": "wf-sanitized",
                "export_children": False,
            },
        ),
        "export_workflow_children": (
            p.ExportWorkflowRequest,
            {
                "type": "export_workflow",
                "request_id": "req-sanitized",
                "workflow_id": "wf-sanitized",
                "export_children": True,
            },
        ),
    }
    requests = {}
    for name, (cls, frame) in request_cases.items():
        parsed = cls.from_json(json.dumps(frame))
        serialized = json.loads(parsed.to_json())
        for key, value in frame.items():
            if serialized.get(key) != value:
                raise ValueError(f"{name}: SDK request field {key} changed")
        requests[name] = serialized

    schedule = p.ScheduleOutput(
        schedule_id="schedule-id-sanitized",
        schedule_name="nightly-sanitized",
        workflow_name="demo_workflow",
        workflow_class_name=None,
        schedule="0 0 * * *",
        status="ACTIVE",
        context=None,
        last_fired_at="2026-03-01T00:00:00Z",
        automatic_backfill=False,
        cron_timezone=None,
        queue_name="demo-queue",
        application_name="app-sanitized",
    )
    workflow_aggregate = p.WorkflowAggregateOutput(
        group={"status": "SUCCESS", "queue_name": None},
        count=0,
        min_created_at=None,
        max_queue_wait_ms=0,
        max_total_latency_ms=None,
    )
    step_aggregate = p.StepAggregateOutput(
        group={"function_name": "demo_step", "status": None},
        count=0,
        max_duration_ms=None,
    )

    def response(cls, message_type, **body):
        return json.loads(
            cls(
                type=message_type,
                request_id="req-sanitized",
                **body,
            ).to_json()
        )

    opaque_export = "H4sIAOpaqueSDKSerializedData==\nnot-decoded"
    responses = {
        "list_schedules": response(
            p.ListSchedulesResponse,
            p.MessageType.LIST_SCHEDULES,
            output=[schedule],
        ),
        "get_schedule": response(
            p.GetScheduleResponse,
            p.MessageType.GET_SCHEDULE,
            output=schedule,
        ),
        "get_schedule_missing": response(
            p.GetScheduleResponse,
            p.MessageType.GET_SCHEDULE,
            output=None,
        ),
        "get_workflow_aggregates": response(
            p.GetWorkflowAggregatesResponse,
            p.MessageType.GET_WORKFLOW_AGGREGATES,
            output=[workflow_aggregate],
        ),
        "get_step_aggregates": response(
            p.GetStepAggregatesResponse,
            p.MessageType.GET_STEP_AGGREGATES,
            output=[step_aggregate],
        ),
        "export_workflow": response(
            p.ExportWorkflowResponse,
            p.MessageType.EXPORT_WORKFLOW,
            serialized_workflow=opaque_export,
        ),
        "export_workflow_missing": response(
            p.ExportWorkflowResponse,
            p.MessageType.EXPORT_WORKFLOW,
            serialized_workflow=None,
        ),
    }
    return {"requests": requests, "responses": responses}


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: capture_inspection_reads.py SDK_CONDUCTOR_DIRECTORY")
    result = capture(Path(sys.argv[1]))
    destination = Path(__file__).resolve().parent / VERSION / "inspection_reads.json"
    destination.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(
        f"{destination}: {len(result['requests'])} requests, "
        f"{len(result['responses'])} responses"
    )
