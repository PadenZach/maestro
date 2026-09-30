"""Released DBOS 3.1.0 process backed only by the gate-owned Postgres cluster.

The loopback WebSocket setting is an internal acceptance-test seam, not an
application-setup fallback for DBOS_CONDUCTOR_URL or production transport.
"""
# pyright: reportMissingImports=false

import hashlib
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

from dbos import DBOS
from dbos._conductor.protocol import (
    EventOutput,
    NotificationOutput,
    QueueOutput,
    ScheduleOutput,
    StepAggregateOutput,
    StreamEntryOutput,
    WorkflowAggregateOutput,
    WorkflowsOutput,
    WorkflowSteps,
)
from dbos._workflow_commands import get_workflow
from websockets.sync.connection import Connection

config = {
    "name": os.environ["POSTGRES_GATE_APP"],
    "system_database_url": os.environ["POSTGRES_GATE_DATABASE_URL"],
    "conductor_url": os.environ["POSTGRES_GATE_TEST_WS"],
    "conductor_key": os.environ["POSTGRES_GATE_KEY"],
    "executor_id": os.environ["POSTGRES_GATE_EXECUTOR"],
    "application_version": "postgres-gate-v1",
    "conductor_executor_metadata": {"gate": "postgres18-read-smoke"},
    "conductor_metadata_only_mode": os.environ["POSTGRES_GATE_METADATA_ONLY"] == "1",
    "run_admin_server": False,
}
dbos = DBOS(config=config)


@DBOS.step()
def gate_step(value: str) -> str:
    return "gate-step-value"


@DBOS.workflow()
def gate_workflow(value: str) -> str:
    DBOS.set_event("gate-event", "gate-event-value")
    for stream_value in ("stream-first", {"stream": 2}, ["stream", 3]):
        DBOS.write_stream("gate-stream", stream_value)
    DBOS.close_stream("gate-stream")
    DBOS.close_stream("gate-empty-stream")
    return gate_step(value)


@DBOS.workflow()
def gate_empty_related_workflow() -> str:
    return "empty-related"


@DBOS.workflow()
def gate_notification_target() -> str:
    return "notification-target"


@DBOS.workflow()
def gate_scheduled(when, context) -> str:
    return "scheduled"


def wire_digest(value) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


# gzip embeds a current-time header, so separate calls to export_workflow do not
# produce a stable blob. Observe the released handler's outbound SDK frame and
# publish only its string digest; never decode, normalize, or persist the blob.
_original_connection_send = Connection.send


def observing_connection_send(self, message, *args, **kwargs):
    if isinstance(message, str):
        try:
            frame = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            frame = None
        if isinstance(frame, dict) and frame.get("type") == "export_workflow":
            serialized = frame.get("serialized_workflow")
            if isinstance(serialized, str):
                digest_path = Path(os.environ["POSTGRES_GATE_EXPORT_DIGEST"])
                pending_path = digest_path.with_suffix(".tmp")
                pending_path.write_text(hashlib.sha256(serialized.encode()).hexdigest())
                pending_path.replace(digest_path)
    return _original_connection_send(self, message, *args, **kwargs)


Connection.send = observing_connection_send


try:
    DBOS.launch()
    gate_queue = DBOS.register_queue(
        "gate-queue",
        global_concurrency=3,
        worker_concurrency=2,
        partition_concurrency=2,
        partition_worker_concurrency=1,
        limiter={"limit": 5, "period": 1.5},
        partition_limiter={"limit": 4, "period": 2.5},
        polling_interval_sec=0.25,
    )
    edge_queue = DBOS.register_queue(
        "gate-edge-queue",
        global_concurrency=0,
        limiter={"limit": 0, "period": 0.5},
        polling_interval_sec=0.125,
    )
    DBOS.create_schedule(
        schedule_name="gate-schedule-context",
        workflow_fn=gate_scheduled,
        schedule="0 0 1 1 *",
        context={"gate": "schedule-context-value"},
        automatic_backfill=False,
        cron_timezone="UTC",
        queue_name=gate_queue.name,
    )
    DBOS.create_schedule(
        schedule_name="gate-schedule-null",
        workflow_fn=gate_scheduled,
        schedule="0 0 1 1 *",
        context=None,
        automatic_backfill=False,
    )
    handle = DBOS.start_workflow(gate_workflow, "gate-input-value")
    assert handle.get_result() == "gate-step-value"
    empty_related_handle = DBOS.start_workflow(gate_empty_related_workflow)
    assert empty_related_handle.get_result() == "empty-related"

    # The zero-limit SDK queue is a deterministic barrier: its target remains
    # enqueued while public DBOS.send operations create pending (unconsumed)
    # notifications, with no timing sleep or direct database write.
    notification_handle = edge_queue.enqueue(gate_notification_target)
    DBOS.send(notification_handle.workflow_id, "notification-null-topic", topic=None)
    DBOS.send(
        notification_handle.workflow_id, {"notification": "empty-topic"}, topic=""
    )
    pending_notifications = dbos._sys_db.get_all_notifications(
        notification_handle.workflow_id
    )
    assert len(pending_notifications) == 2
    assert {notification["topic"] for notification in pending_notifications} == {
        None,
        "",
    }
    assert all(not notification["consumed"] for notification in pending_notifications)

    workflow_info = get_workflow(dbos._sys_db, handle.workflow_id)
    assert workflow_info is not None
    workflow_wire = WorkflowsOutput.from_workflow_information(workflow_info)
    assert workflow_wire.Input is not None and workflow_wire.Output is not None

    step_infos = dbos._sys_db.list_workflow_steps(handle.workflow_id, load_output=True)
    step_wire = next(
        WorkflowSteps.from_step_info(info)
        for info in step_infos
        if info["function_name"] == "gate_step"
    )
    assert step_wire.output is not None

    event_values = dbos._sys_db.get_all_events(handle.workflow_id)
    event_wire = EventOutput.from_event_data("gate-event", event_values["gate-event"])
    event_sha256 = {"gate-event": wire_digest(asdict(event_wire))}

    notification_sha256 = {}
    for notification in dbos._sys_db.get_all_notifications(
        notification_handle.workflow_id
    ):
        notification_wire = NotificationOutput.from_notification_info(notification)
        label = "null-topic" if notification_wire.topic is None else "empty-topic"
        notification_sha256[label] = wire_digest(asdict(notification_wire))
    assert set(notification_sha256) == {"null-topic", "empty-topic"}

    stream_sha256 = {}
    for stream_key, stream_values in dbos._sys_db.get_all_stream_entries(
        handle.workflow_id
    ).items():
        stream_wire = StreamEntryOutput.from_stream_data(stream_key, stream_values)
        stream_sha256[stream_key] = wire_digest(asdict(stream_wire))
    assert set(stream_sha256) == {"gate-stream", "gate-empty-stream"}

    queue_sha256 = {}
    for queue in (gate_queue, edge_queue):
        queue_wire = asdict(QueueOutput.from_queue(queue))
        encoded = json.dumps(
            queue_wire, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        queue_sha256[queue.name] = hashlib.sha256(encoded).hexdigest()

    workflow_aggregate_options = {
        "group_by_status": True,
        "group_by_name": True,
        "group_by_executor_id": True,
        "group_by_application_version": True,
        "group_by_application_name": True,
        "select_count": True,
        "select_min_created_at": True,
        "select_max_queue_wait_ms": True,
        "select_max_total_latency_ms": True,
        "time_bucket_size_ms": 60_000,
        "status": ["SUCCESS"],
        "start_time": "2000-01-01T00:00:00+00:00",
        "end_time": "2100-01-01T00:00:00+00:00",
        "completed_after": "2000-01-01T00:00:00+00:00",
        "completed_before": "2100-01-01T00:00:00+00:00",
        "name": ["gate_workflow"],
        "app_version": ["postgres-gate-v1"],
        "workflow_id_prefix": [handle.workflow_id[:8]],
        "workflow_ids": [handle.workflow_id],
        "was_forked_from": False,
        "has_parent": False,
    }
    workflow_aggregate_rows = dbos._sys_db.get_workflow_aggregates(
        **workflow_aggregate_options
    )
    assert len(workflow_aggregate_rows) == 1
    workflow_aggregate_wire = [
        asdict(
            WorkflowAggregateOutput(
                group=row["group"],
                count=row["count"],
                min_created_at=row["min_created_at"],
                max_queue_wait_ms=row["max_queue_wait_ms"],
                max_total_latency_ms=row["max_total_latency_ms"],
            )
        )
        for row in workflow_aggregate_rows
    ]

    step_aggregate_options = {
        "group_by_function_name": True,
        "group_by_status": True,
        "select_count": True,
        "select_max_duration_ms": True,
        "time_bucket_size_ms": 60_000,
        "status": ["SUCCESS"],
        "function_name": ["gate_step"],
        "workflow_id_prefix": [handle.workflow_id[:8]],
        "completed_after": "2000-01-01T00:00:00+00:00",
        "completed_before": "2100-01-01T00:00:00+00:00",
    }
    step_aggregate_rows = dbos._sys_db.get_step_aggregates(**step_aggregate_options)
    assert len(step_aggregate_rows) == 1
    step_aggregate_wire = [
        asdict(
            StepAggregateOutput(
                group=row["group"],
                count=row["count"],
                max_duration_ms=row["max_duration_ms"],
            )
        )
        for row in step_aggregate_rows
    ]

    schedule_sha256 = {}
    for schedule_name in ("gate-schedule-context", "gate-schedule-null"):
        schedule = dbos._sys_db.get_schedule(schedule_name)
        assert schedule is not None
        digests = {}
        for label, load_context in (("context", True), ("without_context", False)):
            schedule_wire = asdict(
                ScheduleOutput.from_schedule(
                    schedule,
                    dbos._sys_db.serializer,
                    load_context=load_context,
                )
            )
            # queue_name is a required SDK key but is absent from the pinned
            # official HTTP Schedule schema.
            del schedule_wire["queue_name"]
            encoded = json.dumps(
                schedule_wire,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
            digests[label] = hashlib.sha256(encoded).hexdigest()
        schedule_sha256[schedule_name] = digests

    # Only identifiers and digests of SDK-produced wire values cross this
    # temporary SDK-to-gate channel. No payload or credential is published.
    ready = {
        "workflow_id": handle.workflow_id,
        "empty_related_workflow_id": empty_related_handle.workflow_id,
        "notification_workflow_id": notification_handle.workflow_id,
        "input_sha256": hashlib.sha256(workflow_wire.Input.encode()).hexdigest(),
        "output_sha256": hashlib.sha256(workflow_wire.Output.encode()).hexdigest(),
        "step_output_sha256": hashlib.sha256(step_wire.output.encode()).hexdigest(),
        "event_value_sha256": hashlib.sha256(event_wire.value.encode()).hexdigest(),
        "event_sha256": event_sha256,
        "notification_sha256": notification_sha256,
        "stream_sha256": stream_sha256,
        "queue_sha256": queue_sha256,
        "schedule_sha256": schedule_sha256,
        "workflow_aggregate_sha256": wire_digest(workflow_aggregate_wire),
        "step_aggregate_sha256": wire_digest(step_aggregate_wire),
    }
    ready_path = Path(os.environ["POSTGRES_GATE_READY"])
    pending_path = ready_path.with_suffix(".tmp")
    pending_path.write_text(json.dumps(ready))
    pending_path.replace(ready_path)

    for line in sys.stdin:
        if line.strip() == "stop":
            break
finally:
    DBOS.destroy()
