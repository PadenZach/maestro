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
    QueueOutput,
    WorkflowsOutput,
    WorkflowSteps,
)
from dbos._workflow_commands import get_workflow

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
    return gate_step(value)


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
    handle = DBOS.start_workflow(gate_workflow, "gate-input-value")
    assert handle.get_result() == "gate-step-value"

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

    queue_sha256 = {}
    for queue in (gate_queue, edge_queue):
        queue_wire = asdict(QueueOutput.from_queue(queue))
        encoded = json.dumps(
            queue_wire, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        queue_sha256[queue.name] = hashlib.sha256(encoded).hexdigest()

    # Only identifiers and digests of SDK-produced wire values cross this
    # temporary SDK-to-gate channel. No payload or credential is published.
    ready = {
        "workflow_id": handle.workflow_id,
        "input_sha256": hashlib.sha256(workflow_wire.Input.encode()).hexdigest(),
        "output_sha256": hashlib.sha256(workflow_wire.Output.encode()).hexdigest(),
        "step_output_sha256": hashlib.sha256(step_wire.output.encode()).hexdigest(),
        "event_value_sha256": hashlib.sha256(event_wire.value.encode()).hexdigest(),
        "queue_sha256": queue_sha256,
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
