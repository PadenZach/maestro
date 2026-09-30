"""Tiny released-DBOS process. All database access is SDK-owned, never maestro-owned."""
# pyright: reportMissingImports=false
# dbos resolves at runtime from the release-specific isolated .venv, not the host Python.

import hashlib
import json
import os
import sys
from pathlib import Path

from dbos import DBOS
from dbos._conductor.protocol import WorkflowsOutput
from dbos._workflow_commands import get_workflow

version = os.environ["GATE_VERSION"]
config = {
    "name": os.environ["GATE_APP"],
    "system_database_url": "sqlite:///" + os.environ["GATE_DB"],
    "conductor_url": os.environ["GATE_WS"],
    "conductor_key": os.environ["GATE_KEY"],
    "executor_id": os.environ["GATE_EXECUTOR"],
    "application_version": "gate-v1",
    "conductor_executor_metadata": {"gate": "released-python-reads"},
    "run_admin_server": False,
}
if version.startswith("3."):
    config["conductor_metadata_only_mode"] = os.environ.get("GATE_PRIVATE") == "1"
dbos = DBOS(config=config)


@DBOS.step()
def gate_step(value: str) -> str:
    return "step-" + value


@DBOS.workflow()
def gate_workflow(value: str) -> str:
    return gate_step(value)


@DBOS.workflow()
def gate_scheduled(when, context) -> str:
    return gate_step("scheduled")


try:
    DBOS.launch()
    if version == "2.24.0":
        DBOS.register_queue(
            "gate-queue",
            concurrency=2,
            worker_concurrency=1,
            limiter={"limit": 5, "period": 1.0},
        )
    else:
        DBOS.register_queue(
            "gate-queue",
            global_concurrency=3,
            worker_concurrency=2,
            partition_concurrency=2,
            partition_worker_concurrency=1,
            limiter={"limit": 5, "period": 1.0},
            partition_limiter={"limit": 4, "period": 2.0},
        )
    # SDK populates its own SQLite DB. Only publish the id, never serialized data.
    handle = DBOS.start_workflow(gate_workflow, "gate-value")
    assert handle.get_result() == "step-gate-value"
    if version != "2.24.0":
        DBOS.update_workflow_attributes(handle.workflow_id, {"gate": "visible"})
    sdk_info = get_workflow(dbos._sys_db, handle.workflow_id)
    assert sdk_info is not None
    sdk_wire = WorkflowsOutput.from_workflow_information(sdk_info)
    assert sdk_wire.Input is not None and sdk_wire.Output is not None
    # Only digests of SDK-produced wire strings cross the temporary app→gate channel.
    ready = {
        "workflow_id": handle.workflow_id,
        "input_sha256": hashlib.sha256(sdk_wire.Input.encode()).hexdigest(),
        "output_sha256": hashlib.sha256(sdk_wire.Output.encode()).hexdigest(),
    }
    if version != "2.24.0":
        DBOS.create_schedule(
            schedule_name="gate-schedule",
            workflow_fn=gate_scheduled,
            schedule="0 0 1 1 *",
        )
        scheduled = DBOS.trigger_schedule("gate-schedule")
        assert scheduled.get_result() == "step-scheduled"
        ready["scheduled_workflow_id"] = scheduled.workflow_id
    Path(os.environ["GATE_READY"]).write_text(json.dumps(ready))
    original_events = dbos._sys_db.get_all_events
    for line in sys.stdin:
        if line.strip() == "inject-events-error":

            def fail_events(workflow_id: str):
                raise RuntimeError("gate-injected-events-error")

            dbos._sys_db.get_all_events = fail_events
            Path(os.environ["GATE_INJECTION_ACK"]).write_text("on")
        elif line.strip() == "restore-events":
            dbos._sys_db.get_all_events = original_events
            Path(os.environ["GATE_INJECTION_ACK"]).write_text("off")
        elif line.strip() == "reconnect":
            assert dbos.conductor_websocket is not None
            assert dbos.conductor_websocket.websocket is not None
            dbos.conductor_websocket.websocket.close()
        elif line.strip() == "stop":
            break
finally:
    DBOS.destroy()
