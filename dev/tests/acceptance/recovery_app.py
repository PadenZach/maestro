"""Real DBOS executor for the isolated recovery gate; owns no recovery logic."""

import json
import os
import signal
import time
from pathlib import Path

from dbos import DBOS, SetWorkflowID
from dbos._utils import GlobalParams
from websockets.sync.connection import Connection

directory = Path(os.environ["RECOVERY_DIRECTORY"])
dbos = DBOS(
    config={
        "name": os.environ["RECOVERY_APP"],
        "system_database_url": os.environ["RECOVERY_DATABASE_URL"],
        # Loopback ws is an isolated test transport, not deployment configuration.
        "conductor_url": os.environ["RECOVERY_WS"],
        "conductor_key": "recovery-test",
        "application_version": os.environ.get("RECOVERY_VERSION", "v1"),
        "run_admin_server": False,
    }
)


def append(name, value):
    with (directory / name).open("a") as output:
        output.write(json.dumps(value) + "\n")


@DBOS.step()
def checkpoint(workflow_id):
    append(
        "effects.jsonl", {"workflow": workflow_id, "executor": GlobalParams.executor_id}
    )
    return "checkpointed"


@DBOS.workflow()
def recovery_workflow(workflow_id):
    append(
        "starts.jsonl",
        {
            "workflow": workflow_id,
            "executor": GlobalParams.executor_id,
            "time": time.time(),
        },
    )
    checkpoint(workflow_id)
    # This marker is written only after the step's result has been committed.
    (directory / f"{workflow_id}.checkpointed").touch()
    while not (directory / "finish").exists():
        time.sleep(0.05)
    return "recovered"


# Observe real SDK recovery replies and optionally interrupt the acknowledgement.
# The SDK executes its ordinary recovery handler before reaching this hook.
original_send = Connection.send


def observe_send(self, message, *args, **kwargs):
    if isinstance(message, str):
        frame = json.loads(message)
        if frame.get("type") == "recovery":
            append("replies.jsonl", frame)
            if os.environ.get("RECOVERY_HOLD_REPLY") == "1":
                (directory / "reply-held").touch()
                while not (directory / "release-reply").exists():
                    time.sleep(0.05)
    return original_send(self, message, *args, **kwargs)


Connection.send = observe_send
DBOS.launch()
queue = DBOS.register_queue("recovery-test", concurrency=2)
(directory / f"executor-{os.getpid()}.json").write_text(
    json.dumps({"id": GlobalParams.executor_id})
)
if os.environ.get("RECOVERY_SEED") == "1":
    with SetWorkflowID("direct"):
        DBOS.start_workflow(recovery_workflow, "direct")
    with SetWorkflowID("queued"):
        queue.enqueue(recovery_workflow, "queued")
while True:
    signal.pause()
