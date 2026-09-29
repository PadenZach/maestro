"""Offline, stdlib-only capture of sanitized wire examples from pinned Python SDKs.

Usage: python3 internal/protocol/testdata/capture.py VERSION /tmp/pinned-sdk/VERSION
The directory must contain immutable dbos/_conductor/{protocol,conductor}.py
as protocol.py and conductor.py. Output is written beside this script.
No Python SDK installation, network, app database, or Go DTO is used.
"""

import hashlib
import importlib.util
import json
import sys
import types
from dataclasses import fields
from pathlib import Path

PINS = {
    "2.24.0": (
        "b066aec5996365e83aa33748fc863201c248ae80333748c1b80bb9b5bc9144eb",
        "a719584239b447abe42459b769e53caf55d87710948dfa76b952b589e85d92f1",
    ),
    "2.31.1": (
        "0431112b01d4da60f62716f985e3f6a9d7b0a25e0c7883900c98df47119ac6e0",
        "a81f2f60bb9c8a2b8527cc08d487e207437fa2954aa5b71ad771122fb2a9b9f9",
    ),
    "3.1.0": (
        "96acef8072c6ebe87ad3e8d7ecb7f8f7b1aa423af74881b98b4c2a6cabc9eadb",
        "befc49927f7845977e46e2fc45b386ceb2f189821a4930b3e93ea02dd5e44c78",
    ),
}


def load_protocol(version, directory):
    for name, digest in zip(("protocol.py", "conductor.py"), PINS[version]):
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"{version}: {name} SHA-256 mismatch")
    # Protocol dataclasses only: substitute imported SDK types not instantiated here.
    # Execute the *unmodified* pinned protocol.py (including its to_json/from_json).
    for module in ("dbos", "dbos._serialization", "dbos._sys_db"):
        sys.modules[module] = types.ModuleType(module)
    serialization = sys.modules["dbos._serialization"]
    serialization.Serializer = type("Serializer", (), {})
    serialization.safe_deserialize_schedule_context = lambda _: None
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
        "pinned_protocol", directory / "protocol.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load pinned protocol for {version}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def capture(version, directory):
    p = load_protocol(version, directory)
    requests = {}
    responses = {}
    request_classes = {
        "executor_info": "ExecutorInfoRequest",
        "list_workflows": "ListWorkflowsRequest",
        "list_queued_workflows": "ListQueuedWorkflowsRequest",
        "get_workflow": "GetWorkflowRequest",
        "list_steps": "ListStepsRequest",
        "get_workflow_events": "GetWorkflowEventsRequest",
        "get_workflow_notifications": "GetWorkflowNotificationsRequest",
        "get_workflow_streams": "GetWorkflowStreamsRequest",
        "list_queues": "ListQueuesRequest",
        "get_queue": "GetQueueRequest",
        "cancel": "CancelRequest",
        "resume": "ResumeRequest",
    }
    examples = {
        "executor_info": {},
        "list_workflows": {
            "body": {
                "sort_desc": False,
                "load_input": False,
                "load_output": False,
                "queues_only": False,
            }
        },
        "list_queued_workflows": {
            "body": {"sort_desc": False, "load_input": False, "load_output": False}
        },
        "get_workflow": {
            "workflow_id": "wf-sanitized",
            "load_input": False,
            "load_output": True,
        },
        "list_steps": {"workflow_id": "wf-sanitized", "load_output": True},
        "get_workflow_events": {"workflow_id": "wf-sanitized"},
        "get_workflow_notifications": {"workflow_id": "wf-sanitized"},
        "get_workflow_streams": {"workflow_id": "wf-sanitized"},
        "list_queues": {},
        "get_queue": {"name": "demo-queue"},
        "cancel": {"workflow_id": "wf-sanitized", "cancel_children": False},
        "resume": {"workflow_id": "wf-sanitized"},
        "list_workflows_filtered": {
            "body": {
                "sort_desc": True,
                "load_input": True,
                "load_output": False,
                "queues_only": False,
                "status": ["SUCCESS"],
                "workflow_name": ["demo"],
                "workflow_id_prefix": ["wf-"],
                "offset": 2,
                "limit": 5,
                "was_forked_from": False,
            }
        },
        "list_steps_paged": {
            "workflow_id": "wf-sanitized",
            "load_output": False,
            "limit": 5,
            "offset": 2,
        },
        "resume_queued": {"workflow_id": "wf-sanitized", "queue_name": "demo-queue"},
    }
    for name, body in examples.items():
        command = (
            name.removesuffix("_filtered")
            .removesuffix("_paged")
            .removesuffix("_queued")
            if name != "list_queued_workflows"
            else name
        )
        if name == "resume_queued":
            command = "resume"
        wire = {"type": command, "request_id": "req-sanitized", **body}
        cls = getattr(p, request_classes[command])
        parsed = cls.from_json(json.dumps(wire))
        if parsed.type != command or parsed.request_id != "req-sanitized":
            raise ValueError(f"{version}: {name} request envelope changed")
        # Check all supplied fields survive the released SDK's allowlisting;
        # absent optionals are legal when from_json fills the dataclass defaults.
        serialized = json.loads(parsed.to_json())
        for key, value in wire.items():
            if key not in serialized or serialized[key] != value:
                raise ValueError(f"{version}: {name} request field {key} changed")
        requests[name] = wire

    def reply(command, **body):
        cls = getattr(
            p, "".join(word.title() for word in command.split("_")) + "Response"
        )
        obj = cls(
            type=getattr(p.MessageType, command.upper()),
            request_id="req-sanitized",
            **body,
        )
        return json.loads(obj.to_json())

    wf_values: dict[str, object] = {f.name: None for f in fields(p.WorkflowsOutput)}
    wf_values.update(
        WorkflowUUID="wf-sanitized",
        Status="SUCCESS",
        WorkflowName="demo",
        Input='["opaque\\ninput"]',
        Output='{"opaque":true}',
        WasForkedFrom=False,
        CreatedAt="123",
        QueueName="demo-queue",
    )
    if version != "2.24.0":
        # Explicit nulls are emitted by the newer SDK; non-null values are
        # covered separately by newer-version fixture cases, not this baseline.
        wf_values.update(Attributes=None, ScheduleName=None, ApplicationName=None)
    wf = p.WorkflowsOutput(**wf_values)
    step = p.WorkflowSteps(
        function_id=1,
        function_name="demo_step",
        output='["opaque-step"]',
        error=None,
        child_workflow_id=None,
        started_at_epoch_ms="123",
        completed_at_epoch_ms="124",
    )
    queue_values = dict(
        name="demo-queue",
        concurrency=None,
        worker_concurrency=None,
        rate_limit_max=None,
        rate_limit_period_sec=None,
        priority_enabled=True,
        partition_queue=False,
        polling_interval_sec=0.5,
    )
    if version != "2.24.0":
        queue_values["application_name"] = None
    queue = p.QueueOutput(**queue_values)
    responses.update(
        {
            "executor_info": reply(
                "executor_info",
                executor_id="exec-sanitized",
                application_version="app-v1",
                hostname=None,
                language="python",
                dbos_version=version,
                executor_metadata={"role": "fixture"},
            ),
            "list_workflows": reply("list_workflows", output=[wf]),
            "list_queued_workflows": reply("list_queued_workflows", output=[wf]),
            "get_workflow": reply("get_workflow", output=wf),
            "get_workflow_missing": reply("get_workflow", output=None),
            "list_steps": reply("list_steps", output=[step]),
            "list_steps_null": reply("list_steps", output=None),
            "get_workflow_events": reply(
                "get_workflow_events",
                events=[p.EventOutput(key="safe", value='{"opaque":1}')],
            ),
            "get_workflow_notifications": reply(
                "get_workflow_notifications",
                notifications=[
                    p.NotificationOutput(
                        topic=None,
                        message='["opaque"]',
                        created_at_epoch_ms=123,
                        consumed=False,
                    )
                ],
            ),
            "get_workflow_streams": reply(
                "get_workflow_streams",
                streams=[p.StreamEntryOutput(key="safe", values=['"opaque"'])],
            ),
            "list_queues": reply("list_queues", output=[queue]),
            "get_queue": reply("get_queue", output=queue),
            "get_queue_missing": reply("get_queue", output=None),
            "cancel": reply("cancel", success=True),
            "resume": reply("resume", success=True),
            "cancel_failed": reply(
                "cancel", success=False, error_message="sanitized failure"
            ),
        }
    )
    return {"requests": requests, "responses": responses}


def capture_recent(version, directory):
    """Additional non-null recent fields, independently serialized by SDK types."""
    p = load_protocol(version, directory)
    filters = {
        "attributes": {"region": "west", "attempt": 2},
        "schedule_name": ["nightly"],
        "application_name": ["app-sanitized"],
    }
    cases = {
        "list_workflows_recent": (
            "list_workflows",
            {
                "body": {
                    "sort_desc": False,
                    "load_input": False,
                    "load_output": False,
                    "queues_only": False,
                    **filters,
                }
            },
        ),
        "list_queued_workflows_recent": (
            "list_queued_workflows",
            {
                "body": {
                    "sort_desc": False,
                    "load_input": False,
                    "load_output": False,
                    **filters,
                }
            },
        ),
        "list_queues_recent": (
            "list_queues",
            {
                "body": {
                    "application_name": ["app-sanitized"],
                }
            },
        ),
    }
    requests = {}
    for name, (command, body) in cases.items():
        frame = {"type": command, "request_id": "req-sanitized", **body}
        cls = getattr(
            p, "".join(word.title() for word in command.split("_")) + "Request"
        )
        decoded = json.loads(cls.from_json(json.dumps(frame)).to_json())
        for key, value in frame.items():
            if decoded.get(key) != value:
                raise ValueError(f"{version}: {name} request field {key} changed")
        requests[name] = frame

    # Exercise the SDK's conversion, including json.dumps(attributes), rather
    # than encoding the expected workflow with a Go type or handwritten string.
    wf_info = types.SimpleNamespace(
        workflow_id="wf-sanitized",
        status="SUCCESS",
        name="demo",
        class_name=None,
        config_name=None,
        authenticated_user=None,
        assumed_role=None,
        authenticated_roles=None,
        input=None,
        output=None,
        error=None,
        created_at=123,
        updated_at=None,
        queue_name=None,
        app_version=None,
        executor_id=None,
        workflow_timeout_ms=None,
        workflow_deadline_epoch_ms=None,
        deduplication_id=None,
        priority=None,
        queue_partition_key=None,
        forked_from=None,
        was_forked_from=False,
        parent_workflow_id=None,
        dequeued_at=None,
        delay_until_epoch_ms=None,
        completed_at=None,
        attributes=filters["attributes"],
        schedule_name="nightly",
        application_name="app-sanitized",
    )
    wf = p.WorkflowsOutput.from_workflow_information(wf_info)
    limiter = {"limit": 9, "period": 1.5}
    q = types.SimpleNamespace(
        name="demo-queue",
        _concurrency=None,
        _worker_concurrency=None,
        _limiter=None,
        _priority_enabled=True,
        _partition_queue=True,
        _polling_interval_sec=0.5,
        application_name="app-sanitized",
        _partition_concurrency=2,
        _partition_worker_concurrency=3,
        _partition_limiter=limiter,
        _has_partition_limits=lambda: True,
    )
    queue = p.QueueOutput.from_queue(q)
    responses = {
        "get_workflow_recent": json.loads(
            p.GetWorkflowResponse(
                type=p.MessageType.GET_WORKFLOW,
                request_id="req-sanitized",
                output=wf,
            ).to_json()
        ),
        "get_queue_recent": json.loads(
            p.GetQueueResponse(
                type=p.MessageType.GET_QUEUE,
                request_id="req-sanitized",
                output=queue,
            ).to_json()
        ),
    }
    return {"requests": requests, "responses": responses}


if __name__ == "__main__":
    version, directory = sys.argv[1:]
    result = capture(version, Path(directory))
    dest = Path(__file__).resolve().parent / version / "wire.json"
    dest.parent.mkdir(exist_ok=True)
    dest.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(
        f"{dest}: {len(result['requests'])} requests, {len(result['responses'])} responses"
    )
    if version != "2.24.0":
        recent = capture_recent(version, Path(directory))
        recent_dest = dest.with_name("recent.json")
        recent_dest.write_text(json.dumps(recent, indent=2, ensure_ascii=False) + "\n")
        print(
            f"{recent_dest}: {len(recent['requests'])} requests, {len(recent['responses'])} responses"
        )
