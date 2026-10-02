"""Exercise API and Console reads against real DBOS data in isolated Postgres 18."""

import argparse
import datetime
import hashlib
import json
import math
import os
import re
import secrets
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
POSTGRES_PROGRAMS = ("initdb", "postgres", "createdb", "pg_isready")
SDK_VERSION = "3.1.0"
CASE_DEADLINE_SECONDS = 70
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def sanitized_environment(environment, temp):
    """Retain executable lookup only; discard operator proxy, DB, and DBOS config."""
    clean = {
        key: environment[key] for key in ("PATH", "SYSTEMROOT") if key in environment
    }
    clean.update(
        HOME=str(temp),
        TMPDIR=str(temp),
        XDG_CACHE_HOME=str(temp / "cache"),
        XDG_CONFIG_HOME=str(temp / "config"),
        XDG_DATA_HOME=str(temp / "data"),
        PYTHONNOUSERSITE="1",
        PYTHONDONTWRITEBYTECODE="1",
        LC_ALL="C",
    )
    return clean


def postgres_database_url(socket_dir, port, database):
    query = urllib.parse.urlencode(
        {"host": str(socket_dir), "port": str(port)}, quote_via=urllib.parse.quote
    )
    return f"postgresql://gate@/{database}?{query}"


def validate_postgres_bin(pg_bin, environment):
    if not pg_bin.is_dir():
        raise RuntimeError("POSTGRES18_BIN is not a directory")
    for name in POSTGRES_PROGRAMS:
        executable = pg_bin / name
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise RuntimeError(f"POSTGRES18_BIN lacks executable {name}")
    result = subprocess.run(
        [str(pg_bin / "postgres"), "--version"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=8,
        check=True,
    )
    if re.search(r"\bPostgreSQL\)\s+18(?:\.|\s|$)", result.stdout.strip()) is None:
        raise RuntimeError("Postgres 18 is required by the acceptance gate")


def validate_sdk_python(python, environment):
    if not python.is_file() or not os.access(python, os.X_OK):
        raise RuntimeError("missing pinned SDK environment; run mise run sdk:install")
    version = subprocess.check_output(
        [str(python), "-I", "-c", "import importlib.metadata; print(importlib.metadata.version('dbos'))"],
        env=environment, text=True, timeout=10,
    ).strip()
    if version != SDK_VERSION:
        raise RuntimeError(f"dbos {SDK_VERSION} required, found {version}")


@contextmanager
def postgres_cluster(pg_bin, temp, environment):
    validate_postgres_bin(pg_bin, environment)
    cluster, socket_dir = temp / "cluster", temp / "socket"
    socket_dir.mkdir(mode=0o700)
    subprocess.run(
        [str(pg_bin / "initdb"), "-D", str(cluster), "--username=gate",
         "--auth-local=trust", "--auth-host=reject", "--encoding=UTF8", "--locale=C", "--no-sync"],
        env=environment, stdout=subprocess.DEVNULL, check=True, timeout=30,
    )
    port = reserve_loopback_port()
    with (temp / "postgres.log").open("w") as log:
        postgres = subprocess.Popen(
            [str(pg_bin / "postgres"), "-D", str(cluster), "-h", "", "-k", str(socket_dir), "-p", str(port)],
            env=environment, stdout=log, stderr=subprocess.STDOUT,
        )
        try:
            def ready():
                assert postgres.poll() is None, "Postgres exited before readiness"
                return subprocess.run(
                    [str(pg_bin / "pg_isready"), "-h", str(socket_dir), "-p", str(port), "-U", "gate", "-d", "postgres"],
                    env=environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3,
                ).returncode == 0

            wait_for(ready, time.monotonic() + 25, "Postgres 18")
            yield socket_dir, port
        finally:
            stop_process(postgres, label="Postgres")


def reserve_loopback_port():
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        return reserved.getsockname()[1]


def wait_for(check, deadline, label):
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError) as exc:
            last = type(exc).__name__
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {label} ({last})")


def request_json_response(base, path, *, method="GET", payload=None):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        base + path, data=data, headers=headers, method=method
    )
    try:
        with HTTP.open(request, timeout=3) as response:
            return (
                response.status,
                response.headers.get_content_type(),
                json.load(response),
            )
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers.get_content_type(), json.load(exc)


def request_json(base, path):
    status, _, payload = request_json_response(base, path)
    return status, payload


WORKFLOW_HTTP_TO_WIRE = {
    "workflowId": "WorkflowUUID",
    "status": "Status",
    "workflowName": "WorkflowName",
    "workflowClass": "WorkflowClassName",
    "workflowConfig": "WorkflowConfigName",
    "user": "AuthenticatedUser",
    "assumedRole": "AssumedRole",
    "roles": "AuthenticatedRoles",
    "input": "Input",
    "output": "Output",
    "error": "Error",
    "queueName": "QueueName",
    "appVersion": "ApplicationVersion",
    "executorId": "ExecutorID",
    "deduplicationId": "DeduplicationID",
    "queuePartitionKey": "QueuePartitionKey",
    "forkedFrom": "ForkedFrom",
    "wasForkedFrom": "WasForkedFrom",
    "parentWorkflowId": "ParentWorkflowID",
    "attributes": "Attributes",
    "scheduleName": "ScheduleName",
    "applicationName": "ApplicationName",
}
WORKFLOW_HTTP_TIME_TO_WIRE = {
    "createdAt": "CreatedAt",
    "updatedAt": "UpdatedAt",
    "deadline": "WorkflowDeadlineEpochMS",
    "dequeuedAt": "DequeuedAt",
    "delayUntil": "DelayUntilEpochMS",
    "completedAt": "CompletedAt",
}
WORKFLOW_HTTP_NUMBER_TO_WIRE = {
    "priority": "Priority",
    "timeoutMs": "WorkflowTimeoutMS",
}


def _workflow_time_epoch_ms(value):
    if value is None:
        return None
    assert _is_rfc3339(value), "Official Workflow timestamp violates generated schema"
    parsed = datetime.datetime.fromisoformat(
        value[:-1] + "+00:00" if value.endswith("Z") else value
    )
    utc = parsed.astimezone(datetime.timezone.utc)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    delta = utc - epoch
    assert delta.microseconds % 1000 == 0, (
        "Official workflow field values differ from SDK WorkflowsOutput"
    )
    return str(
        (delta.days * 24 * 60 * 60 + delta.seconds) * 1000 + delta.microseconds // 1000
    )


def validate_official_workflow(workflow, expected_sdk_digest, *, schemas):
    """Validate generated response fields and SDK values."""
    schema = schemas["Workflow"]
    required = set(schema["required"])
    properties = schema["properties"]
    assert isinstance(workflow, dict), "Official Workflow response must be an object"
    assert required <= set(workflow) <= set(properties), (
        "Official Workflow fields differ from generated schema"
    )
    for field, value in workflow.items():
        allowed = properties[field]["type"]
        if isinstance(allowed, str):
            allowed = [allowed]
        assert _matches_json_type(value, allowed), (
            f"Official Workflow.{field} violates generated schema"
        )
        field_format = properties[field].get("format")
        if field_format == "date-time" and value is not None:
            assert _is_rfc3339(value), (
                f"Official Workflow.{field} violates generated schema"
            )
        if field_format == "int32" and value is not None:
            assert -(2**31) <= value < 2**31, (
                f"Official Workflow.{field} violates generated schema"
            )
        if field_format == "int64" and value is not None:
            assert -(2**63) <= value < 2**63, (
                f"Official Workflow.{field} violates generated schema"
            )

    wire = {
        wire_field: workflow[http_field]
        for http_field, wire_field in WORKFLOW_HTTP_TO_WIRE.items()
    }
    for http_field, wire_field in WORKFLOW_HTTP_TIME_TO_WIRE.items():
        wire[wire_field] = _workflow_time_epoch_ms(workflow[http_field])
    for http_field, wire_field in WORKFLOW_HTTP_NUMBER_TO_WIRE.items():
        value = workflow[http_field]
        wire[wire_field] = str(value) if value is not None else None
    actual_digest = _sdk_wire_digest(wire)
    assert actual_digest == expected_sdk_digest, (
        "Official workflow field values differ from SDK WorkflowsOutput"
    )


QUEUE_HTTP_TO_WIRE = {
    "name": "name",
    "concurrency": "concurrency",
    "workerConcurrency": "worker_concurrency",
    "rateLimitMax": "rate_limit_max",
    "rateLimitPeriodSecs": "rate_limit_period_sec",
    "priorityEnabled": "priority_enabled",
    "partitionQueue": "partition_queue",
    "pollingIntervalSecs": "polling_interval_sec",
    "applicationName": "application_name",
    "partitionConcurrency": "partition_concurrency",
    "partitionWorkerConcurrency": "partition_worker_concurrency",
    "partitionRateLimitMax": "partition_rate_limit_max",
    "partitionRateLimitPeriodSecs": "partition_rate_limit_period_sec",
}


def _matches_json_type(value, allowed):
    if value is None:
        return "null" in allowed
    if isinstance(value, bool):
        return "boolean" in allowed
    if isinstance(value, int):
        return "integer" in allowed or "number" in allowed
    if isinstance(value, float):
        return "number" in allowed and math.isfinite(value)
    if isinstance(value, str):
        return "string" in allowed
    return False


def validate_official_queue(queue, expected_sdk_digest, *, schemas):
    """Validate one official Queue response against generated OpenAPI and SDK wire."""
    schema = schemas["Queue"]
    required = set(schema["required"])
    properties = schema["properties"]
    assert isinstance(queue, dict), "Official Queue response must be an object"
    assert required <= set(queue) <= set(properties), (
        "Official Queue fields differ from generated schema"
    )
    for field, value in queue.items():
        allowed = properties[field]["type"]
        if isinstance(allowed, str):
            allowed = [allowed]
        assert _matches_json_type(value, allowed), (
            f"Official Queue.{field} violates generated schema"
        )
        field_format = properties[field].get("format")
        if field_format == "int32" and value is not None:
            assert -(2**31) <= value < 2**31, (
                f"Official Queue.{field} violates generated schema"
            )
        if field_format == "double" and value is not None:
            assert isinstance(value, (int, float)) and not isinstance(value, bool), (
                f"Official Queue.{field} violates generated schema"
            )
            assert math.isfinite(value), (
                f"Official Queue.{field} violates generated schema"
            )

    wire = {
        wire_field: queue[http_field]
        for http_field, wire_field in QUEUE_HTTP_TO_WIRE.items()
    }
    encoded = json.dumps(
        wire, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    actual_digest = hashlib.sha256(encoded).hexdigest()
    assert actual_digest == expected_sdk_digest, (
        "Official queue field values differ from SDK QueueOutput"
    )


SCHEDULE_HTTP_TO_WIRE = {
    "scheduleId": "schedule_id",
    "scheduleName": "schedule_name",
    "workflowName": "workflow_name",
    "workflowClass": "workflow_class_name",
    "cronExpression": "schedule",
    "status": "status",
    "context": "context",
    "lastFiredAt": "last_fired_at",
    "automaticBackfill": "automatic_backfill",
    "cronTimezone": "cron_timezone",
    "applicationName": "application_name",
}


def _is_rfc3339(value):
    if (
        not isinstance(value, str)
        or re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})",
            value,
        )
        is None
    ):
        return False
    try:
        parsed = datetime.datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except ValueError:
        return False
    return parsed.tzinfo is not None


def validate_official_schedule(schedule, expected_sdk_digest, *, schemas):
    """Validate one official Schedule response against generated OpenAPI and SDK wire."""
    schema = schemas["Schedule"]
    required = set(schema["required"])
    properties = schema["properties"]
    assert isinstance(schedule, dict), "Official Schedule response must be an object"
    assert required <= set(schedule) <= set(properties), (
        "Official Schedule fields differ from generated schema"
    )
    for field, value in schedule.items():
        allowed = properties[field]["type"]
        if isinstance(allowed, str):
            allowed = [allowed]
        assert _matches_json_type(value, allowed), (
            f"Official Schedule.{field} violates generated schema"
        )
        if properties[field].get("format") == "date-time" and value is not None:
            assert _is_rfc3339(value), (
                f"Official Schedule.{field} violates generated schema"
            )

    wire = {
        wire_field: schedule[http_field]
        for http_field, wire_field in SCHEDULE_HTTP_TO_WIRE.items()
    }
    encoded = json.dumps(
        wire, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    actual_digest = hashlib.sha256(encoded).hexdigest()
    assert actual_digest == expected_sdk_digest, (
        "Official schedule field values differ from SDK ScheduleOutput"
    )


def _validate_official_related(record, schema_name, *, schemas):
    schema = schemas[schema_name]
    required = set(schema["required"])
    properties = schema["properties"]
    assert isinstance(record, dict), (
        f"Official {schema_name} response must be an object"
    )
    assert required <= set(record) <= set(properties), (
        f"Official {schema_name} fields differ from generated schema"
    )
    for field, value in record.items():
        property_schema = properties[field]
        allowed = property_schema["type"]
        if isinstance(allowed, str):
            allowed = [allowed]
        if "array" in allowed:
            valid = isinstance(value, list)
            if valid:
                item_type = property_schema["items"]["type"]
                valid = all(_matches_json_type(item, [item_type]) for item in value)
        else:
            valid = _matches_json_type(value, allowed)
        assert valid, f"Official {schema_name}.{field} violates generated schema"
        if property_schema.get("format") == "date-time" and value is not None:
            assert _is_rfc3339(value), (
                f"Official {schema_name}.{field} violates generated schema"
            )


def _sdk_wire_digest(wire):
    encoded = json.dumps(
        wire, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def validate_official_event(event, expected_sdk_digest, *, schemas):
    _validate_official_related(event, "Event", schemas=schemas)
    assert _sdk_wire_digest(event) == expected_sdk_digest, (
        "Official event field values differ from SDK EventOutput"
    )


def _notification_created_at_epoch_ms(created_at):
    parsed = datetime.datetime.fromisoformat(
        created_at[:-1] + "+00:00" if created_at.endswith("Z") else created_at
    )
    utc = parsed.astimezone(datetime.timezone.utc)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    delta = utc - epoch
    assert delta.microseconds % 1000 == 0, (
        "Official notification field values differ from SDK NotificationOutput"
    )
    return (
        delta.days * 24 * 60 * 60 + delta.seconds
    ) * 1000 + delta.microseconds // 1000


def validate_official_notification(notification, expected_sdk_digest, *, schemas):
    _validate_official_related(notification, "Notification", schemas=schemas)
    wire = {
        "topic": notification["topic"],
        "message": notification["message"],
        "created_at_epoch_ms": _notification_created_at_epoch_ms(
            notification["createdAt"]
        ),
        "consumed": notification["consumed"],
    }
    assert _sdk_wire_digest(wire) == expected_sdk_digest, (
        "Official notification field values differ from SDK NotificationOutput"
    )


def validate_official_stream(stream, expected_sdk_digest, *, schemas):
    _validate_official_related(stream, "StreamEntry", schemas=schemas)
    assert _sdk_wire_digest(stream) == expected_sdk_digest, (
        "Official stream field values differ from SDK StreamEntryOutput"
    )


def _validate_official_aggregate(record, schema_name, *, schemas):
    schema = schemas[schema_name]
    required = set(schema["required"])
    properties = schema["properties"]
    assert isinstance(record, dict), (
        f"Official {schema_name} response must be an object"
    )
    assert required <= set(record) <= set(properties), (
        f"Official {schema_name} fields differ from generated schema"
    )
    group = record.get("group")
    assert isinstance(group, dict), (
        f"Official {schema_name}.group violates generated schema"
    )
    assert all(
        isinstance(key, str) and (value is None or isinstance(value, str))
        for key, value in group.items()
    ), f"Official {schema_name}.group violates generated schema"
    for field, value in record.items():
        if field == "group":
            continue
        property_schema = properties[field]
        allowed = property_schema["type"]
        if isinstance(allowed, str):
            allowed = [allowed]
        assert _matches_json_type(value, allowed), (
            f"Official {schema_name}.{field} violates generated schema"
        )
        if property_schema.get("format") == "date-time":
            assert _is_rfc3339(value), (
                f"Official {schema_name}.{field} violates generated schema"
            )


def _aggregate_created_at_epoch_ms(created_at):
    parsed = datetime.datetime.fromisoformat(
        created_at[:-1] + "+00:00" if created_at.endswith("Z") else created_at
    )
    utc = parsed.astimezone(datetime.timezone.utc)
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    delta = utc - epoch
    assert delta.microseconds % 1000 == 0, (
        "Official workflow aggregate field values differ from SDK WorkflowAggregateOutput"
    )
    return (
        delta.days * 24 * 60 * 60 + delta.seconds
    ) * 1000 + delta.microseconds // 1000


def validate_official_workflow_aggregates(aggregates, expected_sdk_digest, *, schemas):
    assert isinstance(aggregates, list), (
        "Official WorkflowAggregate response must be an array"
    )
    wire = []
    for aggregate in aggregates:
        _validate_official_aggregate(aggregate, "WorkflowAggregate", schemas=schemas)
        wire.append(
            {
                "group": aggregate["group"],
                "count": aggregate.get("count"),
                "min_created_at": (
                    _aggregate_created_at_epoch_ms(aggregate["minCreatedAt"])
                    if "minCreatedAt" in aggregate
                    else None
                ),
                "max_queue_wait_ms": aggregate.get("maxQueueWaitMs"),
                "max_total_latency_ms": aggregate.get("maxTotalLatencyMs"),
            }
        )
    assert _sdk_wire_digest(wire) == expected_sdk_digest, (
        "Official workflow aggregate field values differ from SDK WorkflowAggregateOutput"
    )


def validate_official_step_aggregates(aggregates, expected_sdk_digest, *, schemas):
    assert isinstance(aggregates, list), (
        "Official StepAggregate response must be an array"
    )
    wire = []
    for aggregate in aggregates:
        _validate_official_aggregate(aggregate, "StepAggregate", schemas=schemas)
        wire.append(
            {
                "group": aggregate["group"],
                "count": aggregate.get("count"),
                "max_duration_ms": aggregate.get("maxDurationMs"),
            }
        )
    assert _sdk_wire_digest(wire) == expected_sdk_digest, (
        "Official step aggregate field values differ from SDK StepAggregateOutput"
    )


def validate_official_export(exported, expected_sdk_digest, *, schemas):
    schema = schemas["ExportWorkflowOutputBody"]
    required = set(schema["required"])
    properties = schema["properties"]
    assert isinstance(exported, dict), "Official export response must be an object"
    assert required <= set(exported) <= set(properties), (
        "Official export fields differ from generated schema"
    )
    serialized = exported.get("serializedWorkflow")
    assert isinstance(serialized, str), (
        "Official export serializedWorkflow violates generated schema"
    )
    actual_digest = hashlib.sha256(serialized.encode()).hexdigest()
    assert actual_digest == expected_sdk_digest, (
        "Official export bytes differ from observed SDK export frame"
    )


def validate_problem_response(status, content_type, problem, expected_status, label):
    assert status == expected_status, f"{label} status"
    assert content_type == "application/problem+json", f"{label} content type"
    assert isinstance(problem, dict) and set(problem) == {
        "type",
        "title",
        "status",
        "detail",
    }, f"{label} problem fields"
    assert problem["type"] == "about:blank", f"{label} problem type"
    assert problem["status"] == expected_status, f"{label} problem status"
    assert isinstance(problem["title"], str) and problem["title"], (
        f"{label} problem title"
    )
    assert isinstance(problem["detail"], str) and problem["detail"], (
        f"{label} problem detail"
    )


def stop_process(proc, timeout=10, *, terminate=True, label="owned child"):
    if terminate and proc.poll() is None:
        proc.terminate()
    try:
        code = proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        proc.kill()
        proc.wait(timeout=3)
        raise AssertionError(
            f"{label} did not terminate within its cleanup deadline"
        ) from exc
    if code != 0:
        raise AssertionError(f"{label} exited with status {code}")


def create_database(pg_bin, socket_dir, pg_port, database, environment):
    result = subprocess.run(
        [
            str(pg_bin / "createdb"),
            "--host",
            str(socket_dir),
            "--port",
            str(pg_port),
            "--username",
            "gate",
            database,
        ],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=10,
    )
    if result.returncode != 0:
        raise AssertionError("owned Postgres database creation failed")


def run_case(
    python,
    binary,
    temp,
    database_url,
    metadata_only,
    environment,
):
    temp.mkdir(mode=0o700)
    port = reserve_loopback_port()
    base = f"http://127.0.0.1:{port}"
    token = secrets.token_hex(8)
    key = secrets.token_urlsafe(24)
    app = "postgres-gate-" + token
    executor = "postgres-executor-" + token
    ready_path = temp / "ready.json"
    export_digest_path = temp / "export-frame.sha256"
    case_env = dict(environment)
    case_env.update(
        HOME=str(temp),
        TMPDIR=str(temp),
        POSTGRES_GATE_APP=app,
        POSTGRES_GATE_EXECUTOR=executor,
        POSTGRES_GATE_DATABASE_URL=database_url,
        POSTGRES_GATE_READY=str(ready_path),
        POSTGRES_GATE_EXPORT_DIGEST=str(export_digest_path),
        # Deliberately named as a test-only seam; never consumed as application setup.
        POSTGRES_GATE_TEST_WS=base.replace("http:", "ws:"),
        POSTGRES_GATE_KEY=key,
        POSTGRES_GATE_METADATA_ONLY="1" if metadata_only else "0",
    )
    if environment.get("POSTGRES_GATE_CONSOLE") == "1" and not metadata_only:
        case_env["POSTGRES_GATE_CONSOLE"] = "1"
    else:
        case_env.pop("POSTGRES_GATE_CONSOLE", None)
    with (
        (temp / "maestro.log").open("w+") as maestro_log,
        (temp / "sdk.log").open("w+") as sdk_log,
    ):
        server = subprocess.Popen(
            [
                str(binary),
                "--listen",
                f"127.0.0.1:{port}",
                "--enable-aggregates",
            ],
            cwd=ROOT,
            env=case_env,
            stdout=maestro_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        app_process = None
        try:
            deadline = time.monotonic() + CASE_DEADLINE_SECONDS

            def healthy():
                assert server.poll() is None, "maestro exited before readiness"
                return request_json(base, "/healthz") == (200, {"status": True})

            wait_for(healthy, deadline, "maestro health")
            app_process = subprocess.Popen(
                [str(python), "-I", str(ROOT / "dev/tests/acceptance/postgres_app.py")],
                cwd=temp,
                env=case_env,
                stdin=subprocess.PIPE,
                stdout=sdk_log,
                stderr=subprocess.STDOUT,
                text=True,
            )
            def app_ready():
                assert app_process.poll() is None, "SDK app exited before readiness"
                return (
                    json.loads(ready_path.read_text()) if ready_path.is_file() else None
                )

            ready = wait_for(app_ready, deadline, "SDK workflow on Postgres")
            workflow_id = ready["workflow_id"]
            empty_related_workflow_id = ready["empty_related_workflow_id"]
            notification_workflow_id = ready["notification_workflow_id"]

            def connected():
                status, peers = request_json(base, "/api/executors")
                assert status == 200, "executor endpoint failed"
                return peers[0] if isinstance(peers, list) and len(peers) == 1 else None

            peer = wait_for(connected, deadline, "released SDK handshake")
            status, specification = request_json(base, "/openapi.json")
            assert status == 200, "generated OpenAPI endpoint failed"
            schemas = specification["components"]["schemas"]
            assert peer["app"] == app, "SDK handshake application identity"
            assert (
                peer["language"] == "python" and peer["dbos_version"] == SDK_VERSION
            ), "SDK handshake version"
            assert peer["executor_metadata"] == {"gate": "postgres18-read-smoke"}, (
                "SDK handshake metadata"
            )

            workflow_read_digests = ready["workflow_read_sha256"]
            workflow_read_without_blobs_digests = ready[
                "workflow_read_without_blobs_sha256"
            ]
            direct_workflow_digest = ready["workflow_sha256"]
            direct_workflow_without_blobs_digest = ready[
                "workflow_without_blobs_sha256"
            ]
            workflow_read_executor_id = ready["workflow_read_executor_id"]
            assert (
                isinstance(workflow_read_executor_id, str)
                and workflow_read_executor_id
                and peer["executor_id"] == workflow_read_executor_id
            ), "SDK workflow-read executor identity"
            assert (
                isinstance(workflow_read_digests, dict)
                and len(workflow_read_digests) == 2
                and set(workflow_read_digests)
                == set(workflow_read_without_blobs_digests)
                and all(
                    isinstance(digest, str) and len(digest) == 64
                    for digest in (
                        list(workflow_read_digests.values())
                        + list(workflow_read_without_blobs_digests.values())
                        + [
                            direct_workflow_digest,
                            direct_workflow_without_blobs_digest,
                        ]
                    )
                )
            ), "SDK workflow-read digest manifest"
            workflow_read_ids = list(workflow_read_digests)
            selected_workflow_read_id = workflow_read_ids[0]
            expected_workflow_read_digests = (
                workflow_read_without_blobs_digests
                if metadata_only
                else workflow_read_digests
            )
            official_workflows_root = (
                "/v2/orgs/local/apps/" + urllib.parse.quote(app, safe="") + "/workflows"
            )

            get_list_query = urllib.parse.urlencode(
                {
                    "workflowName": "gate_workflow_read_fixture",
                    "loadInput": "true",
                    "loadOutput": "true",
                }
            )
            workflow_status, content_type, official_workflows = request_json_response(
                base, official_workflows_root + "?" + get_list_query
            )
            assert (
                workflow_status == 200
                and content_type == "application/json"
                and isinstance(official_workflows, list)
                and {row.get("workflowId") for row in official_workflows}
                == set(workflow_read_ids)
            ), "official workflow GET list response"
            for workflow in official_workflows:
                validate_official_workflow(
                    workflow,
                    expected_workflow_read_digests[workflow["workflowId"]],
                    schemas=schemas,
                )
                assert workflow["priority"] == 7, (
                    "official workflow genuine queued priority"
                )
                assert workflow["queueName"] == "gate-queue", (
                    "official workflow genuine queue metadata"
                )
                assert workflow["dequeuedAt"] is not None, (
                    "official workflow genuine dequeue metadata"
                )
                if metadata_only:
                    assert workflow["input"] is None and workflow["output"] is None, (
                        "metadata-only workflow list exposed blobs"
                    )
                else:
                    assert (
                        isinstance(workflow["input"], str)
                        and workflow["input"]
                        and isinstance(workflow["output"], str)
                        and workflow["output"]
                    ), "official workflow list omitted requested blobs"

            ascending_created = [row["createdAt"] for row in official_workflows]
            assert ascending_created == sorted(ascending_created), (
                "official workflow GET default ascending order"
            )
            descending_query = urllib.parse.urlencode(
                {
                    "workflowName": "gate_workflow_read_fixture",
                    "sortDesc": "true",
                    "limit": 1,
                    "offset": 0,
                }
            )
            workflow_status, _, descending_page = request_json_response(
                base, official_workflows_root + "?" + descending_query
            )
            assert (
                workflow_status == 200
                and len(descending_page) == 1
                and descending_page[0]["createdAt"] == max(ascending_created)
            ), "official workflow GET sorting and first page"
            validate_official_workflow(
                descending_page[0],
                workflow_read_without_blobs_digests[descending_page[0]["workflowId"]],
                schemas=schemas,
            )
            exhausted_query = urllib.parse.urlencode(
                {
                    "workflowName": "gate_workflow_read_fixture",
                    "limit": 1,
                    "offset": 2,
                }
            )
            workflow_status, _, exhausted_page = request_json_response(
                base, official_workflows_root + "?" + exhausted_query
            )
            assert workflow_status == 200 and exhausted_page == [], (
                "official workflow GET exhausted page"
            )

            workflow_search_path = official_workflows_root + "/search"
            workflow_search_body = {
                "workflowIds": [selected_workflow_read_id],
                "workflowName": ["gate_workflow_read_fixture"],
                "status": ["SUCCESS"],
                "appVersion": ["postgres-gate-v1"],
                "queueName": ["gate-queue"],
                "executorId": [workflow_read_executor_id],
                "workflowIdPrefix": [selected_workflow_read_id[:8]],
                "attributes": {"tenant": "workflow-read"},
                "startTime": "2000-01-01T00:00:00+00:00",
                "endTime": "2100-01-01T00:00:00+00:00",
                "completedAfter": "2000-01-01T00:00:00+00:00",
                "completedBefore": "2100-01-01T00:00:00+00:00",
                "dequeuedAfter": "2000-01-01T00:00:00+00:00",
                "dequeuedBefore": "2100-01-01T00:00:00+00:00",
                "hasParent": False,
                "wasForkedFrom": False,
                "queuesOnly": True,
                "loadInput": True,
                "loadOutput": True,
                "sortDesc": True,
                "limit": 2,
                "offset": 0,
            }
            workflow_status, content_type, searched_workflows = request_json_response(
                base,
                workflow_search_path,
                method="POST",
                payload=workflow_search_body,
            )
            assert (
                workflow_status == 200
                and content_type == "application/json"
                and isinstance(searched_workflows, list)
                and len(searched_workflows) == 1
                and searched_workflows[0]["workflowId"] == selected_workflow_read_id
            ), "official expanded workflow search"
            validate_official_workflow(
                searched_workflows[0],
                expected_workflow_read_digests[selected_workflow_read_id],
                schemas=schemas,
            )

            workflow_status, _, default_search = request_json_response(
                base,
                workflow_search_path,
                method="POST",
                payload={"workflowIds": [selected_workflow_read_id]},
            )
            assert (
                workflow_status == 200
                and len(default_search) == 1
                and default_search[0]["input"] is None
                and default_search[0]["output"] is None
            ), "official workflow search default blob loading"
            validate_official_workflow(
                default_search[0],
                workflow_read_without_blobs_digests[selected_workflow_read_id],
                schemas=schemas,
            )

            for field, value in (
                ("user", ["missing-user"]),
                ("forkedFrom", ["missing-fork"]),
                ("parentWorkflowId", ["missing-parent"]),
                ("scheduleName", ["missing-schedule"]),
                ("status", ["NO_MATCHING_STATUS"]),
                ("attributes", {"tenant": "missing-tenant"}),
                ("hasParent", True),
                ("wasForkedFrom", True),
            ):
                workflow_status, content_type, no_workflows = request_json_response(
                    base,
                    workflow_search_path,
                    method="POST",
                    payload={
                        "workflowIds": [selected_workflow_read_id],
                        field: value,
                    },
                )
                assert (
                    workflow_status == 200
                    and content_type == "application/json"
                    and no_workflows == []
                ), f"official workflow negative {field} filter"

            workflow_status, content_type, official_workflow = request_json_response(
                base,
                official_workflows_root
                + "/"
                + urllib.parse.quote(selected_workflow_read_id, safe=""),
            )
            assert (
                workflow_status == 200
                and content_type == "application/json"
                and official_workflow["workflowId"] == selected_workflow_read_id
            ), "official workflow get response"
            validate_official_workflow(
                official_workflow,
                expected_workflow_read_digests[selected_workflow_read_id],
                schemas=schemas,
            )

            workflow_status, content_type, problem = request_json_response(
                base, official_workflows_root + "/missing-workflow"
            )
            validate_problem_response(
                workflow_status,
                content_type,
                problem,
                404,
                "official missing workflow get",
            )
            workflow_status, content_type, direct_workflow = request_json_response(
                base,
                official_workflows_root
                + "/"
                + urllib.parse.quote(workflow_id, safe=""),
            )
            assert workflow_status == 200, "SDK nullable workflow must remain readable"
            assert content_type == "application/json", (
                "official direct-workflow content type"
            )
            validate_official_workflow(
                direct_workflow,
                (
                    direct_workflow_without_blobs_digest
                    if metadata_only
                    else direct_workflow_digest
                ),
                schemas=schemas,
            )
            for path, method, payload, label in (
                (
                    official_workflows_root + "?status=SUCCESS&status=ERROR",
                    "GET",
                    None,
                    "official workflow duplicate GET scalar",
                ),
                (
                    official_workflows_root + "?queueName=gate-queue",
                    "GET",
                    None,
                    "official workflow unsupported GET query",
                ),
                (
                    workflow_search_path + "?limit=1",
                    "POST",
                    {},
                    "official workflow search query",
                ),
                (
                    workflow_search_path,
                    "POST",
                    {"applicationName": [app]},
                    "official workflow unknown search field",
                ),
            ):
                workflow_status, content_type, problem = request_json_response(
                    base, path, method=method, payload=payload
                )
                validate_problem_response(
                    workflow_status, content_type, problem, 400, label
                )

            prefix = "/api/" + urllib.parse.quote(app, safe="")
            workflow_path = (
                prefix + "/workflows/" + urllib.parse.quote(workflow_id, safe="")
            )
            status, workflows = request_json(
                base,
                prefix
                + "/workflows?"
                + urllib.parse.urlencode({"id_prefix": workflow_id}),
            )
            assert status == 200 and any(
                row["WorkflowUUID"] == workflow_id for row in workflows
            ), "Postgres-backed workflow list"
            listed = next(
                row for row in workflows if row["WorkflowUUID"] == workflow_id
            )
            assert listed["Input"] is None and listed["Output"] is None, (
                "workflow list must suppress opaque blobs"
            )

            status, detail = request_json(base, workflow_path)
            assert status == 200 and detail["WorkflowUUID"] == workflow_id, (
                "Postgres-backed workflow get"
            )
            assert detail["Status"] == "SUCCESS", "Postgres-backed workflow status"
            status, steps = request_json(base, workflow_path + "/steps")
            assert status == 200, "Postgres-backed workflow steps"
            step = next(
                (row for row in steps if row["function_name"] == "gate_step"), None
            )
            assert step is not None, "SDK step missing from maestro response"
            status, queues = request_json(base, prefix + "/queues")
            assert status == 200 and any(
                row["name"] == "gate-queue" for row in queues
            ), "released SDK queue read"
            queue = next(row for row in queues if row["name"] == "gate-queue")
            assert queue["concurrency"] == 3 and queue["worker_concurrency"] == 2, (
                "released SDK queue fields"
            )

            queue_digests = ready["queue_sha256"]
            assert isinstance(queue_digests, dict) and set(queue_digests) == {
                "gate-queue",
                "gate-edge-queue",
            }, "SDK queue digest manifest"
            official_root = (
                "/v2/orgs/local/apps/" + urllib.parse.quote(app, safe="") + "/queues"
            )
            status, content_type, official_queues = request_json_response(
                base, official_root
            )
            assert (
                status == 200
                and content_type == "application/json"
                and isinstance(official_queues, list)
                and all(isinstance(row, dict) for row in official_queues)
            ), "official queue list response"
            assert {row.get("name") for row in official_queues} == set(queue_digests), (
                "official queue list matches SDK registrations"
            )
            official_by_name = {row["name"]: row for row in official_queues}
            for name, digest in queue_digests.items():
                validate_official_queue(official_by_name[name], digest, schemas=schemas)

            official_gate_queue = official_by_name["gate-queue"]
            assert (
                official_gate_queue["rateLimitPeriodSecs"] == 1.5
                and official_gate_queue["partitionRateLimitPeriodSecs"] == 2.5
                and official_gate_queue["pollingIntervalSecs"] == 0.25
            ), "official queue fractional fields were not preserved"
            official_edge_queue = official_by_name["gate-edge-queue"]
            assert (
                official_edge_queue["concurrency"] == 0
                and official_edge_queue["rateLimitMax"] == 0
                and official_edge_queue["workerConcurrency"] is None
                and official_edge_queue["partitionConcurrency"] is None
                and official_edge_queue["partitionQueue"] is False
                and official_edge_queue["pollingIntervalSecs"] == 0.125
            ), "official queue null/fractional/false/zero fields were not preserved"

            status, content_type, official_queue = request_json_response(
                base, official_root + "/gate-queue"
            )
            assert (
                status == 200
                and content_type == "application/json"
                and isinstance(official_queue, dict)
            ), "official queue get response"
            validate_official_queue(
                official_queue, queue_digests["gate-queue"], schemas=schemas
            )

            status, content_type, problem = request_json_response(
                base, official_root + "/missing-queue"
            )
            validate_problem_response(
                status,
                content_type,
                problem,
                404,
                "official missing queue",
            )
            status, content_type, problem = request_json_response(
                base, official_root + "?applicationName=other"
            )
            validate_problem_response(
                status,
                content_type,
                problem,
                400,
                "official queue unsupported query",
            )

            schedule_digests = ready["schedule_sha256"]
            schedule_names = {
                "gate-schedule-context",
                "gate-schedule-null",
            }
            assert (
                isinstance(schedule_digests, dict)
                and set(schedule_digests) == schedule_names
                and all(
                    isinstance(digests, dict)
                    and set(digests) == {"context", "without_context"}
                    for digests in schedule_digests.values()
                )
            ), "SDK schedule digest manifest"
            official_schedule_root = (
                "/v2/orgs/local/apps/" + urllib.parse.quote(app, safe="") + "/schedules"
            )
            status, content_type, official_schedules = request_json_response(
                base, official_schedule_root
            )
            assert (
                status == 200
                and content_type == "application/json"
                and isinstance(official_schedules, list)
                and all(isinstance(row, dict) for row in official_schedules)
            ), "official schedule list response"
            assert {
                row.get("scheduleName") for row in official_schedules
            } == schedule_names, "official schedule list matches SDK registrations"
            official_schedules_by_name = {
                row["scheduleName"]: row for row in official_schedules
            }
            digest_label = "without_context" if metadata_only else "context"
            for name, digests in schedule_digests.items():
                validate_official_schedule(
                    official_schedules_by_name[name],
                    digests[digest_label],
                    schemas=schemas,
                )

            schedule_with_context = official_schedules_by_name["gate-schedule-context"]
            schedule_with_null = official_schedules_by_name["gate-schedule-null"]
            assert (
                schedule_with_context["automaticBackfill"] is False
                and schedule_with_context["cronTimezone"] == "UTC"
                and schedule_with_context["workflowClass"] is None
                and schedule_with_context["lastFiredAt"] is None
                and schedule_with_null["automaticBackfill"] is False
                and schedule_with_null["cronTimezone"] is None
            ), "official schedule nullable/false fields were not preserved"
            if metadata_only:
                assert (
                    schedule_with_context["context"] is None
                    and schedule_with_null["context"] is None
                ), "metadata-only schedule context was exposed"
            else:
                # The released handler applies str() after SDK deserialization,
                # so even a schedule created with context=None is an opaque
                # nonempty string when context loading is enabled.
                assert (
                    isinstance(schedule_with_context["context"], str)
                    and schedule_with_context["context"]
                    and isinstance(schedule_with_null["context"], str)
                    and schedule_with_null["context"]
                ), "official schedule context missing"

            for name, digests in schedule_digests.items():
                status, content_type, official_schedule = request_json_response(
                    base,
                    official_schedule_root + "/" + urllib.parse.quote(name, safe=""),
                )
                assert (
                    status == 200
                    and content_type == "application/json"
                    and isinstance(official_schedule, dict)
                ), "official schedule get response"
                validate_official_schedule(
                    official_schedule, digests[digest_label], schemas=schemas
                )

            filter_query = urllib.parse.urlencode(
                {
                    "status": "ACTIVE",
                    "workflowName": "gate_scheduled",
                    "scheduleNamePrefix": "gate-schedule-",
                    "loadContext": "false",
                }
            )
            status, content_type, filtered_schedules = request_json_response(
                base, official_schedule_root + "?" + filter_query
            )
            assert (
                status == 200
                and content_type == "application/json"
                and isinstance(filtered_schedules, list)
                and {row.get("scheduleName") for row in filtered_schedules}
                == schedule_names
            ), "official schedule filters"
            for schedule in filtered_schedules:
                assert schedule["context"] is None, (
                    "official loadContext=false exposed context"
                )
                validate_official_schedule(
                    schedule,
                    schedule_digests[schedule["scheduleName"]]["without_context"],
                    schemas=schemas,
                )

            empty_query = urllib.parse.urlencode(
                {"scheduleNamePrefix": "missing-schedule-prefix"}
            )
            status, content_type, empty_schedules = request_json_response(
                base, official_schedule_root + "?" + empty_query
            )
            assert (
                status == 200
                and content_type == "application/json"
                and empty_schedules == []
            ), "official empty schedule list"

            status, content_type, problem = request_json_response(
                base, official_schedule_root + "/missing-schedule"
            )
            validate_problem_response(
                status,
                content_type,
                problem,
                404,
                "official missing schedule",
            )
            for query, label in (
                ("applicationName=other", "official schedule unknown query"),
                (
                    "status=ACTIVE&status=PAUSED",
                    "official schedule repeated query",
                ),
            ):
                status, content_type, problem = request_json_response(
                    base, official_schedule_root + "?" + query
                )
                validate_problem_response(
                    status,
                    content_type,
                    problem,
                    400,
                    label,
                )
            status, content_type, problem = request_json_response(
                base,
                official_schedule_root + "/gate-schedule-context?loadContext=false",
            )
            validate_problem_response(
                status,
                content_type,
                problem,
                400,
                "official schedule get unsupported query",
            )

            event_digests = ready["event_sha256"]
            notification_digests = ready["notification_sha256"]
            stream_digests = ready["stream_sha256"]
            assert isinstance(event_digests, dict) and set(event_digests) == {
                "gate-event"
            }, "SDK event digest manifest"
            assert isinstance(notification_digests, dict) and set(
                notification_digests
            ) == {"null-topic", "empty-topic"}, "SDK notification digest manifest"
            assert isinstance(stream_digests, dict) and set(stream_digests) == {
                "gate-stream",
                "gate-empty-stream",
            }, "SDK stream digest manifest"

            official_workflow_root = (
                "/v2/orgs/local/apps/"
                + urllib.parse.quote(app, safe="")
                + "/workflows/"
            )
            official_related_paths = {
                "events": (
                    official_workflow_root
                    + urllib.parse.quote(workflow_id, safe="")
                    + "/events"
                ),
                "notifications": (
                    official_workflow_root
                    + urllib.parse.quote(notification_workflow_id, safe="")
                    + "/notifications"
                ),
                "streams": (
                    official_workflow_root
                    + urllib.parse.quote(workflow_id, safe="")
                    + "/streams"
                ),
            }

            if metadata_only:
                for related_name, related_path in official_related_paths.items():
                    related_status, content_type, problem = request_json_response(
                        base, related_path
                    )
                    validate_problem_response(
                        related_status,
                        content_type,
                        problem,
                        502,
                        f"official metadata-only {related_name} refusal",
                    )
                    assert "metadata-only mode" in problem["detail"], (
                        f"official metadata-only {related_name} refusal detail"
                    )
            else:
                related_status, content_type, official_events = request_json_response(
                    base, official_related_paths["events"]
                )
                assert (
                    related_status == 200
                    and content_type == "application/json"
                    and isinstance(official_events, list)
                    and len(official_events) == 1
                ), "official event response"
                assert {event.get("key") for event in official_events} == set(
                    event_digests
                ), "official event keys"
                for event in official_events:
                    validate_official_event(
                        event, event_digests[event["key"]], schemas=schemas
                    )

                related_status, content_type, official_notifications = (
                    request_json_response(base, official_related_paths["notifications"])
                )
                assert (
                    related_status == 200
                    and content_type == "application/json"
                    and isinstance(official_notifications, list)
                    and len(official_notifications) == 2
                    and all(
                        isinstance(notification, dict)
                        for notification in official_notifications
                    )
                ), "official notification response"
                seen_notification_labels = set()
                for notification in official_notifications:
                    if notification.get("topic") is None:
                        label = "null-topic"
                    else:
                        assert notification.get("topic") == "", (
                            "official notification empty topic"
                        )
                        label = "empty-topic"
                    seen_notification_labels.add(label)
                    assert notification.get("consumed") is False, (
                        "official pending notification consumed flag"
                    )
                    validate_official_notification(
                        notification, notification_digests[label], schemas=schemas
                    )
                assert seen_notification_labels == set(notification_digests), (
                    "official nullable/empty notification topics"
                )

                related_status, content_type, official_streams = request_json_response(
                    base, official_related_paths["streams"]
                )
                assert (
                    related_status == 200
                    and content_type == "application/json"
                    and isinstance(official_streams, list)
                    and len(official_streams) == 2
                    and all(isinstance(stream, dict) for stream in official_streams)
                ), "official stream response"
                assert {stream.get("key") for stream in official_streams} == set(
                    stream_digests
                ), "official stream keys"
                for stream in official_streams:
                    if stream["key"] == "gate-stream":
                        assert len(stream.get("values", [])) == 3, (
                            "official ordered stream values"
                        )
                    else:
                        assert stream.get("values") == [], (
                            "official empty stream values"
                        )
                    validate_official_stream(
                        stream, stream_digests[stream["key"]], schemas=schemas
                    )

                empty_related_root = official_workflow_root + urllib.parse.quote(
                    empty_related_workflow_id, safe=""
                )
                for related_name in ("events", "notifications", "streams"):
                    related_status, content_type, empty_related = request_json_response(
                        base, empty_related_root + "/" + related_name
                    )
                    assert (
                        related_status == 200
                        and content_type == "application/json"
                        and empty_related == []
                    ), f"official existing empty {related_name}"

            missing_related_root = official_workflow_root + "missing-workflow"
            for related_name in ("events", "notifications", "streams"):
                related_status, content_type, problem = request_json_response(
                    base, missing_related_root + "/" + related_name
                )
                validate_problem_response(
                    related_status,
                    content_type,
                    problem,
                    404,
                    f"official missing workflow {related_name}",
                )
                related_status, content_type, problem = request_json_response(
                    base, official_related_paths[related_name] + "?limit=0"
                )
                validate_problem_response(
                    related_status,
                    content_type,
                    problem,
                    400,
                    f"official {related_name} unsupported query",
                )

            workflow_aggregate_digest = ready["workflow_aggregate_sha256"]
            step_aggregate_digest = ready["step_aggregate_sha256"]
            assert (
                isinstance(workflow_aggregate_digest, str)
                and len(workflow_aggregate_digest) == 64
                and isinstance(step_aggregate_digest, str)
                and len(step_aggregate_digest) == 64
            ), "SDK aggregate digest manifest"

            official_app_root = "/v2/orgs/local/apps/" + urllib.parse.quote(
                app, safe=""
            )
            workflow_aggregate_path = official_app_root + "/workflows/aggregates"
            workflow_aggregate_body = {
                "groupByStatus": True,
                "groupByWorkflowName": True,
                "groupByExecutorId": True,
                "groupByAppVersion": True,
                "groupByApplicationName": True,
                "selectCount": True,
                "selectMinCreatedAt": True,
                "selectMaxQueueWaitMs": True,
                "selectMaxTotalLatencyMs": True,
                "timeBucketSizeMs": 60_000,
                "status": ["SUCCESS"],
                "startTime": "2000-01-01T00:00:00+00:00",
                "endTime": "2100-01-01T00:00:00+00:00",
                "completedAfter": "2000-01-01T00:00:00+00:00",
                "completedBefore": "2100-01-01T00:00:00+00:00",
                "workflowName": ["gate_workflow"],
                "appVersion": ["postgres-gate-v1"],
                "workflowIdPrefix": [workflow_id[:8]],
                "workflowIds": [workflow_id],
                "wasForkedFrom": False,
                "hasParent": False,
            }
            aggregate_status, content_type, workflow_aggregates = request_json_response(
                base,
                workflow_aggregate_path,
                method="POST",
                payload=workflow_aggregate_body,
            )
            assert (
                aggregate_status == 200
                and content_type == "application/json"
                and isinstance(workflow_aggregates, list)
                and len(workflow_aggregates) == 1
            ), "official workflow aggregate response"
            validate_official_workflow_aggregates(
                workflow_aggregates, workflow_aggregate_digest, schemas=schemas
            )

            step_aggregate_path = official_app_root + "/steps/aggregates"
            step_aggregate_body = {
                "groupByFunctionName": True,
                "groupByStatus": True,
                "selectCount": True,
                "selectMaxDurationMs": True,
                "timeBucketSizeMs": 60_000,
                "status": ["SUCCESS"],
                "stepName": ["gate_step"],
                "workflowIdPrefix": [workflow_id[:8]],
                "completedAfter": "2000-01-01T00:00:00+00:00",
                "completedBefore": "2100-01-01T00:00:00+00:00",
            }
            aggregate_status, content_type, step_aggregates = request_json_response(
                base,
                step_aggregate_path,
                method="POST",
                payload=step_aggregate_body,
            )
            assert (
                aggregate_status == 200
                and content_type == "application/json"
                and isinstance(step_aggregates, list)
                and len(step_aggregates) == 1
            ), "official step aggregate response"
            validate_official_step_aggregates(
                step_aggregates, step_aggregate_digest, schemas=schemas
            )

            # The released handlers default to count only when every select flag
            # is omitted. Restrict by workflow ID so the independent fixture
            # expectation is exactly one without reading maestro-owned storage.
            aggregate_status, _, default_count = request_json_response(
                base,
                workflow_aggregate_path,
                method="POST",
                payload={
                    "groupByStatus": True,
                    "workflowIds": [workflow_id],
                },
            )
            assert aggregate_status == 200 and default_count == [
                {"group": {"status": "SUCCESS"}, "count": 1}
            ], "released SDK workflow aggregate default count"

            for path, payload, label in (
                (
                    workflow_aggregate_path,
                    {
                        "groupByStatus": True,
                        "selectCount": True,
                        "status": ["NO_MATCHING_STATUS"],
                    },
                    "workflow",
                ),
                (
                    step_aggregate_path,
                    {
                        "groupByStatus": True,
                        "selectCount": True,
                        "status": ["NO_MATCHING_STATUS"],
                    },
                    "step",
                ),
            ):
                aggregate_status, content_type, empty_aggregates = (
                    request_json_response(base, path, method="POST", payload=payload)
                )
                assert (
                    aggregate_status == 200
                    and content_type == "application/json"
                    and empty_aggregates == []
                ), f"official empty {label} aggregates"

            for path, payload, label in (
                (
                    workflow_aggregate_path,
                    {"unknown": True},
                    "workflow aggregate unknown field",
                ),
                (
                    step_aggregate_path,
                    {"completedAfter": "not-a-date"},
                    "step aggregate invalid date",
                ),
            ):
                aggregate_status, content_type, problem = request_json_response(
                    base, path, method="POST", payload=payload
                )
                validate_problem_response(
                    aggregate_status, content_type, problem, 400, label
                )
            aggregate_status, content_type, problem = request_json_response(
                base,
                workflow_aggregate_path,
                method="POST",
                payload={},
            )
            validate_problem_response(
                aggregate_status,
                content_type,
                problem,
                502,
                "released SDK aggregate missing grouping",
            )

            export_path = (
                official_workflow_root
                + urllib.parse.quote(workflow_id, safe="")
                + "/export"
            )
            if export_digest_path.exists():
                export_digest_path.unlink()
            export_status, content_type, exported = request_json_response(
                base, export_path + "?exportChildren=true"
            )
            if metadata_only:
                validate_problem_response(
                    export_status,
                    content_type,
                    exported,
                    502,
                    "official metadata-only export refusal",
                )
                assert "metadata-only mode" in exported["detail"], (
                    "official metadata-only export refusal detail"
                )
                assert not export_digest_path.exists(), (
                    "metadata-only refusal emitted an export blob"
                )
            else:
                assert (
                    export_status == 200
                    and content_type == "application/json"
                    and isinstance(exported, dict)
                ), "official workflow export response"
                observed_export_digest = wait_for(
                    lambda: (
                        export_digest_path.read_text().strip()
                        if export_digest_path.is_file()
                        else None
                    ),
                    time.monotonic() + 3,
                    "observed SDK export frame digest",
                )
                validate_official_export(
                    exported, observed_export_digest, schemas=schemas
                )

            export_status, content_type, problem = request_json_response(
                base, official_workflow_root + "missing-workflow/export"
            )
            validate_problem_response(
                export_status,
                content_type,
                problem,
                404,
                "official missing workflow export",
            )
            export_status, content_type, problem = request_json_response(
                base, export_path + "?exportChildren=true&exportChildren=false"
            )
            validate_problem_response(
                export_status,
                content_type,
                problem,
                400,
                "official duplicate export option",
            )

            status, events = request_json(base, workflow_path + "/events")
            if metadata_only:
                assert official_gate_queue["applicationName"] == app, (
                    "metadata-only mode hid nonprivate queue configuration"
                )
                assert detail["Input"] is None and detail["Output"] is None, (
                    "metadata-only workflow blobs were exposed"
                )
                assert step["output"] is None, "metadata-only step output was exposed"
                assert status == 502 and "metadata-only mode" in events.get(
                    "error", ""
                ), "real SDK metadata-only refusal was not surfaced"
            else:
                for field in ("Input", "Output"):
                    assert isinstance(detail[field], str) and detail[field], (
                        f"workflow {field} blob missing"
                    )
                    expected = ready[field.lower() + "_sha256"]
                    actual = hashlib.sha256(detail[field].encode()).hexdigest()
                    assert actual == expected, f"{field} opaque digest differs from SDK"
                assert isinstance(step["output"], str) and step["output"], (
                    "step opaque output missing"
                )
                assert (
                    hashlib.sha256(step["output"].encode()).hexdigest()
                    == ready["step_output_sha256"]
                ), "step opaque digest differs from SDK"
                assert (
                    status == 200 and isinstance(events, list) and len(events) == 1
                ), "released SDK event read"
                assert (
                    hashlib.sha256(events[0]["value"].encode()).hexdigest()
                    == ready["event_value_sha256"]
                ), "event opaque digest differs from SDK"
                if case_env.get("POSTGRES_GATE_CONSOLE") == "1":
                    console_ready = temp / "console.json"
                    console_ready.write_text(json.dumps(ready["console"]))
                    subprocess.run(
                        [
                            "node",
                            str(ROOT / "dev/tests/console_browser.mjs"),
                            "real",
                            base,
                            str(console_ready),
                        ],
                        env=environment,
                        timeout=90,
                        check=True,
                    )
        except BaseException:
            for name in ("maestro.log", "sdk.log"):
                print((temp / name).read_text()[-6000:], file=sys.stderr)
            raise
        finally:
            try:
                if app_process is not None:
                    if app_process.stdin is not None and app_process.poll() is None:
                        try:
                            app_process.stdin.write("stop\n")
                            app_process.stdin.flush()
                        except BrokenPipeError:
                            pass
                    stop_process(app_process, terminate=False, label="SDK app")
            finally:
                stop_process(server, label="maestro")


def run(binary, pg_bin, python, temp, console_browser):
    environment = sanitized_environment(os.environ, temp)
    if console_browser:
        for key in ("PLAYWRIGHT_MODULE", "CHROMIUM_BIN"):
            environment[key] = os.environ[key]
        environment["POSTGRES_GATE_CONSOLE"] = "1"
    validate_sdk_python(python, environment)
    with postgres_cluster(pg_bin, temp, environment) as (socket_dir, pg_port):
        for label, metadata_only in (("data", False), ("metadata", True)):
            database = "gate_" + label
            create_database(pg_bin, socket_dir, pg_port, database, environment)
            run_case(python, binary, temp / label,
                     postgres_database_url(socket_dir, pg_port, database), metadata_only, environment)
    print("PASS Postgres 18 + DBOS 3.1.0: API/Console reads, SDK field and blob comparisons, metadata refusal")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--postgres-bin", type=Path, default=os.environ.get("POSTGRES18_BIN"))
    parser.add_argument("--console-browser", action="store_true")
    args = parser.parse_args()
    if args.postgres_bin is None:
        parser.error("--postgres-bin or POSTGRES18_BIN is required")
    python = ROOT / "dev/tests/python" / SDK_VERSION / ".venv/bin/python"
    with tempfile.TemporaryDirectory(prefix="maestro-postgres-") as path:
        run(args.binary.resolve(), args.postgres_bin.resolve(), python, Path(path), args.console_browser)


if __name__ == "__main__":
    main()
