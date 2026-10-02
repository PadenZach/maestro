"""Opt-in real released-Python/maestro boundary gate; no SDK checkout or service."""

import argparse
import hashlib
import html as html_module
import importlib.metadata
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DEADLINE = 65
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


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


def request(base, path):
    try:
        with HTTP.open(base + path, timeout=3) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def stop(proc, timeout=12):
    if proc.poll() is None:
        proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=3)
        raise AssertionError("owned child did not terminate within deadline")


def run(binary, temp, private, dbosctl_bin=None):
    version = importlib.metadata.version("dbos")
    if not (version.startswith("2.31.") or version.startswith("3.")):
        raise ValueError("supported DBOS Python releases are 2.31.x and 3.x")
    temp.mkdir(mode=0o700)
    with socket.socket() as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    token = temp.parent.name.rsplit("-", 1)[-1]
    key = "gate-" + token
    app = "gate-" + token
    executor = "executor-" + token
    # No inherited DBOS/cloud auth, database/profile, or application settings.
    clean = {k: os.environ[k] for k in ("PATH", "SYSTEMROOT") if k in os.environ}
    clean.update(
        HOME=str(temp),
        XDG_CONFIG_HOME=str(temp),
        TMPDIR=str(temp),
        PYTHONNOUSERSITE="1",
        GATE_VERSION=version,
        GATE_APP=app,
        GATE_EXECUTOR=executor,
        GATE_DB=str(temp / "system.sqlite"),
        GATE_READY=str(temp / "ready.json"),
        GATE_INJECTION_ACK=str(temp / "injection-ack"),
        GATE_WS=base.replace("http:", "ws:"),
        GATE_KEY=key,
        GATE_PRIVATE="1" if private else "0",
    )
    with (
        (temp / "server.log").open("w+") as server_log,
        (temp / "sdk.log").open("w+") as sdk_log,
    ):
        server = subprocess.Popen(
            [str(binary), "--listen", f"127.0.0.1:{port}"],
            cwd=ROOT,
            env=clean,
            stdout=server_log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        app_proc = None
        try:
            deadline = time.monotonic() + DEADLINE

            def healthy():
                assert server.poll() is None, "maestro exited before readiness"
                return request(base, "/healthz") == (200, {"status": True})

            wait_for(healthy, deadline, "maestro health")
            app_proc = subprocess.Popen(
                [sys.executable, str(ROOT / "dev/tests/python/app.py")],
                cwd=temp,
                env=clean,
                stdin=subprocess.PIPE,
                stdout=sdk_log,
                stderr=subprocess.STDOUT,
                text=True,
            )

            def started():
                assert app_proc.poll() is None, "SDK app exited before ready"
                ready = temp / "ready.json"
                return json.loads(ready.read_text()) if ready.is_file() else None

            ready = wait_for(started, deadline, "SDK workflow")
            workflow_id = ready["workflow_id"]
            api = base + "/api"

            def connected():
                status, peers = request(api, "/executors")
                assert status == 200, f"executor endpoint status {status}"
                return peers if isinstance(peers, list) and len(peers) == 1 else None

            peer = wait_for(connected, deadline, "handshake")[0]
            assert (
                peer["app"] == app
                and isinstance(peer["executor_id"], str)
                and peer["executor_id"]
            ), "SDK handshake identity"
            executor = peer["executor_id"]
            assert peer["language"] == "python" and peer["dbos_version"] == version
            assert peer["application_version"] == "gate-v1" and peer[
                "executor_metadata"
            ] == {"gate": "released-python-reads"}
            connected_at = peer["connected_at"]
            prefix = "/" + urllib.parse.quote(app, safe="")
            wf_path = prefix + "/workflows/" + urllib.parse.quote(workflow_id, safe="")
            status, listing = request(api, prefix + "/workflows")
            assert status == 200 and any(
                row["WorkflowUUID"] == workflow_id for row in listing
            ), "list workflows"
            listed = next(row for row in listing if row["WorkflowUUID"] == workflow_id)
            assert listed["Input"] is None and listed["Output"] is None, (
                "list must suppress blobs"
            )
            status, detail = request(api, wf_path)
            assert (
                status == 200
                and detail["WorkflowUUID"] == workflow_id
                and detail["Status"] == "SUCCESS"
            ), "get workflow"
            assert detail["QueueName"] is None, "direct SDK workflow is not enqueued"
            if private:
                assert detail["Input"] is None and detail["Output"] is None, (
                    "metadata-only blob load flags"
                )
            else:
                for field in ("Input", "Output"):
                    assert isinstance(detail[field], str) and detail[field], (
                        "workflow blob load flags"
                    )
                    actual_hash = hashlib.sha256(detail[field].encode()).hexdigest()
                    expected_hash = ready[field.lower() + "_sha256"]
                    assert actual_hash == expected_hash, (
                        f"JSON {field} blob differs from SDK"
                    )
            assert json.loads(detail["Attributes"]) == {"gate": "visible"}, (
                "recent Attributes JSON string"
            )
            assert detail["ApplicationName"] == app, "recent ApplicationName"
            if dbosctl_bin:
                # Unmodified external client against ordinary SDK-persisted rows.
                # This is separate from the fake-peer CLI/schema matrix.
                from datetime import datetime, timedelta, timezone

                cli_env = dict(clean, XDG_CACHE_HOME=str(temp / "cache"))
                args = [
                    str(dbosctl_bin),
                    "workflow",
                    "list",
                    "--url",
                    base,
                    "--org",
                    "local",
                    "--app",
                    app,
                    "--limit",
                    "2",
                    "--since",
                    (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
                    "-o",
                    "json",
                ]
                cli_list = subprocess.run(
                    args, env=cli_env, text=True, capture_output=True, timeout=8
                )
                assert cli_list.returncode == 0, (
                    f"real dbosctl list exit={cli_list.returncode}: {cli_list.stderr[:250]}"
                )
                cli_rows = json.loads(cli_list.stdout)
                assert any(row["workflowId"] == workflow_id for row in cli_rows), (
                    "real dbosctl date-filtered list omitted SDK workflow"
                )
                for operation, expected in (("get", dict), ("steps", list)):
                    result = subprocess.run(
                        [
                            str(dbosctl_bin),
                            "workflow",
                            operation,
                            workflow_id,
                            "--url",
                            base,
                            "--org",
                            "local",
                            "--app",
                            app,
                            "-o",
                            "json",
                        ],
                        env=cli_env,
                        text=True,
                        capture_output=True,
                        timeout=8,
                    )
                    assert result.returncode == 0, (
                        f"real dbosctl {operation} exit={result.returncode}: {result.stderr[:250]}"
                    )
                    assert isinstance(json.loads(result.stdout), expected), operation
            status, steps = request(api, wf_path + "/steps")
            assert status == 200 and any(
                step["function_name"] == "gate_step" for step in steps
            ), "steps"
            step = next(step for step in steps if step["function_name"] == "gate_step")
            assert (step["output"] is None) == private, "step load_output flag"
            status, queues = request(api, prefix + "/queues")
            assert status == 200 and any(q["name"] == "gate-queue" for q in queues), (
                "list queues"
            )
            status, queue = request(api, prefix + "/queues/gate-queue")
            assert status == 200 and queue["name"] == "gate-queue", "get queue"
            assert queue["concurrency"] == 3
            assert queue["worker_concurrency"] == 2
            assert (
                queue["rate_limit_max"] == 5 and queue["rate_limit_period_sec"] == 1.0
            ), "queue rate"
            assert queue["application_name"] == app, "recent queue app"
            assert (
                queue["partition_concurrency"] == 2
                and queue["partition_worker_concurrency"] == 1
            ), "recent queue partition limits"
            assert (
                queue["partition_rate_limit_max"] == 4
                and queue["partition_rate_limit_period_sec"] == 2.0
            ), "recent queue partition rate"
            scheduled_id = ready["scheduled_workflow_id"]
            status, scheduled = request(
                api,
                prefix + "/workflows/" + urllib.parse.quote(scheduled_id, safe=""),
            )
            assert status == 200 and scheduled["ScheduleName"] == "gate-schedule", (
                "recent non-null ScheduleName"
            )
            if version.startswith("2."):
                assert app_proc.stdin is not None
                ack = temp / "injection-ack"
                app_proc.stdin.write("inject-events-error\n")
                app_proc.stdin.flush()
                wait_for(
                    lambda: ack.is_file() and ack.read_text() == "on",
                    deadline,
                    "SDK error injection",
                )
                status, error = request(api, wf_path + "/events")
                assert status == 502 and "gate-injected-events-error" in error.get(
                    "error", ""
                ), "released SDK handler command failure must be surfaced"
                app_proc.stdin.write("restore-events\n")
                app_proc.stdin.flush()
                wait_for(
                    lambda: ack.read_text() == "off",
                    deadline,
                    "SDK handler restoration",
                )
                assert request(api, wf_path)[0] == 200, (
                    "handler error closed the socket"
                )
                assert request(api, wf_path + "/events") == (200, []), (
                    "SDK handler remains usable after error"
                )
            if private:
                status, error = request(api, wf_path + "/events")
                assert status == 502 and "metadata-only mode" in error["error"], (
                    "SDK-local data-only command refusal"
                )
                assert request(api, wf_path)[0] == 200, "refusal closed the socket"
                status, html = request_text(
                    base, "/apps" + wf_path + "/blob?kind=output"
                )
                assert (
                    status == 200
                    and "None" not in html
                    and "step-gate-value" not in html
                ), "metadata-only blob refusal"
            else:
                for kind in ("input", "output"):
                    status, fragment = request_text(
                        base, "/apps" + wf_path + "/blob?kind=" + kind
                    )
                    body = re.search(
                        r'<pre class="blob-body mono">(.*?)</pre>', fragment, re.S
                    )
                    assert status == 200 and body is not None, (
                        f"HTMX {kind} blob missing"
                    )
                    actual_hash = hashlib.sha256(
                        html_module.unescape(body.group(1)).encode()
                    ).hexdigest()
                    assert actual_hash == ready[kind + "_sha256"], (
                        f"HTMX {kind} blob differs from SDK"
                    )
            # Trigger an SDK-owned socket close; SDK reconnects and re-handshakes.
            assert app_proc.stdin is not None
            app_proc.stdin.write("reconnect\n")
            app_proc.stdin.flush()

            def reconnected():
                assert app_proc.poll() is None, "SDK app exited during reconnect"
                peers = request(base, "/api/executors")[1]
                return (
                    peers[0]
                    if len(peers) == 1 and peers[0]["connected_at"] != connected_at
                    else None
                )

            assert (
                wait_for(reconnected, deadline, "SDK reconnect")["executor_id"]
                == executor
            )
            assert request(base + "/api", wf_path)[0] == 200, "read after reconnect"
            server.send_signal(signal.SIGTERM)
            assert server.wait(timeout=12) == 0, "maestro SIGTERM shutdown"
            print(
                f"PASS dbos=={version} handshake/read/blob/queue/reconnect/SIGTERM"
                + ("/metadata-refusal" if private else "")
            )
        except Exception:
            print(
                f"SDK exit={app_proc.poll() if app_proc else 'not-started'}, maestro exit={server.poll()}",
                file=sys.stderr,
            )
            for log in ("server.log", "sdk.log"):
                print((temp / log).read_text()[-6000:], file=sys.stderr)
            raise
        finally:
            try:
                if app_proc is not None:
                    try:
                        if app_proc.stdin and app_proc.poll() is None:
                            try:
                                app_proc.stdin.write("stop\n")
                                app_proc.stdin.flush()
                            except BrokenPipeError:
                                pass
                    finally:
                        stop(app_proc)
            finally:
                stop(server)


def request_text(base, path):
    try:
        with HTTP.open(base + path, timeout=3) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--dbosctl-bin", type=Path)
    args = parser.parse_args()
    version = importlib.metadata.version("dbos")
    print(f"Testing DBOS {version}", flush=True)
    with tempfile.TemporaryDirectory(prefix="maestro-python-gate-") as path:
        temp = Path(path)
        run(args.binary.resolve(), temp / "data", False, args.dbosctl_bin)
        if version.startswith("3."):
            run(args.binary.resolve(), temp / "metadata", True, args.dbosctl_bin)


if __name__ == "__main__":
    main()
