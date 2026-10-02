"""Exercise executor death and disposable Maestro state with real SDKs/Postgres.

Uses only a temporary cluster, databases, files, and child processes owned by the
gate. SDK environments are the existing pinned tests/sdk environments.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack
from pathlib import Path

from api import (
    ROOT,
    create_database,
    postgres_cluster,
    postgres_database_url,
    request_json,
    reserve_loopback_port,
    sanitized_environment,
    wait_for,
)


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def kill(proc):
    proc.kill()
    proc.wait(timeout=5)


def lines(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def run_case(binary, pg_bin, socket_dir, pg_port, environment, temp, version, case):
    directory = temp / f"{version}-{case}"
    directory.mkdir()
    database = "recovery_" + version.replace(".", "_") + "_" + case
    create_database(pg_bin, socket_dir, pg_port, database, environment)
    python = ROOT / "tests/sdk" / version / ".venv/bin/python"
    actual = subprocess.check_output(
        [
            str(python),
            "-c",
            "import importlib.metadata; print(importlib.metadata.version('dbos'))",
        ],
        env=environment,
        text=True,
    ).strip()
    assert actual == version, (actual, version)
    port = reserve_loopback_port()
    base = f"http://127.0.0.1:{port}"
    # Shorten only the configurable timeout; use the production recovery loop.
    timeout = 2
    env = dict(
        environment,
        RECOVERY_DIRECTORY=str(directory),
        RECOVERY_APP="recovery-app",
        RECOVERY_DATABASE_URL=postgres_database_url(socket_dir, pg_port, database),
        RECOVERY_WS=base.replace("http:", "ws:"),
    )

    def sql(statement):
        return subprocess.check_output(
            [
                str(pg_bin / "psql"),
                "-XAt",
                "-v",
                "ON_ERROR_STOP=1",
                "-h",
                str(socket_dir),
                "-p",
                str(pg_port),
                "-U",
                "gate",
                "-d",
                database,
                "-c",
                statement,
            ],
            env=environment,
            text=True,
            timeout=10,
        ).strip()

    with ExitStack() as stack:

        def spawn(command, name, extra=None):
            log = stack.enter_context((directory / f"{name}.log").open("w"))
            proc = subprocess.Popen(
                command,
                env=dict(env, **(extra or {})),
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            stack.callback(stop, proc)
            return proc

        def server(name):
            proc = spawn(
                [
                    str(binary),
                    "--listen",
                    f"127.0.0.1:{port}",
                    "--recovery-timeout",
                    f"{timeout}s",
                ],
                name,
            )
            wait_for(
                lambda: request_json(base, "/api/executors")[0] == 200,
                time.monotonic() + 10,
                "Maestro readiness",
            )
            return proc

        def worker(name, **extra):
            proc = spawn(
                [str(python), str(ROOT / "tests/postgres/recovery_app.py")],
                name,
                extra,
            )

            def launched():
                assert proc.poll() is None, f"SDK {name} exited; see {name}.log"
                return (directory / f"executor-{proc.pid}.json").exists()

            wait_for(launched, time.monotonic() + 20, "SDK launch")
            return proc

        def connected(count):
            _, payload = request_json(base, "/api/executors")
            return len(payload) == count

        if case != "outage":
            maestro = server("maestro-before")
        old = worker("old", RECOVERY_SEED="1")
        wait_for(
            lambda: all(
                (directory / f"{name}.checkpointed").exists()
                for name in ("direct", "queued")
            ),
            time.monotonic() + 20,
            "committed checkpoints",
        )
        assert len(lines(directory / "effects.jsonl")) == 2

        if case == "survivor":
            worker("survivor")
            wait_for(
                lambda: connected(2), time.monotonic() + 15, "two connected executors"
            )
        elif case == "lost_reply":
            worker("survivor", RECOVERY_HOLD_REPLY="1")
            wait_for(
                lambda: connected(2), time.monotonic() + 15, "two connected executors"
            )

        killed_at = time.time()
        kill(old)
        if case == "outage":
            # An executor lived entirely during the outage. Also make the real
            # abandoned work old and add completed history to the same database.
            sql(
                "UPDATE dbos.workflow_status SET created_at=created_at-7776000000 WHERE workflow_uuid IN ('direct','queued')"
            )
            sql(
                "INSERT INTO dbos.workflow_status (workflow_uuid,status,name,application_version,executor_id,application_name,created_at,updated_at,priority) SELECT 'history-' || n, 'SUCCESS', 'recovery_workflow', 'v1', 'history-owner', 'recovery-app', 1, 1, 0 FROM generate_series(1,30000) n"
            )
            maestro = server("maestro-after")
            worker("wrong-version", RECOVERY_VERSION="v2")
            wait_for(
                lambda: connected(1), time.monotonic() + 15, "wrong-version executor"
            )
            time.sleep(timeout + 1)
            assert len(lines(directory / "starts.jsonl")) == 2, (
                "recovered with a mismatched version"
            )
            worker("replacement")
        elif case == "restart":
            kill(maestro)
            maestro = server("maestro-after")
            worker("replacement")

        wait_for(
            lambda: len(lines(directory / "starts.jsonl")) == 4,
            time.monotonic() + 35,
            "both workflows recovered",
        )
        starts = lines(directory / "starts.jsonl")
        assert min(row["time"] for row in starts[2:]) >= killed_at + timeout, (
            "recovery before timeout"
        )
        if case == "lost_reply":
            wait_for(
                lambda: (directory / "reply-held").exists(),
                time.monotonic() + 10,
                "SDK recovery before acknowledgement",
            )
            kill(maestro)
            maestro = server("maestro-after")
            (directory / "release-reply").touch()
            wait_for(
                lambda: connected(1),
                time.monotonic() + 20,
                "survivor reconnect after lost reply",
            )
            time.sleep(timeout + 1)
            assert len(lines(directory / "starts.jsonl")) == 4, (
                "lost acknowledgement reran recovered workflows"
            )
            assert len(lines(directory / "replies.jsonl")) == 1

        (directory / "finish").touch()
        wait_for(
            lambda: sql(
                "SELECT count(*) FROM dbos.workflow_status WHERE workflow_uuid IN ('direct','queued') AND status='SUCCESS'"
            )
            == "2",
            time.monotonic() + 15,
            "database-confirmed completion",
        )
        assert len(lines(directory / "effects.jsonl")) == 2, (
            "checkpointed steps ran again"
        )
        assert len(lines(directory / "starts.jsonl")) == 4
        print(
            f"PASS DBOS {version}: {case}; direct and queued workflows recovered; checkpoint effects preserved",
            flush=True,
        )


def run(binary, pg_bin, temp, versions):
    env = sanitized_environment(os.environ, temp)
    with postgres_cluster(pg_bin, temp, env) as (socket_dir, port):
        for version in versions:
            for case in ("survivor", "restart", "outage", "lost_reply"):
                run_case(binary, pg_bin, socket_dir, port, env, temp, version, case)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--postgres-bin", type=Path, default=os.environ.get("POSTGRES18_BIN")
    )
    parser.add_argument("--version", action="append", choices=("2.31.1", "3.1.0"))
    args = parser.parse_args()
    if args.postgres_bin is None:
        parser.error("--postgres-bin or POSTGRES18_BIN is required")
    with tempfile.TemporaryDirectory(prefix="maestro-recovery-") as scratch:
        try:
            run(args.binary.resolve(), args.postgres_bin.resolve(), Path(scratch),
                args.version or ("2.31.1", "3.1.0"))
        except BaseException:
            # Only gate-owned application logs; no inherited configuration.
            for log in Path(scratch).glob("*/*.log"):
                print(f"{log.name}:\n{log.read_text()[-6000:]}", file=sys.stderr)
            raise


if __name__ == "__main__":
    main()
