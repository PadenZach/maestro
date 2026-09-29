"""Provision the opt-in SDK matrix without reading operator uv configuration."""

import os
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def stop_gate_group(proc):
    # The SDK and maestro inherit this unique gate session. Keep the session
    # leader unreaped until all signals are sent, so its process-group ID cannot
    # be recycled and accidentally identify an unrelated process group.
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError:
        # macOS can report EPERM when the group contains only dead zombies.
        if proc.poll() is not None:
            return
        raise
    time.sleep(0.25)
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        if proc.poll() is None:
            raise


def run_gate(command, env, timeout):
    if os.name != "posix" or not hasattr(os, "WNOWAIT"):
        raise RuntimeError("gate process-group cleanup requires POSIX waitid/WNOWAIT")
    proc = subprocess.Popen(command, env=env, start_new_session=True)
    deadline = time.monotonic() + timeout
    try:
        while (
            os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None
        ):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout)
            time.sleep(min(0.05, remaining))
    except BaseException as original:
        try:
            stop_gate_group(proc)
        except BaseException as cleanup_error:
            original.add_note(f"gate group cleanup failed: {cleanup_error!r}")
        try:
            proc.wait(timeout=3)
        except BaseException as wait_error:
            original.add_note(f"gate parent reap failed: {wait_error!r}")
        raise

    # waitid(WNOWAIT) keeps the exited leader PID reserved through group cleanup.
    # Clean even on normal exit so a misbehaving gate cannot report success with
    # an owned child still listening; retain the exact original exit status.
    cleanup_error = None
    try:
        stop_gate_group(proc)
    except BaseException as exc:
        cleanup_error = exc
    code = proc.wait(timeout=3)
    if code != 0:
        failure = subprocess.CalledProcessError(code, command)
        if cleanup_error is not None:
            failure.add_note(f"gate group cleanup failed: {cleanup_error!r}")
            raise failure from cleanup_error
        raise failure
    if cleanup_error is not None:
        raise cleanup_error


def main():
    uv = shutil.which("uv")
    if uv is None:
        raise RuntimeError("uv 0.6.9 is required for the explicit Python gate")
    with tempfile.TemporaryDirectory(prefix="maestro-uv-gate-") as scratch:
        temp = Path(scratch)
        install_dir = ROOT / "dev/tests/python/.python"
        clean = {k: os.environ[k] for k in ("PATH", "SYSTEMROOT") if k in os.environ}
        clean.update(
            HOME=scratch,
            XDG_CONFIG_HOME=str(temp / "config"),
            XDG_CACHE_HOME=str(temp / "cache"),
            XDG_DATA_HOME=str(temp / "data"),
            TMPDIR=scratch,
            UV_CACHE_DIR=str(temp / "uv-cache"),
            UV_PYTHON_INSTALL_DIR=str(install_dir),
        )
        version = subprocess.run(
            [uv, "--no-config", "--version"],
            env=clean,
            capture_output=True,
            text=True,
            timeout=8,
            check=True,
        ).stdout.strip()
        if version.split()[1] != "0.6.9":
            raise RuntimeError(f"uv 0.6.9 required, found {version}")
        subprocess.run(
            [uv, "python", "install", "3.12.9", "--no-config"],
            env=clean,
            check=True,
            timeout=120,
        )
        # uv's install layout differs by platform; locate only the interpreter we just installed.
        installed = list(install_dir.glob("cpython-3.12.9-*/bin/python3.12"))
        if len(installed) != 1:
            raise RuntimeError("isolated CPython 3.12.9 was not installed")
        python = installed[0]
        for release in ("2.24.0", "2.31.1", "3.1.0"):
            subprocess.run(
                [
                    uv,
                    "sync",
                    "--locked",
                    "--no-config",
                    "--python",
                    str(python),
                    "--default-index",
                    "https://pypi.org/simple",
                    "--no-progress",
                ],
                cwd=ROOT / "dev/tests/python" / release,
                env=clean,
                check=True,
                timeout=120,
            )
        run_gate(
            ["python3", "-I", "-S", str(ROOT / "dev/tests/python/gate.py")],
            clean,
            timeout=540,  # 90s build + four 65s rows + bounded shutdown/diagnostics.
        )


if __name__ == "__main__":
    main()
