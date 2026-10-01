"""Explicit dbosctl read gate: pinned unmodified CLI, fake peer and optional released Python SDK."""

import argparse
import hashlib
import importlib.util
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "owned_gate", ROOT / "dev/tests/python/install.py"
)
if spec is None or spec.loader is None:
    raise RuntimeError("missing owned process-group gate")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
run_gate = module.run_gate
PIN = "17879522360ced42ea9a2a7778e0a2d0973d7901"


def command(argv, *, env, cwd=None, timeout=150, capture=False):
    return subprocess.run(
        argv,
        cwd=cwd,
        env=env,
        timeout=timeout,
        check=True,
        capture_output=capture,
        text=True,
    )


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_child(real):
    with tempfile.TemporaryDirectory(prefix="maestro-dbosctl-owned-") as folder:
        temp = Path(folder)
        home = temp / "home"
        home.mkdir(mode=0o700)
        # Whitelist for setup AND execution; neither git/Go nor CLI/SDK inherits
        # operator profiles, DBOS_*, cloud credentials, proxies or sysdb settings.
        env = {
            key: os.environ[key] for key in ("PATH", "SYSTEMROOT") if key in os.environ
        }
        env.update(
            HOME=str(home),
            XDG_CONFIG_HOME=str(temp / "config"),
            XDG_CACHE_HOME=str(temp / "cache"),
            XDG_DATA_HOME=str(temp / "data"),
            GOPATH=str(temp / "go"),
            GOMODCACHE=str(temp / "go/pkg/mod"),
            GOCACHE=str(temp / "go-build"),
            GIT_CONFIG_NOSYSTEM="1",
            GIT_TERMINAL_PROMPT="0",
            HTTP_PROXY="",
            HTTPS_PROXY="",
            ALL_PROXY="",
            NO_PROXY="*",
            PYTHONNOUSERSITE="1",
            TMPDIR=str(temp),
        )
        source = temp / "dbos-ctl"
        command(
            [
                "git",
                "clone",
                "--quiet",
                "--depth=1",
                "--branch",
                "v0.10.1",
                "https://github.com/dbos-inc/dbos-ctl.git",
                str(source),
            ],
            env=env,
        )
        commit = command(
            ["git", "rev-parse", "HEAD"], env=env, cwd=source, capture=True
        ).stdout.strip()
        if commit != PIN:
            raise RuntimeError("dbosctl tag did not resolve to pinned commit")
        binary = temp / "dbosctl"
        command(
            ["go", "build", "-o", str(binary), "./cmd/dbosctl"],
            env=env,
            cwd=source,
            timeout=240,
        )
        version = command([str(binary), "version"], env=env, capture=True).stdout
        if "v0.10.1" not in version or "commit    1787952" not in version:
            raise RuntimeError("dbosctl version/VCS pin mismatch")
        digest = checksum(binary)
        print(f"PIN dbosctl v0.10.1 {commit} sha256={digest}", flush=True)
        env["DBOSCTL_BIN"] = str(binary)
        # Missing binary is a hard failure in the explicit gate, not a test skip.
        command(
            [
                "go",
                "test",
                "-tags=dbosctl",
                "-race",
                "-count=1",
                "./internal/api",
                "-run",
                "^Test(LocalHTTPV2|HTTPWorkflow|HTTPV2|DBOSCTL)",
                "-v",
            ],
            env=env,
            cwd=ROOT,
            timeout=150,
        )
        if checksum(binary) != digest:
            raise RuntimeError("dbosctl changed during fake-peer acceptance")
        if real:
            command(
                [
                    "python3",
                    "-I",
                    "-S",
                    str(ROOT / "dev/tests/python/gate.py"),
                    "--dbosctl-bin",
                    str(binary),
                ],
                env=env,
                cwd=ROOT,
                timeout=360,
            )
            if checksum(binary) != digest:
                raise RuntimeError("dbosctl changed during real-SDK acceptance")
        print(
            "PASS dbosctl read gate"
            + (" with selected real Python" if real else " fake peer"),
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--real", action="store_true", help="include pinned released Python SDK gate"
    )
    parser.add_argument("--owned-child", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.owned_child:
        run_child(args.real)
        return
    # A unique process session owns all git/Go/CLI/Python/maestro/SDK descendants.
    # run_gate terminates and joins it on nonzero, timeout, interrupt AND success;
    # waitid(WNOWAIT) prevents reusing the session-leader PID before cleanup.
    run_gate(
        [sys.executable, "-I", "-S", str(Path(__file__).resolve()), "--owned-child"]
        + (["--real"] if args.real else []),
        {key: os.environ[key] for key in ("PATH", "SYSTEMROOT") if key in os.environ},
        timeout=660 if args.real else 330,
    )


if __name__ == "__main__":
    main()
