"""Exercise the actual scanned image under the Compose runtime restrictions."""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "dev"))
from oci import load_image, output, run  # noqa: E402 - explicit repository helper path

HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


@contextmanager
def probe_volume(probe):
    # A named volume also works when act's runner and the Docker daemon have
    # different filesystems. The release image itself remains unchanged.
    volume = output("docker", "volume", "create")
    try:
        helper = output(
            "docker",
            "create",
            "--network",
            "none",
            "--mount",
            f"type=volume,src={volume},dst=/probe",
            "busybox:1.37.0@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e",
        )
        try:
            run("docker", "cp", str(probe), f"{helper}:/probe/probe")
        finally:
            run("docker", "rm", helper, stdout=subprocess.DEVNULL)
        yield volume
    finally:
        run("docker", "volume", "rm", volume, stdout=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--all-platforms", action="store_true")
    parser.add_argument("--probe-writable-root", action="store_true")
    args = parser.parse_args()
    compose = json.loads(
        output(
            "docker",
            "compose",
            "-f",
            str(ROOT / "compose.yaml"),
            "config",
            "--format",
            "json",
        )
    )["services"]["maestro"]
    assert compose["user"] == "65532:65532"
    assert compose["read_only"] and compose["cap_drop"] == ["ALL"]
    assert compose["security_opt"] == ["no-new-privileges:true"]
    assert compose["pids_limit"] == 128
    assert all(port["host_ip"] == "127.0.0.1" for port in compose["ports"])
    native = output("docker", "version", "--format", "{{.Server.Arch}}")
    for arch in ("amd64", "arm64") if args.all_platforms else (native,):
        tag = f"maestro:test-{arch}"
        load_image(arch, tag)
        with tempfile.TemporaryDirectory(prefix="maestro-container-") as scratch:
            probe = Path(scratch) / "probe"
            run(
                "go",
                "build",
                "-trimpath",
                "-o",
                str(probe),
                str(Path(__file__).with_name("probe_linux.go")),
                env=dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH=arch),
            )
            with probe_volume(probe) as volume:
                command = [
                    "docker",
                    "run",
                    "--detach",
                    "--platform",
                    f"linux/{arch}",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges:true",
                    "--pids-limit",
                    "128",
                    "--publish",
                    "127.0.0.1::8090",
                    "--mount",
                    f"type=volume,src={volume},dst=/probe,readonly",
                ]
                if not args.probe_writable_root:
                    command.append("--read-only")
                container = output(*command, tag)
                try:
                    state = json.loads(output("docker", "inspect", container))[0]
                    port = state["NetworkSettings"]["Ports"]["8090/tcp"][0]["HostPort"]
                    base = f"http://127.0.0.1:{port}"
                    deadline = time.monotonic() + 30
                    while True:
                        try:
                            with HTTP.open(base + "/healthz", timeout=2) as response:
                                assert json.load(response) == {"status": True}
                            break
                        except (OSError, urllib.error.URLError):
                            if time.monotonic() >= deadline:
                                raise
                            time.sleep(0.1)
                    for route, expected in (
                        ("/", b"Maestro"),
                        ("/openapi.json", b"openapi"),
                        ("/static/htmx.min.js", b"htmx"),
                    ):
                        with HTTP.open(base + route, timeout=3) as response:
                            assert (
                                response.status == 200
                                and expected.lower() in response.read().lower()
                            ), route
                    run("docker", "exec", container, "/probe/probe")
                    run("docker", "stop", "--timeout", "15", container)
                    state = json.loads(output("docker", "inspect", container))[0]
                    assert state["State"]["ExitCode"] == 0, (
                        "container did not shut down cleanly"
                    )
                    print(
                        f"PASS linux/{arch} HTTP, assets, runtime restrictions and SIGTERM",
                        flush=True,
                    )
                finally:
                    run("docker", "rm", "--force", container, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
