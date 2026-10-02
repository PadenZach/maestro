"""Version-bump releases: plan, build a candidate, then publish a tested digest.

VERSION contains SemVer without a v prefix or build metadata. A change on main
creates a release; rerun the original workflow to resume a failed publication.
Candidates are retained by commit so retries use the same image bytes.
"""

import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile
from urllib.parse import quote
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PLATFORMS = [(system, arch) for system in ("linux", "darwin", "windows") for arch in ("amd64", "arm64")]


def run(*args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, text=True, **kwargs)


def output(*args):
    return run(*args, capture_output=True).stdout.strip()


def version_key(value):
    """SemVer precedence, restricted to versions that are also OCI tag names."""
    match = re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
        r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?", value
    )
    if not match or len(value) > 128:
        raise ValueError(f"Invalid VERSION: {value!r}; use e.g. 0.1.0 or 0.2.0-rc.1")
    major, minor, patch, prerelease = match.groups()
    identifiers = []
    for part in prerelease.split(".") if prerelease else []:
        if part.isdigit():
            if len(part) > 1 and part.startswith("0"):
                raise ValueError("Numeric prerelease identifiers cannot have leading zeros")
            identifiers.append((0, int(part)))
        else:
            identifiers.append((1, part))
    return (int(major), int(minor), int(patch), prerelease is None, tuple(identifiers))


def version():
    value = (ROOT / "VERSION").read_text().strip()
    version_key(value)
    return value


def emit(**values):
    for key, value in values.items():
        print(f"{key}={value}", flush=True)
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a") as stream:
            for key, value in values.items():
                stream.write(f"{key}={value}\n")


def plan():
    current = version()
    previous = None
    base = os.environ.get("BASE_SHA", "")
    if base and set(base) != {"0"}:
        # Fail on an unavailable base; only an absent VERSION means first release.
        if output("git", "ls-tree", "--name-only", base, "--", "VERSION"):
            previous = output("git", "show", f"{base}:VERSION")
    changed = previous != current
    if changed and previous is not None and version_key(current) <= version_key(previous):
        raise ValueError(f"VERSION must increase: {previous} -> {current}")
    release = (
        changed
        and os.environ.get("GITHUB_EVENT_NAME") == "push"
        and os.environ.get("GITHUB_REF") == "refs/heads/main"
    )
    emit(version=current, release=str(release).lower())


def image_digest(reference):
    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", reference,
         "--format", "{{.Manifest.Digest}}"],
        cwd=ROOT, text=True, capture_output=True,
    )
    if result.returncode:
        if re.search(r"(?:not found|manifest unknown)", result.stderr, re.IGNORECASE):
            return None
        raise RuntimeError(result.stderr.strip())
    digest = result.stdout.strip()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError(f"Invalid registry digest for {reference}: {digest!r}")
    return digest


def candidate():
    if output("git", "status", "--porcelain"):
        raise ValueError("Published candidates require a clean checkout")
    repository = os.environ["IMAGE_REPOSITORY"].lower()
    revision = output("git", "rev-parse", "HEAD")
    reference = f"{repository}:candidate-{revision}"
    digest = image_digest(reference)
    if digest is None:
        with tempfile.TemporaryDirectory(prefix="maestro-image-") as scratch:
            metadata = Path(scratch) / "metadata.json"
            run(
                "docker", "buildx", "build", "--push", "--platform", "linux/amd64,linux/arm64",
                "--tag", reference, "--sbom=true", "--provenance=mode=min",
                "--build-arg", f"VERSION={version()}",
                "--build-arg", f"REVISION={revision}", "--build-arg", "MODIFIED=false",
                "--build-arg", f"CREATED={output('git', 'show', '-s', '--format=%cI', 'HEAD')}",
                "--metadata-file", str(metadata), ".",
            )
            digest = json.loads(metadata.read_text())["containerimage.digest"]
    for arch in ("amd64", "arm64"):
        run("mise", "run", "image:scan", env=dict(
            os.environ, IMAGE=f"{repository}@{digest}",
            TRIVY_PLATFORM=f"linux/{arch}", TRIVY_IMAGE_SRC="remote",
        ))
    emit(repository=repository, digest=digest)


def promote(repository, tag, digest):
    reference = f"{repository}:{tag}"
    existing = image_digest(reference)
    if existing == digest:
        return
    if existing is not None:
        raise ValueError(f"Refusing to overwrite {reference}: {existing} != {digest}")
    run("docker", "buildx", "imagetools", "create", "--tag", reference, f"{repository}@{digest}")
    if image_digest(reference) != digest:
        raise ValueError(f"Promoted image digest changed: {reference}")


def api(path, *args):
    response = output("gh", "api", path, *args)
    return json.loads(response) if response else None


def archive_name(current, system, arch):
    extension = "zip" if system == "windows" else "tar.gz"
    return f"maestro_{current}_{system}_{arch}.{extension}"


def write_archive(destination, binary, license_file, system, epoch):
    """One executable at the archive root, with stable metadata for retries."""
    executable = "maestro.exe" if system == "windows" else "maestro"
    files = [(executable, binary.read_bytes(), 0o755), ("LICENSE", license_file.read_bytes(), 0o644)]
    if system == "windows":
        timestamp = datetime.fromtimestamp(epoch, timezone.utc).timetuple()[:6]
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, content, mode in files:
                info = zipfile.ZipInfo(name, timestamp)
                info.create_system = 3
                info.external_attr = (0o100000 | mode) << 16
                info.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(info, content)
    else:
        with destination.open("wb") as stream, gzip.GzipFile(filename="", mode="wb", fileobj=stream, mtime=epoch) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for name, content, mode in files:
                    info = tarfile.TarInfo(name)
                    info.size, info.mode, info.mtime = len(content), mode, epoch
                    archive.addfile(info, io.BytesIO(content))


def verify_binary(binary, system, arch, revision):
    metadata = output("go", "version", "-m", str(binary))
    for expected in (f"GOOS={system}", f"GOARCH={arch}", "CGO_ENABLED=0"):
        if expected not in metadata:
            raise ValueError(f"Unexpected binary metadata for {system}/{arch}: missing {expected}")
    # -trimpath omits linker flags from Go's build info; check the embedded value.
    if revision.encode() not in binary.read_bytes():
        raise ValueError(f"Binary for {system}/{arch} is missing revision {revision}")


def build_assets(repository, digest, current, revision):
    destination = ROOT / "dist" / "release"
    destination.mkdir(parents=True, exist_ok=True)
    epoch = int(output("git", "show", "-s", "--format=%ct", "HEAD"))
    assets = []
    with tempfile.TemporaryDirectory(prefix="maestro-binaries-") as scratch:
        for system, arch in PLATFORMS:
            binary = Path(scratch) / ("maestro.exe" if system == "windows" else "maestro")
            if system == "linux":
                # Extract the bytes already scanned and tested in CI; never rebuild the image.
                container = output("docker", "create", "--platform", f"linux/{arch}",
                                   "--pull=always", f"{repository}@{digest}")
                try:
                    run("docker", "cp", f"{container}:/maestro", str(binary))
                finally:
                    run("docker", "rm", container, stdout=subprocess.DEVNULL)
            else:
                run("go", "build", "-trimpath", "-buildvcs=false",
                    "-ldflags", f"-s -w -X github.com/zpaden/maestro/internal/web.Revision={revision} "
                    "-X github.com/zpaden/maestro/internal/web.Modified=false",
                    "-o", str(binary), "./cmd/maestro",
                    env=dict(os.environ, CGO_ENABLED="0", GOOS=system, GOARCH=arch))
            verify_binary(binary, system, arch, revision)
            archive = destination / archive_name(current, system, arch)
            write_archive(archive, binary, ROOT / "LICENSE", system, epoch)
            assets.append(archive)
    checksums = destination / "checksums.txt"
    checksums.write_text("".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in assets))
    return [*assets, checksums]


def release_notes(project, repository, current):
    release_url = f"https://github.com/{project}/releases/tag/v{current}"
    download_url = f"https://github.com/{project}/releases/download/v{current}"
    image_name = quote(repository.split("/", 2)[-1], safe="")
    image_url = f"https://github.com/{project}/pkgs/container/{image_name}?tag={current}"
    lines = [
        f"[Release and downloads]({release_url}) · [Tagged OCI image]({image_url})", "",
        "## Binaries", "", "| Platform | Download |", "| --- | --- |",
    ]
    for system, arch in PLATFORMS:
        label = {"linux": "Linux", "darwin": "macOS", "windows": "Windows"}[system]
        filename = archive_name(current, system, arch)
        lines.append(f"| {label} {arch} | [{filename}]({download_url}/{filename}) |")
    lines.extend([
        "", f"[SHA-256 checksums]({download_url}/checksums.txt)", "",
        "## Install with mise", "", "```sh",
        f"mise use -g github:{project}@{current}", "maestro --help", "```", "",
        "## Container", "", f"Image: `{repository}:{current}` (Linux amd64 and arm64).", "",
        "```sh", f"docker pull {repository}:{current}", "```", "",
    ])
    return "\n".join(lines)


def finalize_release(project, tag, current):
    release = api(f"repos/{project}/releases/tags/{tag}")
    api(f"repos/{project}/releases/{release['id']}", "--method", "PATCH", "-F", "draft=false",
        "-f", f"make_latest={'false' if '-' in current else 'legacy'}")


def tag_commit(repository, tag):
    refs = api(f"repos/{repository}/git/matching-refs/tags/{tag}")
    for ref in refs:
        if ref["ref"] != f"refs/tags/{tag}":
            continue
        obj = ref["object"]
        while obj["type"] == "tag":
            obj = api(f"repos/{repository}/git/tags/{obj['sha']}")["object"]
        if obj["type"] != "commit":
            raise ValueError(f"Tag {tag} does not point to a commit")
        return obj["sha"]
    return None


def publish():
    repository = os.environ["IMAGE_REPOSITORY"].lower()
    digest = os.environ["IMAGE_DIGEST"]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise ValueError("IMAGE_DIGEST must be an immutable sha256 digest")
    revision = output("git", "rev-parse", "HEAD")
    if os.environ.get("RELEASE") != "true":
        promote(repository, f"sha-{revision}", digest)
        return

    current = version()
    tag = f"v{current}"
    project = os.environ["GITHUB_REPOSITORY"]
    commit = tag_commit(project, tag)
    if commit is not None and commit != revision:
        raise ValueError(f"Refusing to move {tag} from {commit} to {revision}")
    releases = api(f"repos/{project}/releases", "--paginate", "--slurp")
    release = next((item for page in releases for item in page if item["tag_name"] == tag), None)
    if release and not release["draft"]:
        if commit != revision or image_digest(f"{repository}:{current}") != digest:
            raise ValueError(f"Published release {tag} does not match this commit and image")
        print(f"Release {tag} is already published; nothing to change")
        return
    assets = build_assets(repository, digest, current, revision)
    if commit is None:
        api(f"repos/{project}/git/refs", "--method", "POST",
            "-f", f"ref=refs/tags/{tag}", "-f", f"sha={revision}")
    if release is None:
        args = ["gh", "release", "create", tag, "--verify-tag", "--draft",
                "--title", tag, "--generate-notes", "--target", revision, "--latest=false"]
        if "-" in current:
            args.append("--prerelease")
        with tempfile.TemporaryDirectory(prefix="maestro-notes-") as scratch:
            notes = Path(scratch) / "notes.txt"
            notes.write_text(release_notes(project, repository, current))
            run(*args, "--notes-file", str(notes))
    promote(repository, f"sha-{revision}", digest)
    promote(repository, current, digest)
    run("gh", "release", "upload", tag, *(str(path) for path in assets), "--clobber")
    # Attach everything before publication so immutable GitHub releases work too.
    finalize_release(project, tag, current)
    print(f"Published {tag}: {repository}@{digest}")


def repair():
    """Add missing binaries to an existing release without moving its tag or OCI image."""
    current = version()
    tag = f"v{current}"
    if os.environ["RELEASE_TAG"] != tag:
        raise ValueError("The selected tag must match VERSION in the release source")
    project = os.environ["GITHUB_REPOSITORY"]
    repository = os.environ["IMAGE_REPOSITORY"].lower()
    revision = output("git", "rev-parse", "HEAD")
    if tag_commit(project, tag) != revision:
        raise ValueError("Release source must match the existing tag exactly")
    release = api(f"repos/{project}/releases/tags/{tag}")
    if release.get("immutable") or release["draft"]:
        raise ValueError("Repair requires an existing, published, mutable release")
    digest = image_digest(f"{repository}:{current}")
    if digest is None:
        raise ValueError("The versioned OCI image must already exist")
    assets = build_assets(repository, digest, current, revision)
    existing = {asset["name"]: asset for asset in release["assets"]}
    missing = []
    for path in assets:
        checksum = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        if path.name not in existing:
            missing.append(str(path))
        elif existing[path.name].get("digest") != checksum:
            raise ValueError(f"Refusing to replace published binary asset {path.name}")
    if missing:
        run("gh", "release", "upload", tag, *missing)
    uploaded = api(f"repos/{project}/releases/tags/{tag}")
    digests = {asset["name"]: asset.get("digest") for asset in uploaded["assets"]}
    for path in assets:
        if digests.get(path.name) != "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest():
            raise ValueError(f"Uploaded asset failed checksum verification: {path.name}")
    with tempfile.TemporaryDirectory(prefix="maestro-notes-") as scratch:
        notes = Path(scratch) / "notes.txt"
        notes.write_text(release_notes(project, repository, current) +
                         f"\n[Full changelog](https://github.com/{project}/commits/{tag})\n")
        run("gh", "release", "edit", tag, "--notes-file", str(notes))
    finalize_release(project, tag, current)
    for asset in release["assets"]:
        if asset["name"] in {"release.json", "sdk-2.json", "sdk-3.json"}:
            api(f"repos/{project}/releases/assets/{asset['id']}", "--method", "DELETE")
    print(f"Updated {tag} with binaries and the existing image {repository}:{current}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "plan", "candidate", "publish", "repair"))
    parser.add_argument("--source-dir", type=Path, default=ROOT)
    args = parser.parse_args()
    ROOT = args.source_dir.resolve()
    {"check": version, "plan": plan, "candidate": candidate, "publish": publish, "repair": repair}[args.command]()
