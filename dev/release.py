"""Version-bump releases: plan, build a candidate, then publish a tested digest.

VERSION contains SemVer without a v prefix or build metadata. A change on main
creates a release; rerun the original workflow to resume a failed publication.
Candidates are retained by commit so retries use the same image bytes.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


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
    return json.loads(output("gh", "api", path, *args))


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
    assets = [ROOT / "dist" / f"sdk-{major}.json" for major in (2, 3)]
    if not all(path.is_file() for path in assets):
        raise ValueError("Expected both SDK version manifests in dist before publishing")
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
            notes.write_text(
                f"Container: `{repository}:{current}`\n\n"
                f"Immutable image: `{repository}@{digest}`\n\n"
                f"Commit: `{revision}`\n\n"
                "The release assets record the image digest and tested DBOS SDK versions.\n"
            )
            run(*args, "--notes-file", str(notes))
    promote(repository, f"sha-{revision}", digest)
    promote(repository, current, digest)
    with tempfile.TemporaryDirectory(prefix="maestro-release-") as scratch:
        manifest = Path(scratch) / "release.json"
        manifest.write_text(json.dumps({
            "version": current, "revision": revision, "image": f"{repository}@{digest}",
            "platforms": ["linux/amd64", "linux/arm64"],
        }, indent=2) + "\n")
        run("gh", "release", "upload", tag, str(manifest), *(str(path) for path in assets), "--clobber")
    # Attach everything before publication so immutable GitHub releases work too.
    run("gh", "release", "edit", tag, "--draft=false", "--latest=false")
    print(f"Published {tag}: {repository}@{digest}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "plan", "candidate", "publish"))
    command = parser.parse_args().command
    {"check": version, "plan": plan, "candidate": candidate, "publish": publish}[command]()
