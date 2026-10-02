# Maestro

Maestro is a Go service for inspecting DBOS workflows through a web console and
JSON API.

## Local development

With mise installed, install the configured tools and start the server:

```sh
mise install
mise run dev
```

Open the console at <http://127.0.0.1:8090>.

Run the development checks with Python 3 available:

```sh
mise run check
```

## Releases

The release version lives in [`VERSION`](VERSION). To release:

1. Increase the version, for example from `0.1.0` to `0.1.1`. Use SemVer without
   a `v` prefix or build metadata; prereleases such as `0.2.0-rc.1` are supported.
2. Commit the change and merge it into `main`.
3. GitHub Actions runs the checks, scans the image, and smoke-tests the exact
   candidate on Linux amd64 and arm64. After success, it publishes the versioned
   container image, creates the matching Git tag (for example `v0.1.1`), and
   publishes a GitHub Release. Tags are created automatically.

The release includes generated notes, the immutable image digest, and the tested
DBOS SDK versions. Prerelease versions are marked as prereleases on GitHub.
Ordinary pushes to `main` publish images tagged `sha-<full-commit-id>`; pull
requests only run checks. Images use explicit version or commit tags rather than
a moving `latest` tag.

To retry a failed release, rerun its original CI run in GitHub Actions. Retries
reuse the candidate image and refuse to overwrite existing tags with different
content. Fixes to an already published release require a new version bump.

The UI displays the version plus a short commit ID, such as
`0.1.0+1feaaa9ffe7a`; local builds with uncommitted changes append `.dirty`.
