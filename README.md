# Maestro

Maestro is a Go service for inspecting DBOS workflows through a web console and
JSON API.

## Install

Download Linux or macOS binaries from the
[releases page](https://github.com/PadenZach/maestro/releases), or install with
[mise's GitHub backend](https://mise.jdx.dev/dev-tools/backends/github.html):

```sh
mise use -g github:PadenZach/maestro@0.3.0
maestro --help
```

Archives contain `maestro` at their root. Each release
also includes SHA-256 checksums in `checksums.txt`. Mise selects the platform
automatically.
Private repository access requires a GitHub token with access to this repository.

The [tagged OCI image](https://github.com/PadenZach/maestro/pkgs/container/maestro?tag=0.3.0)
supports Linux amd64 and arm64:

```sh
docker pull ghcr.io/padenzach/maestro:0.3.0
```

## Local development

With mise installed, install the configured tools and start the server:

```sh
mise install
mise run dev
```

Open the console at <http://127.0.0.1:8090>. Use `--listen 0.0.0.0:8090`
to accept remote connections; authentication belongs to your gateway.

Run the development checks:

```sh
mise run check
```

`mise run sdk:test` tests current DBOS Python 2.31.x and 3.x releases.
For the pinned Postgres tests, run `mise run sdk:install`, set `POSTGRES18_BIN`
to your Postgres 18 bin directory, then run `mise run postgres:test` or
`mise run recovery:test`. `mise run dbosctl:test` also checks the pinned CLI.
The local UI demo runs with `uv run --project maestro-ui-demo maestro-ui-demo/app.py`.

## Workflow recovery

Maestro recovers pending workflows after an executor disconnects for 60 seconds.
Configure the wait with `--recovery-timeout` or `CONDUCTOR_RECOVERY_TIMEOUT`.
Recovery state is rebuilt after restarts; no local database is required. Run one
active Maestro instance.

## Releases

Bump [`VERSION`](VERSION), commit, and merge into `main`. CI checks the change
and publishes the container image, Git tag, and Linux/macOS binaries to GitHub
Releases. `mise run package` builds the four archives and `checksums.txt` locally
in `dist/release`; `mise run build` builds the native container image.
Rerun the original CI run to retry a failed release.
