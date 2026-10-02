#!/usr/bin/env bash
# Exercise the release image under the Compose runtime restrictions.
set -euo pipefail

image="${IMAGE:-maestro:local}"
arch="$(docker image inspect --format '{{.Architecture}}' "$image")"
scratch="$(mktemp -d)"
container=''
trap 'if [[ -n "$container" ]]; then docker rm --force "$container" >/dev/null; fi; rm -rf "$scratch"' EXIT

docker compose config --format json | jq -e '.services.maestro |
  .user == "65532:65532" and .read_only and .cap_drop == ["ALL"] and
  .security_opt == ["no-new-privileges:true"] and .pids_limit == 128 and
  all(.ports[]; .host_ip == "127.0.0.1")' >/dev/null

CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath -o "$scratch/probe" tests/container/probe_linux.go
container="$(docker run --detach --platform "linux/$arch" --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true --pids-limit 128 --publish 127.0.0.1::8090 \
  --mount "type=bind,src=$scratch/probe,dst=/probe,readonly" "$image")"
port="$(docker inspect "$container" | jq -r '.[0].NetworkSettings.Ports["8090/tcp"][0].HostPort')"
base="http://127.0.0.1:$port"
curl --noproxy '*' -fsS --retry 30 --retry-connrefused --retry-delay 1 --max-time 2 \
  "$base/healthz" | jq -e '. == {"status": true}' >/dev/null
curl --noproxy '*' -fsS "$base/" > "$scratch/index.html"
curl --noproxy '*' -fsS "$base/openapi.json" > "$scratch/openapi.json"
curl --noproxy '*' -fsS "$base/static/htmx.min.js" > "$scratch/htmx.js"
grep -qi Maestro "$scratch/index.html"
grep -qi htmx "$scratch/htmx.js"
jq -e '.openapi' "$scratch/openapi.json" >/dev/null
if [[ -n "${EXPECTED_VERSION:-}" ]]; then
  test "$(docker exec "$container" /maestro --version)" = "maestro $EXPECTED_VERSION"
  revision="${EXPECTED_REVISION:0:12}"
  [[ "${EXPECTED_MODIFIED:-false}" != true ]] || revision="$revision.dirty"
  grep -Fq "aria-label=\"Maestro version\">$EXPECTED_VERSION&#43;$revision</span>" "$scratch/index.html"
  jq -e --arg version "$EXPECTED_VERSION" '.info.version == $version' "$scratch/openapi.json" >/dev/null
fi
docker exec "$container" /probe
docker stop --timeout 15 "$container"
test "$(docker inspect --format '{{.State.ExitCode}}' "$container")" = 0
echo "PASS linux/$arch HTTP, assets, runtime restrictions and SIGTERM"
