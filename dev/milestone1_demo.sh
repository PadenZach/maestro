#!/usr/bin/env bash
#
# Milestone 1 demo.
#
# Stands up maestro + a throwaway Postgres, connects a REAL DBOS executor
# (the unmodified Python `dbos` client), and shows it register on connect and
# deregister on disconnect — proving wire compatibility end to end.
#
# Requires: docker, uv, and the dbos-transact-py repo (for the `dbos` package).
# Override the repo location with DBOS_REPO=/path/to/dbos-transact-py.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBOS_REPO="${DBOS_REPO:-$ROOT/../dbos-transact-py}"
PORT="${MAESTRO_PORT:-8090}"
KEY="${MAESTRO_KEY:-dev-key}"
PG_CONTAINER="maestro-demo-pg"
DB_URL="postgresql://postgres:dbos@localhost:5432/maestro_demo"

MAESTRO_PID=""
PY_PID=""

cleanup() {
  echo "--- cleanup ---"
  [ -n "$PY_PID" ] && kill "$PY_PID" 2>/dev/null || true
  [ -n "$MAESTRO_PID" ] && kill "$MAESTRO_PID" 2>/dev/null || true
  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null || { echo "docker is required"; exit 1; }
command -v uv >/dev/null || { echo "uv is required"; exit 1; }
[ -d "$DBOS_REPO" ] || { echo "dbos repo not found at $DBOS_REPO (set DBOS_REPO)"; exit 1; }

count_executors() {
  curl -s "localhost:$PORT/api/executors" \
    | python3 -c 'import sys,json;print(len(json.load(sys.stdin)))' 2>/dev/null || echo 0
}

echo "--- starting Postgres ($PG_CONTAINER) ---"
docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$PG_CONTAINER" -e POSTGRES_PASSWORD=dbos -p 5432:5432 postgres:16 >/dev/null
for _ in $(seq 1 30); do
  docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$PG_CONTAINER" psql -U postgres -c "CREATE DATABASE maestro_demo;" >/dev/null

# Fail fast if the port is already taken — otherwise a stale server could
# silently answer this demo instead of the binary we build below.
if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port :$PORT is already in use; stop the other process first"; exit 1
fi

echo "--- building + starting maestro on :$PORT ---"
# Build a real binary and run it directly. `go run` double-forks (go run ->
# compiled child), so killing its PID would leak the actual server; running the
# built binary means the PID we track IS the server.
( cd "$ROOT" && go build -o "$ROOT/bin/maestro" ./cmd/maestro )
"$ROOT/bin/maestro" --listen ":$PORT" --key "$KEY" &
MAESTRO_PID=$!
for _ in $(seq 1 30); do
  curl -fsS "localhost:$PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.5
done
echo "maestro health  : $(curl -s localhost:$PORT/healthz)"
echo "executors before: $(curl -s localhost:$PORT/api/executors)"

echo "--- launching a REAL DBOS executor (via $DBOS_REPO) ---"
DBOS_SYSTEM_DATABASE_URL="$DB_URL" \
DBOS_CONDUCTOR_KEY="$KEY" \
DBOS_CONDUCTOR_URL="ws://localhost:$PORT" \
  uv run --project "$DBOS_REPO" python "$ROOT/dev/py_executor.py" >/tmp/maestro_demo_py.log 2>&1 &
PY_PID=$!

echo "--- waiting for the executor to connect ---"
for _ in $(seq 1 40); do
  [ "$(count_executors)" = "1" ] && break
  sleep 1
done

echo
echo "=== CONNECTED EXECUTOR ==="
curl -s "localhost:$PORT/api/executors" | python3 -m json.tool
grep -iq "Connected to DBOS conductor" /tmp/maestro_demo_py.log \
  && echo "(client log confirms: \"Connected to DBOS conductor\")"

echo
echo "--- stopping the executor; it should deregister ---"
kill "$PY_PID" 2>/dev/null || true
PY_PID=""
for _ in $(seq 1 20); do
  [ "$(count_executors)" = "0" ] && break
  sleep 1
done
echo "executors after disconnect: $(curl -s localhost:$PORT/api/executors)"
echo
echo "=== milestone:1 demo complete ==="
