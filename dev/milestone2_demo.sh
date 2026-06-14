#!/usr/bin/env bash
#
# Milestone 2 demo.
#
# Stands up maestro + a throwaway Postgres, connects a REAL DBOS executor that
# runs a multi-step workflow and a queue, then proves the observability-read
# surface end to end: lists workflows, fetches one workflow's steps, and lists
# queues — all proxied through the live executor over the wire protocol. Finally
# it points you at the HTML console.
#
# Requires: docker, uv, and the dbos-transact-py repo (for the `dbos` package).
# Override the repo location with DBOS_REPO=/path/to/dbos-transact-py.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBOS_REPO="${DBOS_REPO:-$ROOT/../dbos-transact-py}"
PORT="${MAESTRO_PORT:-8090}"
KEY="${MAESTRO_KEY:-dev-key}"
APP="py-dev-app"
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

if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "port :$PORT is already in use; stop the other process first"; exit 1
fi

echo "--- building + starting maestro on :$PORT ---"
( cd "$ROOT" && go build -o "$ROOT/bin/maestro" ./cmd/maestro )
"$ROOT/bin/maestro" --listen ":$PORT" --key "$KEY" &
MAESTRO_PID=$!
for _ in $(seq 1 30); do
  curl -fsS "localhost:$PORT/healthz" >/dev/null 2>&1 && break
  sleep 0.5
done
echo "maestro health: $(curl -s localhost:$PORT/healthz)"

echo "--- launching a REAL DBOS executor with steps + a queue ---"
DBOS_SYSTEM_DATABASE_URL="$DB_URL" \
DBOS_CONDUCTOR_KEY="$KEY" \
DBOS_CONDUCTOR_URL="ws://localhost:$PORT" \
  uv run --project "$DBOS_REPO" python "$ROOT/dev/py_executor_steps.py" >/tmp/maestro_demo_m2_py.log 2>&1 &
PY_PID=$!

echo "--- waiting for the executor to connect ---"
for _ in $(seq 1 40); do
  [ "$(count_executors)" = "1" ] && break
  sleep 1
done
[ "$(count_executors)" = "1" ] || { echo "executor never connected; see /tmp/maestro_demo_m2_py.log"; exit 1; }

# Give the sample workflows a moment to finish.
sleep 3

echo
echo "=== LIST_WORKFLOWS (proxied through the executor) ==="
curl -s "localhost:$PORT/api/$APP/workflows" | python3 -m json.tool | head -40

WF_ID=$(curl -s "localhost:$PORT/api/$APP/workflows" \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d[0]["WorkflowUUID"] if d else "")')

if [ -n "$WF_ID" ]; then
  echo
  echo "=== LIST_STEPS for $WF_ID ==="
  curl -s "localhost:$PORT/api/$APP/workflows/$WF_ID/steps" | python3 -m json.tool
fi

echo
echo "=== LIST_QUEUES ==="
curl -s "localhost:$PORT/api/$APP/queues" | python3 -m json.tool

echo
echo "=== DBOS Console ==="
echo "Open http://localhost:$PORT/ in a browser to explore:"
echo "  • apps list           http://localhost:$PORT/"
echo "  • workflows           http://localhost:$PORT/apps/$APP/workflows"
[ -n "$WF_ID" ] && echo "  • workflow detail     http://localhost:$PORT/apps/$APP/workflows/$WF_ID"
echo "  • queues              http://localhost:$PORT/apps/$APP/queues"
echo
echo "Press Ctrl-C to tear everything down."
wait "$MAESTRO_PID"
