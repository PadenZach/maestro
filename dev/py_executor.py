"""
Dev harness: a minimal DBOS application that connects to a locally running
`maestro` (the Go port of DBOS Conductor) as a live executor.

The DBOS client builds its WebSocket URL as
    {conductor_url}/websocket/{name}/{conductor_key}
so `conductor_url` here is the base address of the Go server.

Prerequisites
-------------
1. A local Postgres (DBOS needs it for its system database). The dbos-transact-py
   repo ships a starter:
       export PGPASSWORD=dbos
       python3 dbos/_templates/dbos-db-starter/start_postgres_docker.py
2. The `dbos` package importable. Easiest is to run inside the dbos-transact-py
   project environment, e.g. from that repo:
       uv run python /Users/zpaden/workspace/maestro/dev/py_executor.py

Usage
-----
Terminal 1 — start maestro:
    cd /Users/zpaden/workspace/maestro
    go run ./cmd/maestro --key dev-key

Terminal 2 — start this executor:
    DBOS_SYSTEM_DATABASE_URL=postgresql://postgres:dbos@localhost:5432/conductor_dev \
    uv run python dev/py_executor.py

Verify:
    curl -s localhost:8090/api/executors | python3 -m json.tool
maestro should also log: "executor connected".

Tip: `./dev/executor_connection_demo.sh` runs the manual Docker example.
"""

import os
import time

from dbos import DBOS, DBOSConfig

config: DBOSConfig = {
    "name": os.environ.get("CONDUCTOR_APP_NAME", "py-dev-app"),
    "system_database_url": os.environ.get(
        "DBOS_SYSTEM_DATABASE_URL",
        "postgresql://postgres:dbos@localhost:5432/conductor_dev",
    ),
    "conductor_key": os.environ.get("DBOS_CONDUCTOR_KEY", "dev-key"),
    "conductor_url": os.environ.get("DBOS_CONDUCTOR_URL", "ws://localhost:8090"),
    "conductor_executor_metadata": {"role": "dev-harness"},
}

dbos = DBOS(config=config)


@DBOS.workflow()
def heartbeat_workflow(n: int) -> int:
    DBOS.logger.info(f"heartbeat workflow run {n}")
    return n


if __name__ == "__main__":
    DBOS.launch()
    print("DBOS launched; executor should now be connected to conductor.")
    i = 0
    try:
        while True:
            heartbeat_workflow(i)
            i += 1
            time.sleep(10)
    except KeyboardInterrupt:
        print("shutting down")
        DBOS.destroy()
