"""
Observability demo harness: a DBOS application with a multi-step workflow and a
queue, so the maestro console has something interesting to render (a gantt step
timeline, queued workflows, etc.).

It runs a handful of `agentic_research_workflow` invocations on startup — some
called directly, some enqueued — then idles so the executor stays connected and
the console stays populated. Wire-up (conductor key/url) mirrors py_executor.py.
"""

import os
import time

from dbos import DBOS, DBOSConfig, Queue

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

research_queue = Queue("research_queue")


@DBOS.step()
def search_hackernews(topic: str) -> list[str]:
    time.sleep(0.05)
    return [f"story about {topic} #{i}" for i in range(3)]


@DBOS.step()
def get_comments(story: str) -> int:
    time.sleep(0.03)
    return len(story)


@DBOS.step()
def synthesise(stories: list[str], counts: list[int]) -> str:
    time.sleep(0.02)
    return f"summary of {len(stories)} stories ({sum(counts)} chars)"


@DBOS.workflow()
def agentic_research_workflow(topic: str) -> str:
    stories = search_hackernews(topic)
    counts = [get_comments(s) for s in stories]
    return synthesise(stories, counts)


if __name__ == "__main__":
    DBOS.launch()
    print("DBOS launched; running sample workflows...")

    for topic in ("durable-execution", "postgres", "htmx"):
        result = agentic_research_workflow(topic)
        print(f"  direct  {topic!r:24} -> {result}")

    for topic in ("kafka", "websockets"):
        handle = research_queue.enqueue(agentic_research_workflow, topic)
        print(f"  enqueue {topic!r:24} -> {handle.workflow_id}")

    print("idling; the executor stays connected for the console.")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("shutting down")
        DBOS.destroy()
