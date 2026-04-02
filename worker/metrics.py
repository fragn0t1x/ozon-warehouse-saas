from __future__ import annotations

import os
from pathlib import Path
from time import perf_counter

from celery.signals import celeryd_init, task_postrun, task_prerun, worker_ready
from loguru import logger
from prometheus_client import CollectorRegistry, Counter, Histogram, multiprocess, start_http_server


METRICS_DIR = Path(os.getenv("PROMETHEUS_MULTIPROC_DIR", "/tmp/worker-prometheus"))
os.environ.setdefault("PROMETHEUS_MULTIPROC_DIR", str(METRICS_DIR))

TASKS_TOTAL = Counter(
    "ozon_celery_tasks_total",
    "Total number of finished Celery tasks",
    ["task_name", "state"],
)

TASK_RUNTIME_SECONDS = Histogram(
    "ozon_celery_task_runtime_seconds",
    "Execution time for Celery tasks",
    ["task_name"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0),
)

_TASK_START_TIMES: dict[str, float] = {}
_METRICS_SERVER_STARTED = False


@celeryd_init.connect
def prepare_metrics_dir(**_kwargs):
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    for metrics_file in METRICS_DIR.glob("*.db"):
        metrics_file.unlink(missing_ok=True)


@worker_ready.connect
def start_metrics_server(**_kwargs):
    global _METRICS_SERVER_STARTED
    if _METRICS_SERVER_STARTED:
        return

    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    port = int(os.getenv("WORKER_METRICS_PORT", "9808"))
    start_http_server(port, addr="0.0.0.0", registry=registry)
    _METRICS_SERVER_STARTED = True
    logger.info("📈 Worker metrics server started on port {}", port)


@task_prerun.connect
def mark_task_start(task_id=None, **_kwargs):
    if task_id:
        _TASK_START_TIMES[task_id] = perf_counter()


@task_postrun.connect
def observe_task_finish(task_id=None, task=None, state=None, **_kwargs):
    task_name = getattr(task, "name", "unknown")
    task_state = state or "UNKNOWN"

    TASKS_TOTAL.labels(task_name=task_name, state=task_state).inc()

    started_at = _TASK_START_TIMES.pop(task_id, None) if task_id else None
    if started_at is not None:
        TASK_RUNTIME_SECONDS.labels(task_name=task_name).observe(perf_counter() - started_at)
