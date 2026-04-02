import json
from datetime import datetime, timezone
from typing import Any

from redis import Redis

from app.config import settings
from app.services.sync_locks import sync_lock_key
from app.services.sync_scheduler import StoreSyncScheduler
from app.utils.redis_cache import get_redis


SYNC_STATUS_TTL_SECONDS = 24 * 60 * 60
STALE_QUEUED_GRACE_SECONDS = 90
SYNC_RUNTIME_KINDS: tuple[str, ...] = ("full", "products", "stocks", "supplies", "reports", "finance", "closed_months")

redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)


def _status_key(store_id: int) -> str:
    return f"sync:status:{store_id}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _kind_default(kind: str) -> dict[str, Any]:
    return {
        "kind": kind,
        "status": "idle",
        "message": "Нет запусков",
        "task_id": None,
        "phase": None,
        "phase_label": None,
        "progress_percent": 0,
        "queued_at": None,
        "started_at": None,
        "finished_at": None,
        "updated_at": None,
        "last_success_at": None,
        "last_failure_at": None,
    }


def _empty_status(store_id: int) -> dict[str, Any]:
    return {
        "store_id": store_id,
        "status": "idle",
        "message": "Синхронизация не запускалась",
        "task_id": None,
        "queued_at": None,
        "started_at": None,
        "finished_at": None,
        "updated_at": None,
        "active_sync_kinds": [],
        "sync_kinds": {},
    }


def _latest_timestamp(*values: Any) -> Any:
    present = [value for value in values if value]
    if not present:
        return None
    return max(present)


def _recompute_payload(store_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    sync_kinds = dict(payload.get("sync_kinds") or {})
    normalized_sync_kinds: dict[str, Any] = {}
    for kind, raw_item in sync_kinds.items():
        item = dict(raw_item or {})
        status = str(item.get("status") or "")
        phase = str(item.get("phase") or "")
        phase_label = str(item.get("phase_label") or "")
        progress_percent = int(item.get("progress_percent") or 0)
        last_success_at = item.get("last_success_at")
        current_month = item.get("current_month")
        looks_completed = (
            status == "running"
            and progress_percent >= 100
            and current_month in {None, "", "null"}
            and (phase in {"done", "completed"} or phase_label in {"Готово", "Завершено"})
            and last_success_at
        )
        if looks_completed:
            item["status"] = "success"
            item["phase"] = "completed"
            item["phase_label"] = "Завершено"
            item["finished_at"] = item.get("finished_at") or last_success_at
        normalized_sync_kinds[kind] = item
    sync_kinds = normalized_sync_kinds
    running_sync_kinds = sorted(
        kind for kind, item in sync_kinds.items() if (item or {}).get("status") == "running"
    )
    queued_sync_kinds = sorted(
        kind for kind, item in sync_kinds.items() if (item or {}).get("status") == "queued"
    )

    full_kind = sync_kinds.get("full") or {}
    latest_finished = _latest_timestamp(*[(item or {}).get("finished_at") for item in sync_kinds.values()])
    latest_started = _latest_timestamp(*[(item or {}).get("started_at") for item in sync_kinds.values()])
    latest_queued = _latest_timestamp(*[(item or {}).get("queued_at") for item in sync_kinds.values()])

    status = payload.get("status") or "idle"
    message = payload.get("message") or "Синхронизация не запускалась"
    task_id = payload.get("task_id")
    queued_at = payload.get("queued_at")
    started_at = payload.get("started_at")
    finished_at = payload.get("finished_at")

    if "full" in running_sync_kinds:
        status = full_kind.get("status") or "running"
        message = full_kind.get("message") or "Полная синхронизация выполняется"
        queued_at = full_kind.get("queued_at") or queued_at
        started_at = full_kind.get("started_at") or started_at
        finished_at = None
    elif full_kind.get("status") == "queued":
        status = "queued"
        message = full_kind.get("message") or "Полная синхронизация в очереди"
        queued_at = full_kind.get("queued_at") or queued_at
        started_at = full_kind.get("started_at")
        finished_at = None
    elif full_kind:
        if running_sync_kinds:
            status = "running"
            message = "Выполняются фоновые синхронизации"
            queued_at = payload.get("queued_at") or latest_queued
            started_at = latest_started or payload.get("started_at")
            finished_at = full_kind.get("finished_at") or latest_finished
        elif queued_sync_kinds:
            status = "queued"
            message = "Фоновые синхронизации ждут запуска"
            queued_at = latest_queued or payload.get("queued_at")
            started_at = latest_started or payload.get("started_at")
            finished_at = full_kind.get("finished_at") or latest_finished
        else:
            status = full_kind.get("status") or "idle"
            message = full_kind.get("message") or "Синхронизация не запускалась"
            queued_at = full_kind.get("queued_at")
            started_at = full_kind.get("started_at")
            finished_at = full_kind.get("finished_at")
    elif running_sync_kinds:
        status = "running"
        message = "Выполняются фоновые синхронизации"
        queued_at = latest_queued or payload.get("queued_at")
        started_at = latest_started or payload.get("started_at")
        finished_at = None
    elif queued_sync_kinds:
        status = "queued"
        message = "Фоновые синхронизации ждут запуска"
        queued_at = latest_queued or payload.get("queued_at")
        started_at = latest_started or payload.get("started_at")
        finished_at = None
    elif sync_kinds:
        latest_item = max(
            sync_kinds.values(),
            key=lambda item: _latest_timestamp(
                item.get("finished_at"),
                item.get("started_at"),
                item.get("queued_at"),
                item.get("updated_at"),
            ) or "",
        )
        status = latest_item.get("status") or "idle"
        message = latest_item.get("message") or "Синхронизация не запускалась"
        queued_at = latest_item.get("queued_at")
        started_at = latest_item.get("started_at")
        finished_at = latest_item.get("finished_at")

    payload.update(
        {
            "store_id": store_id,
            "status": status,
            "message": message,
            "task_id": task_id,
            "queued_at": queued_at,
            "started_at": started_at,
            "finished_at": finished_at,
            "active_sync_kinds": running_sync_kinds,
            "updated_at": _now_iso(),
            "sync_kinds": sync_kinds,
        }
    )
    return payload


def get_store_sync_status(store_id: int) -> dict[str, Any]:
    raw = redis_client.get(_status_key(store_id))
    if not raw:
        return _empty_status(store_id)
    original_payload = json.loads(raw)
    payload = json.loads(raw)
    normalized = _recompute_payload(store_id, payload)
    original_cmp = {**original_payload}
    normalized_cmp = {**normalized}
    original_cmp.pop("updated_at", None)
    normalized_cmp.pop("updated_at", None)
    if json.dumps(normalized_cmp, sort_keys=True) != json.dumps(original_cmp, sort_keys=True):
        redis_client.set(_status_key(store_id), json.dumps(normalized), ex=SYNC_STATUS_TTL_SECONDS)
    else:
        normalized["updated_at"] = original_payload.get("updated_at")
    return normalized


def set_store_sync_status(store_id: int, **updates: Any) -> dict[str, Any]:
    current = get_store_sync_status(store_id)
    payload = {
        **current,
        **updates,
        "store_id": store_id,
    }
    payload = _recompute_payload(store_id, payload)
    redis_client.set(_status_key(store_id), json.dumps(payload), ex=SYNC_STATUS_TTL_SECONDS)
    return payload


def set_store_sync_kind_status(store_id: int, kind: str, **updates: Any) -> dict[str, Any]:
    current = get_store_sync_status(store_id)
    sync_kinds = dict(current.get("sync_kinds") or {})
    kind_payload = {**_kind_default(kind), **dict(sync_kinds.get(kind) or {}), **updates}
    kind_payload["kind"] = kind
    kind_payload["updated_at"] = _now_iso()
    sync_kinds[kind] = kind_payload
    return set_store_sync_status(store_id, sync_kinds=sync_kinds)


def mark_store_sync_status(store_id: int, **updates: Any) -> dict[str, Any]:
    return set_store_sync_status(store_id, **updates)


def mark_store_sync_queued(store_id: int, task_id: str | None = None) -> dict[str, Any]:
    return set_store_sync_status(
        store_id,
        task_id=task_id,
        status="queued",
        message="Полная синхронизация в очереди",
        queued_at=_now_iso(),
        started_at=None,
        finished_at=None,
    )


def mark_store_sync_running(store_id: int, task_id: str, message: str = "Полная синхронизация выполняется") -> dict[str, Any]:
    current = get_store_sync_status(store_id)
    return set_store_sync_status(
        store_id,
        status="running",
        message=message,
        task_id=task_id,
        queued_at=current.get("queued_at") or _now_iso(),
        started_at=_now_iso(),
        finished_at=None,
    )


def mark_store_sync_success(store_id: int, task_id: str, message: str = "Полная синхронизация завершена") -> dict[str, Any]:
    current = get_store_sync_status(store_id)
    return set_store_sync_status(
        store_id,
        status="success",
        message=message,
        task_id=task_id,
        queued_at=current.get("queued_at"),
        started_at=current.get("started_at"),
        finished_at=_now_iso(),
    )


def mark_store_sync_failed(store_id: int, task_id: str, message: str) -> dict[str, Any]:
    current = get_store_sync_status(store_id)
    return set_store_sync_status(
        store_id,
        status="failed",
        message=message,
        task_id=task_id,
        queued_at=current.get("queued_at"),
        started_at=current.get("started_at"),
        finished_at=_now_iso(),
    )


def mark_store_sync_cancelled(store_id: int, task_id: str | None = None, message: str = "Синхронизация остановлена") -> dict[str, Any]:
    current = get_store_sync_status(store_id)
    return set_store_sync_status(
        store_id,
        status="cancelled",
        message=message,
        task_id=task_id or current.get("task_id"),
        queued_at=current.get("queued_at"),
        started_at=current.get("started_at"),
        finished_at=_now_iso(),
    )


def mark_store_kind_queued(
    store_id: int,
    kind: str,
    message: str = "Синхронизация в очереди",
    **extra: Any,
) -> dict[str, Any]:
    return set_store_sync_kind_status(
        store_id,
        kind,
        status="queued",
        message=message,
        phase="queued",
        phase_label="В очереди",
        progress_percent=0,
        queued_at=_now_iso(),
        started_at=None,
        finished_at=None,
        **extra,
    )


def mark_store_kind_running(
    store_id: int,
    kind: str,
    message: str = "Синхронизация выполняется",
    **extra: Any,
) -> dict[str, Any]:
    current = get_store_sync_status(store_id).get("sync_kinds", {}).get(kind) or {}
    return set_store_sync_kind_status(
        store_id,
        kind,
        status="running",
        message=message,
        phase=current.get("phase") or "running",
        phase_label=current.get("phase_label") or "Выполняется",
        progress_percent=current.get("progress_percent") if current.get("progress_percent") is not None else 10,
        queued_at=current.get("queued_at") or _now_iso(),
        started_at=_now_iso(),
        finished_at=None,
        **extra,
    )


def mark_store_kind_progress(
    store_id: int,
    kind: str,
    *,
    progress_percent: int,
    message: str,
    phase: str,
    phase_label: str,
    **extra: Any,
) -> dict[str, Any]:
    current = get_store_sync_status(store_id).get("sync_kinds", {}).get(kind) or {}
    return set_store_sync_kind_status(
        store_id,
        kind,
        status="running",
        message=message,
        phase=phase,
        phase_label=phase_label,
        progress_percent=max(0, min(int(progress_percent), 100)),
        queued_at=current.get("queued_at") or _now_iso(),
        started_at=current.get("started_at") or _now_iso(),
        finished_at=None,
        **extra,
    )


def mark_store_kind_success(
    store_id: int,
    kind: str,
    message: str = "Синхронизация завершена",
    **extra: Any,
) -> dict[str, Any]:
    current = get_store_sync_status(store_id).get("sync_kinds", {}).get(kind) or {}
    finished_at = _now_iso()
    return set_store_sync_kind_status(
        store_id,
        kind,
        status="success",
        message=message,
        phase="completed",
        phase_label="Завершено",
        progress_percent=100,
        queued_at=current.get("queued_at"),
        started_at=current.get("started_at"),
        finished_at=finished_at,
        last_success_at=finished_at,
        **extra,
    )


def mark_store_kind_failed(store_id: int, kind: str, message: str) -> dict[str, Any]:
    current = get_store_sync_status(store_id).get("sync_kinds", {}).get(kind) or {}
    finished_at = _now_iso()
    return set_store_sync_kind_status(
        store_id,
        kind,
        status="failed",
        message=message,
        phase="failed",
        phase_label="Ошибка",
        progress_percent=current.get("progress_percent") if current.get("progress_percent") is not None else 0,
        queued_at=current.get("queued_at"),
        started_at=current.get("started_at"),
        finished_at=finished_at,
        last_failure_at=finished_at,
    )


def mark_store_kind_skipped(store_id: int, kind: str, message: str) -> dict[str, Any]:
    current = get_store_sync_status(store_id).get("sync_kinds", {}).get(kind) or {}
    return set_store_sync_kind_status(
        store_id,
        kind,
        status="skipped",
        message=message,
        phase="skipped",
        phase_label="Пропущено",
        progress_percent=current.get("progress_percent") if current.get("progress_percent") is not None else 0,
        queued_at=current.get("queued_at"),
        started_at=current.get("started_at"),
        finished_at=_now_iso(),
    )


def mark_store_kind_cancelled(store_id: int, kind: str, message: str = "Синхронизация остановлена") -> dict[str, Any]:
    current = get_store_sync_status(store_id).get("sync_kinds", {}).get(kind) or {}
    finished_at = _now_iso()
    return set_store_sync_kind_status(
        store_id,
        kind,
        status="cancelled",
        message=message,
        phase="cancelled",
        phase_label="Отменено",
        progress_percent=current.get("progress_percent") if current.get("progress_percent") is not None else 0,
        queued_at=current.get("queued_at"),
        started_at=current.get("started_at"),
        finished_at=finished_at,
    )


async def reconcile_store_sync_runtime_state(
    store_id: int,
    *,
    queued_grace_seconds: int = STALE_QUEUED_GRACE_SECONDS,
) -> dict[str, Any]:
    payload = get_store_sync_status(store_id)
    scheduler_state = await StoreSyncScheduler().get_state(store_id)
    active = dict(scheduler_state.get("active") or {})
    active_kind = str(active.get("kind") or "")

    redis = await get_redis()
    lock_kinds: set[str] = set()
    if redis:
        for kind in SYNC_RUNTIME_KINDS:
            try:
                if await redis.exists(sync_lock_key(kind, store_id)):
                    lock_kinds.add(kind)
            except Exception:
                continue

    runtime_running = set(lock_kinds)
    if active_kind:
        runtime_running.add(active_kind)

    runtime_queued = set((scheduler_state.get("queued_background") or {}).keys())
    queued_after_full = dict(scheduler_state.get("queued_after_full") or {})
    queued_after_full_kind = str(queued_after_full.get("kind") or "")
    if queued_after_full_kind:
        runtime_queued.add(queued_after_full_kind)

    sync_kinds = dict(payload.get("sync_kinds") or {})
    if not sync_kinds:
        return payload

    now = datetime.now(timezone.utc)
    changed = False
    for kind, raw_item in list(sync_kinds.items()):
        item = dict(raw_item or {})
        status = str(item.get("status") or "")

        if status == "running" and kind not in runtime_running:
            item.update(
                {
                    "status": "cancelled",
                    "message": "Статус сброшен: активная задача не найдена",
                    "phase": "cancelled",
                    "phase_label": "Отменено",
                    "finished_at": item.get("finished_at") or _now_iso(),
                    "task_id": None,
                }
            )
            sync_kinds[kind] = item
            changed = True
            continue

        if status == "queued" and kind not in runtime_running and kind not in runtime_queued:
            queued_at = _parse_iso(item.get("queued_at"))
            queued_age_seconds = int((now - queued_at).total_seconds()) if queued_at else queued_grace_seconds + 1
            if queued_age_seconds >= queued_grace_seconds:
                item.update(
                    {
                        "status": "cancelled",
                        "message": "Очередь очищена: задача не найдена",
                        "phase": "cancelled",
                        "phase_label": "Отменено",
                        "finished_at": item.get("finished_at") or _now_iso(),
                        "task_id": None,
                    }
                )
                sync_kinds[kind] = item
                changed = True

    if not changed:
        return payload

    updates: dict[str, Any] = {"sync_kinds": sync_kinds}
    if not runtime_running:
        updates["task_id"] = None
    return set_store_sync_status(store_id, **updates)
