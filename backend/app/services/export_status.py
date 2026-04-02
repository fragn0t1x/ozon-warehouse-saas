import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from redis import Redis

from app.config import settings


EXPORT_STATUS_TTL_SECONDS = 24 * 60 * 60
EXPORT_FILE_TTL_SECONDS = 24 * 60 * 60
EXPORT_ROOT_DIR = "/shared_exports"
EXPORT_HISTORY_LIMIT = 5

redis_client = Redis.from_url(settings.REDIS_URL, decode_responses=True)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def export_status_key(kind: str, store_id: int, user_id: int) -> str:
    return f"export:status:{kind}:{store_id}:{user_id}"


def export_lock_key(kind: str, store_id: int) -> str:
    return f"export:lock:{kind}:{store_id}"


def ensure_export_root_dir() -> str:
    os.makedirs(EXPORT_ROOT_DIR, exist_ok=True)
    try:
        os.chmod(EXPORT_ROOT_DIR, 0o777)
    except Exception:
        pass
    cleanup_expired_export_files()
    return EXPORT_ROOT_DIR


def cleanup_expired_export_files(max_age_seconds: int = EXPORT_FILE_TTL_SECONDS) -> None:
    if not os.path.isdir(EXPORT_ROOT_DIR):
        return

    threshold = time.time() - max(int(max_age_seconds), 1)
    try:
        for entry in os.scandir(EXPORT_ROOT_DIR):
            if not entry.is_file():
                continue
            if not entry.name.lower().endswith(".xlsx"):
                continue
            try:
                if entry.stat().st_mtime < threshold:
                    os.remove(entry.path)
            except FileNotFoundError:
                continue
            except Exception:
                continue
    except FileNotFoundError:
        return
    except Exception:
        return


def _empty_status(kind: str, store_id: int, user_id: int) -> dict[str, Any]:
    return {
        "kind": kind,
        "store_id": store_id,
        "user_id": user_id,
        "status": "idle",
        "message": "Отчет еще не формировался",
        "phase": None,
        "phase_label": None,
        "progress_percent": 0,
        "queued_at": None,
        "started_at": None,
        "finished_at": None,
        "updated_at": None,
        "task_id": None,
        "file_path": None,
        "file_name": None,
        "download_url": None,
        "order_window_days": None,
        "selection_label": None,
        "processed_items": 0,
        "total_items": 0,
        "last_success_at": None,
        "last_failure_at": None,
        "error": None,
        "recent_runs": [],
    }


def _raw_export_status(kind: str, store_id: int, user_id: int) -> dict[str, Any]:
    raw = redis_client.get(export_status_key(kind, store_id, user_id))
    if not raw:
        return _empty_status(kind, store_id, user_id)
    try:
        payload = json.loads(raw)
    except Exception:
        return _empty_status(kind, store_id, user_id)
    return {**_empty_status(kind, store_id, user_id), **payload}


def get_export_status(kind: str, store_id: int, user_id: int) -> dict[str, Any]:
    cleanup_expired_export_files()
    payload = _raw_export_status(kind, store_id, user_id)
    file_path = str(payload.get("file_path") or "").strip()
    if file_path and not os.path.exists(file_path):
        payload = {
            **payload,
            "status": "idle",
            "message": "Готовый отчет удален. Сформируй Excel заново",
            "phase": None,
            "phase_label": None,
            "progress_percent": 0,
            "file_path": None,
            "file_name": None,
            "download_url": None,
            "updated_at": _now_iso(),
        }
        redis_client.set(
            export_status_key(kind, store_id, user_id),
            json.dumps(payload),
            ex=EXPORT_STATUS_TTL_SECONDS,
        )
    return payload


def set_export_status(kind: str, store_id: int, user_id: int, **updates: Any) -> dict[str, Any]:
    cleanup_expired_export_files()
    current = _raw_export_status(kind, store_id, user_id)
    payload = {
        **current,
        **updates,
        "kind": kind,
        "store_id": store_id,
        "user_id": user_id,
        "updated_at": _now_iso(),
    }
    redis_client.set(export_status_key(kind, store_id, user_id), json.dumps(payload), ex=EXPORT_STATUS_TTL_SECONDS)
    return payload


def _push_recent_run(
    current: dict[str, Any],
    *,
    status: str,
    message: str,
    order_window_days: int | None,
    selection_label: str | None = None,
    file_name: str | None = None,
    error: str | None = None,
) -> list[dict[str, Any]]:
    current_runs = current.get("recent_runs")
    if not isinstance(current_runs, list):
        current_runs = []

    entry = {
        "status": status,
        "message": message,
        "order_window_days": order_window_days,
        "selection_label": selection_label,
        "file_name": file_name,
        "error": error,
        "finished_at": _now_iso(),
    }
    return [entry, *current_runs][:EXPORT_HISTORY_LIMIT]


def _cleanup_previous_file(current: dict[str, Any], next_file_path: str | None = None) -> None:
    previous_path = str(current.get("file_path") or "").strip()
    if not previous_path or previous_path == str(next_file_path or "").strip():
        return
    try:
        if os.path.exists(previous_path):
            os.remove(previous_path)
    except Exception:
        pass


def mark_export_queued(kind: str, store_id: int, user_id: int, *, task_id: str | None, message: str, order_window_days: int | None = None, selection_label: str | None = None) -> dict[str, Any]:
    current = get_export_status(kind, store_id, user_id)
    return set_export_status(
        kind,
        store_id,
        user_id,
        status="queued",
        message=message,
        phase="queued",
        phase_label="В очереди",
        progress_percent=5,
        queued_at=_now_iso(),
        started_at=None,
        finished_at=None,
        task_id=task_id,
        order_window_days=order_window_days,
        selection_label=selection_label,
        error=None,
        file_path=current.get("file_path"),
        file_name=current.get("file_name"),
        download_url=current.get("download_url"),
        processed_items=0,
        total_items=0,
    )


def mark_export_running(
    kind: str,
    store_id: int,
    user_id: int,
    *,
    task_id: str | None,
    message: str,
    phase: str,
    phase_label: str,
    progress_percent: int,
    order_window_days: int | None = None,
    selection_label: str | None = None,
    processed_items: int | None = None,
    total_items: int | None = None,
) -> dict[str, Any]:
    current = get_export_status(kind, store_id, user_id)
    return set_export_status(
        kind,
        store_id,
        user_id,
        status="running",
        message=message,
        phase=phase,
        phase_label=phase_label,
        progress_percent=max(0, min(progress_percent, 99)),
        queued_at=current.get("queued_at") or _now_iso(),
        started_at=current.get("started_at") or _now_iso(),
        finished_at=None,
        task_id=task_id,
        order_window_days=order_window_days if order_window_days is not None else current.get("order_window_days"),
        selection_label=selection_label if selection_label is not None else current.get("selection_label"),
        processed_items=processed_items if processed_items is not None else current.get("processed_items") or 0,
        total_items=total_items if total_items is not None else current.get("total_items") or 0,
        error=None,
    )


def mark_export_success(
    kind: str,
    store_id: int,
    user_id: int,
    *,
    task_id: str | None,
    message: str,
    file_path: str,
    file_name: str,
    download_url: str,
    order_window_days: int | None = None,
    selection_label: str | None = None,
    processed_items: int = 0,
    total_items: int = 0,
) -> dict[str, Any]:
    current = get_export_status(kind, store_id, user_id)
    _cleanup_previous_file(current, file_path)
    return set_export_status(
        kind,
        store_id,
        user_id,
        status="success",
        message=message,
        phase="done",
        phase_label="Готово",
        progress_percent=100,
        task_id=task_id,
        started_at=current.get("started_at") or _now_iso(),
        finished_at=_now_iso(),
        file_path=file_path,
        file_name=file_name,
        download_url=download_url,
        order_window_days=order_window_days if order_window_days is not None else current.get("order_window_days"),
        selection_label=selection_label if selection_label is not None else current.get("selection_label"),
        processed_items=processed_items,
        total_items=total_items,
        last_success_at=_now_iso(),
        error=None,
        recent_runs=_push_recent_run(
            current,
            status="success",
            message=message,
            order_window_days=order_window_days if order_window_days is not None else current.get("order_window_days"),
            selection_label=selection_label if selection_label is not None else current.get("selection_label"),
            file_name=file_name,
        ),
    )


def mark_export_failed(kind: str, store_id: int, user_id: int, *, task_id: str | None, message: str, error: str | None = None) -> dict[str, Any]:
    current = get_export_status(kind, store_id, user_id)
    return set_export_status(
        kind,
        store_id,
        user_id,
        status="error",
        message=message,
        phase="error",
        phase_label="Ошибка",
        progress_percent=current.get("progress_percent") or 0,
        task_id=task_id,
        started_at=current.get("started_at") or current.get("queued_at"),
        finished_at=_now_iso(),
        last_failure_at=_now_iso(),
        error=error or message,
        recent_runs=_push_recent_run(
            current,
            status="error",
            message=message,
            order_window_days=current.get("order_window_days"),
            selection_label=current.get("selection_label"),
            file_name=current.get("file_name"),
            error=error or message,
        ),
    )


def acquire_export_lock(kind: str, store_id: int, ttl_seconds: int = 60 * 60) -> bool:
    return bool(redis_client.set(export_lock_key(kind, store_id), "1", ex=max(int(ttl_seconds), 1), nx=True))


def release_export_lock(kind: str, store_id: int) -> None:
    redis_client.delete(export_lock_key(kind, store_id))


def has_export_lock(kind: str, store_id: int) -> bool:
    return bool(redis_client.exists(export_lock_key(kind, store_id)))


def clear_export_status(kind: str, store_id: int, user_id: int) -> dict[str, Any]:
    current = get_export_status(kind, store_id, user_id)
    _cleanup_previous_file(current)
    redis_client.delete(export_status_key(kind, store_id, user_id))
    return _empty_status(kind, store_id, user_id)
