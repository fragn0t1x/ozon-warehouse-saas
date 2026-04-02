from ast import literal_eval
from celery import Celery
from uuid import uuid4

from app.services.sync_scheduler import BACKGROUND_SYNC_KINDS, StoreSyncScheduler
from app.config import settings
from app.services.sync_locks import sync_lock_key
from app.services.sync_status import (
    get_store_sync_status,
    mark_store_kind_cancelled,
    mark_store_kind_queued,
    mark_store_sync_cancelled,
    mark_store_sync_failed,
    mark_store_sync_queued,
    redis_client as sync_status_redis_client,
)


celery_app = Celery(
    "backend_sync_dispatcher",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)


TASK_NAME_BY_KIND = {
    "full": "worker.tasks.sync_full_task",
    "closed_months": "worker.tasks.sync_closed_month_history_task",
    "products": "worker.tasks.sync_products_task",
    "stocks": "worker.tasks.sync_stocks_task",
    "supplies": "worker.tasks.sync_supplies_task",
    "reports": "worker.tasks.sync_report_snapshots_task",
    "finance": "worker.tasks.sync_finance_snapshots_task",
}


def _enqueue_kind_task(task_name: str, *, store_id: int, kind: str, message: str, args: list):
    task_id = str(uuid4())
    mark_store_kind_queued(store_id, kind, message)
    return celery_app.send_task(task_name, args=args, task_id=task_id)


def enqueue_full_sync(
    store_id: int,
    months_back: int = 2,
    *,
    skip_products: bool = False,
    bootstrap: bool = False,
    trigger: str = "manual",
):
    task_id = str(uuid4())
    mark_store_sync_queued(store_id, task_id)
    mark_store_kind_queued(
        store_id,
        "full",
        "Первая полная синхронизация в очереди" if bootstrap else "Полная синхронизация в очереди",
    )
    try:
        return celery_app.send_task(
            "worker.tasks.sync_full_task",
            args=[store_id, months_back, skip_products, bootstrap, trigger],
            task_id=task_id,
        )
    except Exception as exc:
        mark_store_sync_failed(store_id, task_id, f"Не удалось поставить задачу в очередь: {exc}")
        raise


def enqueue_report_snapshot_sync(store_id: int, postings_days_back: int = 30):
    return celery_app.send_task(
        "worker.tasks.sync_report_snapshots_task",
        args=[store_id, postings_days_back],
    )


def enqueue_finance_snapshot_sync(store_id: int, days_back: int = 62):
    return celery_app.send_task(
        "worker.tasks.sync_finance_snapshots_task",
        args=[store_id, days_back],
    )


def _find_task_id_for_store_kind(store_id: int, kind: str) -> str | None:
    task_name = TASK_NAME_BY_KIND.get(kind)
    if not task_name:
        return None

    inspect = celery_app.control.inspect()
    for bucket in (inspect.active() or {}, inspect.reserved() or {}, inspect.scheduled() or {}):
        for tasks in bucket.values():
            for task in tasks or []:
                if task.get("name") != task_name:
                    continue
                raw_args = task.get("args")
                raw_request = task.get("request", {})
                parsed_args = None
                if isinstance(raw_args, str):
                    try:
                        parsed_args = literal_eval(raw_args)
                    except Exception:
                        parsed_args = None
                elif isinstance(raw_args, (list, tuple)):
                    parsed_args = raw_args
                if parsed_args and len(parsed_args) >= 1 and int(parsed_args[0]) == int(store_id):
                    return task.get("id")
                request_args = raw_request.get("args")
                if isinstance(request_args, (list, tuple)) and len(request_args) >= 1 and int(request_args[0]) == int(store_id):
                    return task.get("id")
    return None


async def preempt_background_syncs_for_closed_months(store_id: int) -> list[str]:
    scheduler = StoreSyncScheduler()
    status = get_store_sync_status(store_id)
    sync_kinds = status.get("sync_kinds", {}) or {}
    cancelled: list[str] = []

    for kind in BACKGROUND_SYNC_KINDS:
        kind_state = sync_kinds.get(kind) or {}
        kind_status = str(kind_state.get("status") or "")
        if kind_status not in {"queued", "running"}:
            continue

        task_id = kind_state.get("task_id") or _find_task_id_for_store_kind(store_id, kind)
        if task_id:
            celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
        sync_status_redis_client.delete(sync_lock_key(kind, store_id))
        await scheduler.finish(store_id, kind, task_id=task_id, success=False, error="preempted_by_closed_months")
        await scheduler.defer_background_kind(store_id, kind, reason="preempted_by_closed_months")
        mark_store_kind_cancelled(store_id, kind, "Остановлено: приоритет у выгрузки закрытых месяцев")
        cancelled.append(kind)

    return cancelled


async def preempt_background_syncs_for_manual_kind(store_id: int, target_kind: str) -> list[str]:
    scheduler = StoreSyncScheduler()
    status = get_store_sync_status(store_id)
    sync_kinds = status.get("sync_kinds", {}) or {}
    cancelled: list[str] = []

    for kind in BACKGROUND_SYNC_KINDS:
        kind_state = sync_kinds.get(kind) or {}
        kind_status = str(kind_state.get("status") or "")
        if kind_status not in {"queued", "running"} and not sync_status_redis_client.exists(sync_lock_key(kind, store_id)):
            continue

        for task_id in _find_task_ids_for_store_kind(store_id, kind):
            celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
        sync_status_redis_client.delete(sync_lock_key(kind, store_id))
        await scheduler.finish(store_id, kind, task_id=None, success=False, error=f"preempted_by_manual_{target_kind}")
        if kind != target_kind:
            await scheduler.defer_background_kind(store_id, kind, reason=f"preempted_by_manual_{target_kind}")
        mark_store_kind_cancelled(store_id, kind, "Остановлено: приоритет у ручной синхронизации")
        cancelled.append(kind)

    return cancelled


def enqueue_closed_month_history_sync(store_id: int, months_back: int = 3, *, start_month: str | None = None):
    latest_closed_month = None
    if start_month:
        from app.services.closed_month_history_service import ClosedMonthHistoryService

        months_requested = len(ClosedMonthHistoryService._closed_months_from_start(start_month))
        latest_closed_month = ClosedMonthHistoryService._latest_closed_month()
        effective_start_month = ClosedMonthHistoryService._shift_month(
            latest_closed_month,
            -(months_requested - 1),
        ) if months_requested > 0 else start_month
        queue_message = f"История закрытых месяцев в очереди: с {effective_start_month}"
    else:
        from app.services.closed_month_history_service import ClosedMonthHistoryService

        months_requested = min(months_back, ClosedMonthHistoryService.MAX_HISTORY_MONTHS)
        queue_message = f"История закрытых месяцев в очереди: {months_back} мес."
        effective_start_month = None
    task_id = str(uuid4())
    mark_store_kind_queued(
        store_id,
        "closed_months",
        queue_message,
        task_id=task_id,
        months_requested=months_requested,
        months_completed=0,
        start_month=effective_start_month,
        end_month=latest_closed_month,
        current_month=None,
    )
    return celery_app.send_task(
        "worker.tasks.sync_closed_month_history_task",
        args=[store_id, months_requested, effective_start_month],
        task_id=task_id,
    )


async def schedule_closed_month_history_sync(store_id: int, months_back: int = 3, *, start_month: str | None = None) -> dict:
    from app.services.closed_month_history_service import ClosedMonthHistoryService

    latest_closed_month = ClosedMonthHistoryService._latest_closed_month() if start_month else None
    if start_month:
        months_requested = len(ClosedMonthHistoryService._closed_months_from_start(start_month))
        effective_start_month = ClosedMonthHistoryService._shift_month(
            latest_closed_month,
            -(months_requested - 1),
        ) if months_requested > 0 else start_month
    else:
        months_requested = min(months_back, ClosedMonthHistoryService.MAX_HISTORY_MONTHS)
        effective_start_month = None

    sync_kinds = (get_store_sync_status(store_id).get("sync_kinds", {}) or {})
    full_status = str(((sync_kinds.get("full") or {}).get("status")) or "")
    full_is_busy = full_status in {"queued", "running"} or bool(sync_status_redis_client.exists(sync_lock_key("full", store_id)))

    if full_is_busy:
        await StoreSyncScheduler().defer_after_full(
            store_id,
            "closed_months",
            payload={
                "months_requested": months_requested,
                "start_month": effective_start_month,
                "end_month": latest_closed_month,
            },
        )
        mark_store_kind_queued(
            store_id,
            "closed_months",
            "Выгрузка истории закрытых месяцев запустится сразу после полной синхронизации",
            task_id=None,
            months_requested=months_requested,
            months_completed=0,
            start_month=effective_start_month,
            end_month=latest_closed_month,
            current_month=None,
        )
        return {
            "status": "deferred_after_full",
            "store_id": store_id,
            "task_id": None,
            "months_requested": months_requested,
            "start_month": effective_start_month,
            "end_month": latest_closed_month,
        }

    task = enqueue_closed_month_history_sync(store_id, months_back=months_requested, start_month=effective_start_month)
    return {
        "status": "queued",
        "store_id": store_id,
        "task_id": getattr(task, "id", None),
        "months_requested": months_requested,
        "start_month": effective_start_month,
        "end_month": latest_closed_month,
    }


def _resume_deferred_background_syncs(store_id: int, kinds: list[str]) -> None:
    for kind in kinds:
        if kind == "products":
            _enqueue_kind_task(
                "worker.tasks.sync_products_task",
                store_id=store_id,
                kind="products",
                message="Отложенная синхронизация товаров в очереди",
                args=[store_id, "background"],
            )
        elif kind == "supplies":
            _enqueue_kind_task(
                "worker.tasks.sync_supplies_task",
                store_id=store_id,
                kind="supplies",
                message="Отложенная синхронизация поставок в очереди",
                args=[store_id, 1, "background"],
            )
        elif kind == "stocks":
            _enqueue_kind_task(
                "worker.tasks.sync_stocks_task",
                store_id=store_id,
                kind="stocks",
                message="Отложенная синхронизация остатков в очереди",
                args=[store_id, "background"],
            )
        elif kind == "reports":
            _enqueue_kind_task(
                "worker.tasks.sync_report_snapshots_task",
                store_id=store_id,
                kind="reports",
                message="Отложенная синхронизация отчетов в очереди",
                args=[store_id, 30],
            )
        elif kind == "finance":
            _enqueue_kind_task(
                "worker.tasks.sync_finance_snapshots_task",
                store_id=store_id,
                kind="finance",
                message="Отложенная синхронизация финансов в очереди",
                args=[store_id, 62],
            )


async def cancel_closed_month_history_sync(store_id: int) -> dict:
    current_kind = (get_store_sync_status(store_id).get("sync_kinds", {}) or {}).get("closed_months") or {}
    task_id = current_kind.get("task_id")
    current_status = str(current_kind.get("status") or "")
    if current_status not in {"queued", "running"}:
        raise ValueError("Активной выгрузки закрытых месяцев сейчас нет")

    if not task_id:
        inspect = celery_app.control.inspect()
        candidates: list[dict] = []
        for bucket in (inspect.active() or {}, inspect.reserved() or {}, inspect.scheduled() or {}):
            for tasks in bucket.values():
                for task in tasks or []:
                    if task.get("name") != "worker.tasks.sync_closed_month_history_task":
                        continue
                    raw_args = task.get("args")
                    raw_request = task.get("request", {})
                    parsed_args = None
                    if isinstance(raw_args, str):
                        try:
                            parsed_args = literal_eval(raw_args)
                        except Exception:
                            parsed_args = None
                    elif isinstance(raw_args, (list, tuple)):
                        parsed_args = raw_args
                    if parsed_args and len(parsed_args) >= 1 and int(parsed_args[0]) == int(store_id):
                        candidates.append(task)
                        continue
                    request_args = raw_request.get("args")
                    if isinstance(request_args, (list, tuple)) and len(request_args) >= 1 and int(request_args[0]) == int(store_id):
                        candidates.append(task)

        if candidates:
            task_id = candidates[0].get("id")

    if not task_id:
        await StoreSyncScheduler().clear_after_full(store_id, kind="closed_months")
        sync_status_redis_client.delete(sync_lock_key("closed_months", store_id))
        await StoreSyncScheduler().finish(store_id, "closed_months", task_id=None, success=False, error="cancelled")
        deferred_kinds = await StoreSyncScheduler().drain_ready_background_queue(store_id)
        if deferred_kinds:
            _resume_deferred_background_syncs(store_id, deferred_kinds)
        mark_store_kind_cancelled(store_id, "closed_months", "Выгрузка истории закрытых месяцев остановлена")
        return {
            "status": "cancelled",
            "store_id": store_id,
            "task_id": None,
            "message": "Активная задача не найдена, статус выгрузки сброшен",
        }

    celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
    await StoreSyncScheduler().clear_after_full(store_id, kind="closed_months")
    sync_status_redis_client.delete(sync_lock_key("closed_months", store_id))
    await StoreSyncScheduler().finish(store_id, "closed_months", task_id=task_id, success=False, error="cancelled")
    deferred_kinds = await StoreSyncScheduler().drain_ready_background_queue(store_id)
    if deferred_kinds:
        _resume_deferred_background_syncs(store_id, deferred_kinds)
    mark_store_kind_cancelled(store_id, "closed_months", "Выгрузка истории закрытых месяцев остановлена")
    return {
        "status": "cancelled",
        "store_id": store_id,
        "task_id": task_id,
        "message": "Выгрузка истории закрытых месяцев остановлена",
    }


def _find_task_ids_for_store_kind(store_id: int, kind: str) -> list[str]:
    task_name = TASK_NAME_BY_KIND.get(kind)
    if not task_name:
        return []

    inspect = celery_app.control.inspect()
    result: list[str] = []
    for bucket in (inspect.active() or {}, inspect.reserved() or {}, inspect.scheduled() or {}):
        for tasks in bucket.values():
            for task in tasks or []:
                if task.get("name") != task_name:
                    continue
                raw_args = task.get("args")
                raw_request = task.get("request", {})
                parsed_args = None
                if isinstance(raw_args, str):
                    try:
                        parsed_args = literal_eval(raw_args)
                    except Exception:
                        parsed_args = None
                elif isinstance(raw_args, (list, tuple)):
                    parsed_args = raw_args
                if parsed_args and len(parsed_args) >= 1 and int(parsed_args[0]) == int(store_id):
                    task_id = task.get("id")
                    if task_id:
                        result.append(task_id)
                    continue
                request_args = raw_request.get("args")
                if isinstance(request_args, (list, tuple)) and len(request_args) >= 1 and int(request_args[0]) == int(store_id):
                    task_id = task.get("id")
                    if task_id:
                        result.append(task_id)
    return list(dict.fromkeys(result))


async def preempt_syncs_for_manual_full(store_id: int) -> list[str]:
    scheduler = StoreSyncScheduler()
    sync_kinds = (get_store_sync_status(store_id).get("sync_kinds", {}) or {})
    cancelled: list[str] = []

    for kind in ("closed_months",) + BACKGROUND_SYNC_KINDS:
        kind_state = sync_kinds.get(kind) or {}
        kind_status = str(kind_state.get("status") or "")
        if kind_status not in {"queued", "running"} and not sync_status_redis_client.exists(sync_lock_key(kind, store_id)):
            continue

        for task_id in _find_task_ids_for_store_kind(store_id, kind):
            celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")
        sync_status_redis_client.delete(sync_lock_key(kind, store_id))
        await scheduler.finish(store_id, kind, task_id=None, success=False, error="preempted_by_full")
        if kind == "closed_months":
            await scheduler.clear_after_full(store_id, kind="closed_months")
            mark_store_kind_cancelled(store_id, kind, "Остановлено: приоритет у полной синхронизации")
        else:
            mark_store_kind_cancelled(store_id, kind, "Остановлено: приоритет у полной синхронизации")
        cancelled.append(kind)

    return cancelled


async def cancel_sync_kind(store_id: int, kind: str) -> dict:
    current_kind = (get_store_sync_status(store_id).get("sync_kinds", {}) or {}).get(kind) or {}
    current_status = str(current_kind.get("status") or "")
    if current_status not in {"queued", "running"} and not sync_status_redis_client.exists(sync_lock_key(kind, store_id)):
        raise ValueError("Активной синхронизации этого типа сейчас нет")

    task_ids = _find_task_ids_for_store_kind(store_id, kind)
    for task_id in task_ids:
        celery_app.control.revoke(task_id, terminate=True, signal="SIGTERM")

    if kind == "full":
        sync_status_redis_client.delete(sync_lock_key("full", store_id))
        await StoreSyncScheduler().finish(store_id, "full", task_id=None, success=False, error="cancelled")
        mark_store_sync_cancelled(store_id, task_ids[0] if task_ids else None, "Полная синхронизация остановлена")
        mark_store_kind_cancelled(store_id, "full", "Полная синхронизация остановлена")
        follow_up = await enqueue_post_full_follow_up_if_any(store_id)
        if not follow_up:
            deferred_kinds = await StoreSyncScheduler().drain_ready_background_queue(store_id)
            if deferred_kinds:
                _resume_deferred_background_syncs(store_id, deferred_kinds)
        return {
            "status": "cancelled",
            "store_id": store_id,
            "kind": kind,
            "task_id": task_ids[0] if task_ids else None,
            "follow_up_kind": (follow_up or {}).get("kind"),
        }

    sync_status_redis_client.delete(sync_lock_key(kind, store_id))
    await StoreSyncScheduler().finish(store_id, kind, task_id=None, success=False, error="cancelled")
    mark_store_kind_cancelled(store_id, kind, "Синхронизация остановлена")
    deferred_kinds = await StoreSyncScheduler().drain_ready_background_queue(store_id)
    if deferred_kinds:
        _resume_deferred_background_syncs(store_id, deferred_kinds)
    return {
        "status": "cancelled",
        "store_id": store_id,
        "kind": kind,
        "task_id": task_ids[0] if task_ids else None,
    }


async def enqueue_post_full_follow_up_if_any(store_id: int) -> dict | None:
    queued = await StoreSyncScheduler().pop_ready_after_full(store_id)
    if not queued:
        return None

    kind = str(queued.get("kind") or "")
    payload = dict(queued.get("payload") or {})
    if kind != "closed_months":
        return None

    months_requested = int(payload.get("months_requested") or 3)
    start_month = payload.get("start_month")
    task = enqueue_closed_month_history_sync(store_id, months_back=months_requested, start_month=start_month)
    return {
        "kind": kind,
        "task_id": getattr(task, "id", None),
        "months_requested": months_requested,
        "start_month": start_month,
        "end_month": payload.get("end_month"),
    }


def enqueue_products_sync(store_id: int, *, source: str = "manual"):
    return _enqueue_kind_task(
        "worker.tasks.sync_products_task",
        store_id=store_id,
        kind="products",
        message="Ручная синхронизация товаров в очереди" if source == "manual" else "Синхронизация товаров в очереди",
        args=[store_id, source],
    )


def enqueue_supplies_sync(store_id: int, months_back: int = 1, *, source: str = "manual"):
    return _enqueue_kind_task(
        "worker.tasks.sync_supplies_task",
        store_id=store_id,
        kind="supplies",
        message="Ручная синхронизация поставок в очереди" if source == "manual" else "Синхронизация поставок в очереди",
        args=[store_id, months_back, source],
    )


def enqueue_stocks_sync(store_id: int, *, source: str = "manual"):
    return _enqueue_kind_task(
        "worker.tasks.sync_stocks_task",
        store_id=store_id,
        kind="stocks",
        message="Ручная синхронизация остатков в очереди" if source == "manual" else "Синхронизация остатков в очереди",
        args=[store_id, source],
    )


def enqueue_reports_sync(store_id: int, postings_days_back: int = 30, *, source: str = "manual"):
    return _enqueue_kind_task(
        "worker.tasks.sync_report_snapshots_task",
        store_id=store_id,
        kind="reports",
        message="Ручная синхронизация отчётов в очереди" if source == "manual" else "Синхронизация отчётов в очереди",
        args=[store_id, postings_days_back, source],
    )


def enqueue_finance_sync(store_id: int, days_back: int = 62, *, source: str = "manual"):
    return _enqueue_kind_task(
        "worker.tasks.sync_finance_snapshots_task",
        store_id=store_id,
        kind="finance",
        message="Ручная синхронизация финансов в очереди" if source == "manual" else "Синхронизация финансов в очереди",
        args=[store_id, days_back, source],
    )
