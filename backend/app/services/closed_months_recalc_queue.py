from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.closed_month_history_service import ClosedMonthHistoryService
from app.services.sync_locks import has_any_sync_lock
from app.services.sync_scheduler import StoreSyncScheduler
from app.services.sync_status import mark_store_kind_queued
from app.utils.redis_cache import get_redis


PENDING_RECALC_TTL_SECONDS = 7 * 24 * 60 * 60
PENDING_RECALC_DEBOUNCE_SECONDS = 45
PENDING_RECALC_RETRY_SECONDS = 60
PENDING_RECALC_STALE_INFLIGHT_SECONDS = 6 * 60 * 60
PENDING_RECALC_SCAN_BATCH_SIZE = 200
PENDING_RECALC_KIND = "closed_months"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _month_sort_key(month: str | None) -> tuple[int, int]:
    if not month:
        return (9999, 12)
    year_str, month_str = str(month).split("-", 1)
    return int(year_str), int(month_str)


def _earlier_month(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return left if _month_sort_key(left) <= _month_sort_key(right) else right


def _default_state(store_id: int) -> dict[str, Any]:
    return {
        "store_id": int(store_id),
        "start_month": None,
        "revision": 0,
        "updated_at": None,
        "not_before": None,
        "inflight_task_id": None,
        "inflight_revision": None,
        "inflight_start_month": None,
        "inflight_dispatched_at": None,
    }


class ClosedMonthsRecalcQueue:
    @staticmethod
    def state_key(store_id: int) -> str:
        return f"closed_months:recalc:store:{store_id}"

    @staticmethod
    def state_pattern() -> str:
        return "closed_months:recalc:store:*"

    @staticmethod
    def guard_key(store_id: int) -> str:
        return f"closed_months:recalc:store:{store_id}:guard"

    async def _acquire_guard(self, store_id: int) -> bool:
        redis = await get_redis()
        if not redis:
            return True
        try:
            acquired = await redis.set(self.guard_key(store_id), "1", ex=15, nx=True)
            return bool(acquired)
        except Exception:
            return True

    async def _release_guard(self, store_id: int) -> None:
        redis = await get_redis()
        if not redis:
            return
        try:
            await redis.delete(self.guard_key(store_id))
        except Exception:
            return

    def _normalize_start_month(self, start_month: str | None) -> str:
        months = ClosedMonthHistoryService._closed_months_from_start(start_month)
        if not months:
            raise ValueError("Выбранный месяц позже последнего закрытого месяца Ozon")
        return months[0]

    def _normalize_state(self, store_id: int, payload: dict[str, Any] | None) -> dict[str, Any]:
        state = {**_default_state(store_id), **dict(payload or {})}
        inflight_dispatched_at = _parse_iso(state.get("inflight_dispatched_at"))
        if inflight_dispatched_at and (_now() - inflight_dispatched_at).total_seconds() > PENDING_RECALC_STALE_INFLIGHT_SECONDS:
            state["inflight_task_id"] = None
            state["inflight_revision"] = None
            state["inflight_start_month"] = None
            state["inflight_dispatched_at"] = None
            state["not_before"] = _iso(_now() + timedelta(seconds=PENDING_RECALC_RETRY_SECONDS))
        return state

    async def _load_state(self, store_id: int) -> dict[str, Any]:
        redis = await get_redis()
        if not redis:
            return _default_state(store_id)
        try:
            raw = await redis.get(self.state_key(store_id))
            if not raw:
                return _default_state(store_id)
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                return _default_state(store_id)
            return self._normalize_state(store_id, parsed)
        except Exception:
            return _default_state(store_id)

    async def _save_state(self, store_id: int, state: dict[str, Any]) -> None:
        redis = await get_redis()
        if not redis:
            return
        payload = {**_default_state(store_id), **state, "store_id": int(store_id), "updated_at": _iso(_now())}
        try:
            await redis.set(
                self.state_key(store_id),
                json.dumps(payload, ensure_ascii=False),
                ex=PENDING_RECALC_TTL_SECONDS,
            )
        except Exception:
            return

    async def clear_state(self, store_id: int) -> None:
        redis = await get_redis()
        if not redis:
            return
        try:
            await redis.delete(self.state_key(store_id), self.guard_key(store_id))
        except Exception:
            return

    async def get_state(self, store_id: int) -> dict[str, Any]:
        return await self._load_state(store_id)

    async def get_revision(self, store_id: int) -> int:
        state = await self._load_state(store_id)
        return int(state.get("revision") or 0)

    async def has_pending(self, store_id: int) -> bool:
        state = await self._load_state(store_id)
        return bool(state.get("start_month") or state.get("inflight_task_id"))

    async def queue(self, store_id: int, *, start_month: str) -> dict[str, Any]:
        normalized_start_month = self._normalize_start_month(start_month)
        if not await self._acquire_guard(store_id):
            return {"status": "busy", "store_id": store_id}
        try:
            state = await self._load_state(store_id)
            state["start_month"] = _earlier_month(state.get("start_month"), normalized_start_month)
            state["revision"] = int(state.get("revision") or 0) + 1
            if not state.get("inflight_task_id"):
                state["not_before"] = _iso(_now() + timedelta(seconds=PENDING_RECALC_DEBOUNCE_SECONDS))
            await self._save_state(store_id, state)
        finally:
            await self._release_guard(store_id)

        mark_store_kind_queued(
            store_id,
            PENDING_RECALC_KIND,
            "Пересчет закрытых месяцев запланирован после изменений себестоимости",
            task_id=state.get("inflight_task_id"),
            start_month=state.get("start_month"),
        )
        return {
            "status": "queued",
            "store_id": store_id,
            "start_month": state.get("start_month"),
            "revision": int(state.get("revision") or 0),
            "debounce_seconds": PENDING_RECALC_DEBOUNCE_SECONDS,
        }

    async def claim_ready(self, store_id: int, *, task_id: str) -> dict[str, Any] | None:
        if not await self._acquire_guard(store_id):
            return None
        try:
            state = await self._load_state(store_id)
            start_month = str(state.get("start_month") or "").strip() or None
            if not start_month or state.get("inflight_task_id"):
                return None
            not_before = _parse_iso(state.get("not_before"))
            if not_before and not_before > _now():
                return None

            scheduler_state = await StoreSyncScheduler().get_state(store_id)
            active = dict(scheduler_state.get("active") or {})
            active_kind = str(active.get("kind") or "")
            if active_kind in {"full", "closed_months"}:
                return None
            if await has_any_sync_lock(store_id, ("full", "closed_months")):
                return None

            inflight_revision = int(state.get("revision") or 0)
            dispatched_at = _now()
            state["inflight_task_id"] = task_id
            state["inflight_revision"] = inflight_revision
            state["inflight_start_month"] = start_month
            state["inflight_dispatched_at"] = _iso(dispatched_at)
            state["not_before"] = None
            await self._save_state(store_id, state)
            return {
                "store_id": store_id,
                "task_id": task_id,
                "start_month": start_month,
                "months_requested": len(ClosedMonthHistoryService._closed_months_from_start(start_month)),
                "revision": inflight_revision,
            }
        finally:
            await self._release_guard(store_id)

    async def release_claim(self, store_id: int, *, task_id: str) -> None:
        if not await self._acquire_guard(store_id):
            return
        try:
            state = await self._load_state(store_id)
            if state.get("inflight_task_id") != task_id:
                return
            state["inflight_task_id"] = None
            state["inflight_revision"] = None
            state["inflight_start_month"] = None
            state["inflight_dispatched_at"] = None
            state["not_before"] = _iso(_now() + timedelta(seconds=PENDING_RECALC_RETRY_SECONDS))
            await self._save_state(store_id, state)
        finally:
            await self._release_guard(store_id)

    async def finish_task(self, store_id: int, *, task_id: str, success: bool) -> dict[str, Any]:
        if not await self._acquire_guard(store_id):
            return {"status": "busy", "store_id": store_id}
        try:
            state = await self._load_state(store_id)
            if state.get("inflight_task_id") != task_id:
                return {"status": "ignored", "store_id": store_id}

            inflight_revision = int(state.get("inflight_revision") or 0)
            inflight_start_month = str(state.get("inflight_start_month") or "").strip() or None
            current_revision = int(state.get("revision") or 0)
            current_start_month = str(state.get("start_month") or "").strip() or None

            state["inflight_task_id"] = None
            state["inflight_revision"] = None
            state["inflight_start_month"] = None
            state["inflight_dispatched_at"] = None

            if not success:
                state["not_before"] = _iso(_now() + timedelta(seconds=PENDING_RECALC_RETRY_SECONDS))
                await self._save_state(store_id, state)
                return {"status": "retry_scheduled", "store_id": store_id, "start_month": current_start_month}

            if current_start_month == inflight_start_month and current_revision == inflight_revision:
                await self.clear_state(store_id)
                return {"status": "cleared", "store_id": store_id}

            state["not_before"] = _iso(_now() + timedelta(seconds=PENDING_RECALC_DEBOUNCE_SECONDS))
            await self._save_state(store_id, state)
            return {
                "status": "follow_up_scheduled",
                "store_id": store_id,
                "start_month": current_start_month,
                "revision": current_revision,
            }
        finally:
            await self._release_guard(store_id)

    async def list_store_ids(self) -> list[int]:
        redis = await get_redis()
        if not redis:
            return []
        result: list[int] = []
        try:
            async for key in redis.scan_iter(match=self.state_pattern(), count=PENDING_RECALC_SCAN_BATCH_SIZE):
                store_id = str(key).rsplit(":", 1)[-1]
                if store_id.isdigit():
                    result.append(int(store_id))
        except Exception:
            return []
        return sorted(set(result))
