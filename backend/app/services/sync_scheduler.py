from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from loguru import logger

from app.utils.redis_cache import get_redis

SyncKind = Literal["full", "closed_months", "products", "stocks", "supplies", "reports", "finance"]
SyncSource = Literal["manual", "background", "store_created", "startup", "system"]

BACKGROUND_SYNC_KINDS: tuple[SyncKind, ...] = ("products", "stocks", "supplies", "reports", "finance")
FULL_SYNC_KIND: SyncKind = "full"
CLOSED_MONTHS_KIND: SyncKind = "closed_months"
DEFAULT_STATE_TTL_SECONDS = 7 * 24 * 60 * 60
DEFAULT_LOCK_TTL_SECONDS = 15
DEFAULT_FULL_COOLDOWN_SECONDS = 15 * 60


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


@dataclass
class SchedulerDecision:
    allowed: bool
    action: str
    message: str
    remaining_seconds: int = 0


class StoreSyncScheduler:
    def __init__(self, cooldown_seconds: int = DEFAULT_FULL_COOLDOWN_SECONDS):
        self.cooldown_seconds = max(int(cooldown_seconds), 0)

    @staticmethod
    def state_key(store_id: int) -> str:
        return f"sync:scheduler:store:{store_id}:state"

    @staticmethod
    def guard_key(store_id: int) -> str:
        return f"sync:scheduler:store:{store_id}:guard"

    async def _acquire_guard(self, store_id: int) -> bool:
        redis = await get_redis()
        if not redis:
            return True
        try:
            acquired = await redis.set(self.guard_key(store_id), "1", ex=DEFAULT_LOCK_TTL_SECONDS, nx=True)
            return bool(acquired)
        except Exception as e:
            logger.warning("Failed to acquire scheduler guard for store {}: {}", store_id, e)
            return True

    async def _release_guard(self, store_id: int) -> None:
        redis = await get_redis()
        if not redis:
            return
        try:
            await redis.delete(self.guard_key(store_id))
        except Exception as e:
            logger.warning("Failed to release scheduler guard for store {}: {}", store_id, e)

    async def _load_state(self, store_id: int) -> dict[str, Any]:
        redis = await get_redis()
        default = {
            "active": None,
            "queued_background": {},
            "queued_after_full": None,
            "cooldown_until": None,
            "updated_at": None,
        }
        if not redis:
            return default
        try:
            raw = await redis.get(self.state_key(store_id))
            if not raw:
                return default
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                return default
            parsed.setdefault("active", None)
            parsed.setdefault("queued_background", {})
            parsed.setdefault("queued_after_full", None)
            parsed.setdefault("cooldown_until", None)
            parsed.setdefault("updated_at", None)
            return parsed
        except Exception as e:
            logger.warning("Failed to load scheduler state for store {}: {}", store_id, e)
            return default

    async def _save_state(self, store_id: int, state: dict[str, Any]) -> None:
        redis = await get_redis()
        if not redis:
            return
        state["updated_at"] = _iso(_utcnow())
        try:
            await redis.set(self.state_key(store_id), json.dumps(state, ensure_ascii=False), ex=DEFAULT_STATE_TTL_SECONDS)
        except Exception as e:
            logger.warning("Failed to save scheduler state for store {}: {}", store_id, e)

    async def clear_store_state(self, store_id: int) -> None:
        redis = await get_redis()
        if not redis:
            return
        try:
            await redis.delete(self.state_key(store_id), self.guard_key(store_id))
        except Exception as e:
            logger.warning("Failed to clear scheduler state for store {}: {}", store_id, e)

    async def get_active_entry(self, store_id: int) -> dict[str, Any] | None:
        state = await self._load_state(store_id)
        active = dict(state.get("active") or {})
        if not active.get("kind"):
            return None
        return active

    async def get_state(self, store_id: int) -> dict[str, Any]:
        return await self._load_state(store_id)

    async def get_cooldown_remaining_seconds(self, store_id: int) -> int:
        state = await self._load_state(store_id)
        cooldown_until = _parse_iso(state.get("cooldown_until"))
        if not cooldown_until:
            return 0
        remaining = int((cooldown_until - _utcnow()).total_seconds())
        return max(remaining, 0)

    async def mark_background_requested(self, store_id: int, kind: SyncKind, *, reason: str | None = None) -> SchedulerDecision:
        if kind not in BACKGROUND_SYNC_KINDS:
            return SchedulerDecision(False, "invalid", "Некорректный тип фоновой синхронизации")

        if not await self._acquire_guard(store_id):
            return SchedulerDecision(False, "busy", "Планировщик занят, повторите на следующем цикле")

        try:
            state = await self._load_state(store_id)
            active = state.get("active") or {}
            active_kind = active.get("kind")
            cooldown_remaining = await self.get_cooldown_remaining_seconds(store_id)

            if active_kind == FULL_SYNC_KIND:
                state.setdefault("queued_background", {})[kind] = {
                    "requested_at": _iso(_utcnow()),
                    "reason": reason or "full_sync_running",
                }
                await self._save_state(store_id, state)
                return SchedulerDecision(False, "deferred_full", "Ожидает завершения полной синхронизации")

            if active_kind:
                state.setdefault("queued_background", {})[kind] = {
                    "requested_at": _iso(_utcnow()),
                    "reason": reason or f"active_{active_kind}",
                }
                await self._save_state(store_id, state)
                return SchedulerDecision(False, "deferred_active", "Ожидает завершения другой синхронизации")

            if cooldown_remaining > 0:
                state.setdefault("queued_background", {})[kind] = {
                    "requested_at": _iso(_utcnow()),
                    "reason": reason or "cooldown_after_full",
                }
                await self._save_state(store_id, state)
                return SchedulerDecision(False, "deferred_cooldown", "Ожидает окно после полной синхронизации", cooldown_remaining)

            return SchedulerDecision(True, "schedule", "Можно ставить фоновую синхронизацию в очередь")
        finally:
            await self._release_guard(store_id)

    async def try_start(self, store_id: int, kind: SyncKind, *, task_id: str | None, source: SyncSource) -> SchedulerDecision:
        if not await self._acquire_guard(store_id):
            return SchedulerDecision(False, "busy", "Планировщик занят, повторите позже")

        try:
            state = await self._load_state(store_id)
            active = state.get("active") or {}
            active_kind = active.get("kind")
            cooldown_remaining = await self.get_cooldown_remaining_seconds(store_id)

            if active_kind:
                if active_kind == kind and active.get("task_id") == task_id:
                    return SchedulerDecision(True, "resume", "Эта же синхронизация уже отмечена как активная")
                if kind in BACKGROUND_SYNC_KINDS:
                    state.setdefault("queued_background", {})[kind] = {
                        "requested_at": _iso(_utcnow()),
                        "reason": f"active_{active_kind}",
                    }
                    await self._save_state(store_id, state)
                return SchedulerDecision(False, "blocked_active", f"Уже выполняется синхронизация {active_kind}")

            if kind in BACKGROUND_SYNC_KINDS and source == "background" and cooldown_remaining > 0:
                state.setdefault("queued_background", {})[kind] = {
                    "requested_at": _iso(_utcnow()),
                    "reason": "cooldown_after_full",
                }
                await self._save_state(store_id, state)
                return SchedulerDecision(False, "blocked_cooldown", "Фоновая синхронизация ждёт окно после полной", cooldown_remaining)

            state["active"] = {
                "kind": kind,
                "task_id": task_id,
                "source": source,
                "started_at": _iso(_utcnow()),
            }
            if kind == FULL_SYNC_KIND:
                state["queued_background"] = {}
                state["cooldown_until"] = None
            else:
                state.setdefault("queued_background", {}).pop(kind, None)
            await self._save_state(store_id, state)
            return SchedulerDecision(True, "started", "Синхронизация разрешена")
        finally:
            await self._release_guard(store_id)

    async def finish(self, store_id: int, kind: SyncKind, *, task_id: str | None, success: bool, error: str | None = None) -> None:
        if not await self._acquire_guard(store_id):
            return
        try:
            state = await self._load_state(store_id)
            active = state.get("active") or {}
            if active.get("kind") == kind and (task_id is None or active.get("task_id") == task_id):
                state["active"] = None
            if kind == FULL_SYNC_KIND and success and self.cooldown_seconds > 0:
                state["cooldown_until"] = _iso(_utcnow() + timedelta(seconds=self.cooldown_seconds))
            elif kind == FULL_SYNC_KIND and not success:
                state["cooldown_until"] = None
            state["last_finished"] = {
                "kind": kind,
                "task_id": task_id,
                "success": success,
                "error": error,
                "finished_at": _iso(_utcnow()),
            }
            await self._save_state(store_id, state)
        finally:
            await self._release_guard(store_id)

    async def drain_ready_background_queue(self, store_id: int) -> list[SyncKind]:
        if not await self._acquire_guard(store_id):
            return []
        try:
            state = await self._load_state(store_id)
            active = state.get("active") or {}
            if active.get("kind"):
                return []

            cooldown_until = _parse_iso(state.get("cooldown_until"))
            if cooldown_until and cooldown_until > _utcnow():
                return []

            queued_background = dict(state.get("queued_background") or {})
            if not queued_background:
                return []

            drained = [
                kind
                for kind in BACKGROUND_SYNC_KINDS
                if kind in queued_background
            ]
            state["queued_background"] = {}
            await self._save_state(store_id, state)
            return drained
        finally:
            await self._release_guard(store_id)

    async def defer_background_kind(self, store_id: int, kind: SyncKind, *, reason: str | None = None) -> None:
        if kind not in BACKGROUND_SYNC_KINDS:
            return
        if not await self._acquire_guard(store_id):
            return
        try:
            state = await self._load_state(store_id)
            state.setdefault("queued_background", {})[kind] = {
                "requested_at": _iso(_utcnow()),
                "reason": reason or "deferred",
            }
            await self._save_state(store_id, state)
        finally:
            await self._release_guard(store_id)

    async def defer_after_full(self, store_id: int, kind: SyncKind, *, payload: dict[str, Any] | None = None) -> None:
        if not await self._acquire_guard(store_id):
            return
        try:
            state = await self._load_state(store_id)
            state["queued_after_full"] = {
                "kind": kind,
                "payload": dict(payload or {}),
                "requested_at": _iso(_utcnow()),
            }
            await self._save_state(store_id, state)
        finally:
            await self._release_guard(store_id)

    async def pop_ready_after_full(self, store_id: int) -> dict[str, Any] | None:
        if not await self._acquire_guard(store_id):
            return None
        try:
            state = await self._load_state(store_id)
            active = state.get("active") or {}
            if active.get("kind"):
                return None
            queued = state.get("queued_after_full")
            if not queued:
                return None
            state["queued_after_full"] = None
            await self._save_state(store_id, state)
            return dict(queued)
        finally:
            await self._release_guard(store_id)

    async def clear_after_full(self, store_id: int, *, kind: SyncKind | None = None) -> None:
        if not await self._acquire_guard(store_id):
            return
        try:
            state = await self._load_state(store_id)
            queued = state.get("queued_after_full")
            if not queued:
                return
            if kind and queued.get("kind") != kind:
                return
            state["queued_after_full"] = None
            await self._save_state(store_id, state)
        finally:
            await self._release_guard(store_id)
