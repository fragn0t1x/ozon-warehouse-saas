from celery import Celery
import os
import asyncio
import json
from datetime import datetime, timezone
from loguru import logger
from celery.signals import worker_ready, worker_shutdown
from redis import Redis

redis_url = os.getenv("REDIS_URL", "redis://redis:6379")

celery = Celery("worker", broker=redis_url, backend=redis_url)

celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,
    task_soft_time_limit=25 * 60,
    broker_connection_retry_on_startup=True,
)

# Явно указываем, где искать задачи
celery.autodiscover_tasks(['worker.tasks'])

# Регистрируем Prometheus-метрики worker через celery signals.
import worker.metrics  # noqa: E402,F401

# Создаем event loop для каждой задачи
def get_event_loop():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


def _clear_stale_sync_locks() -> None:
    redis = Redis.from_url(redis_url, decode_responses=True)
    patterns = [
        "sync:full:*",
        "sync:products:*",
        "sync:stocks:*",
        "sync:supplies:*",
    ]
    removed = 0
    try:
        for pattern in patterns:
            keys = redis.keys(pattern)
            if keys:
                removed += redis.delete(*keys)
        if removed:
            logger.warning(f"🧹 Cleared {removed} stale sync locks on worker startup")
    except Exception as e:
        logger.error(f"Failed to clear stale sync locks: {e}")
    finally:
        try:
            redis.close()
        except Exception:
            pass


def _clear_stale_scheduler_states() -> None:
    redis = Redis.from_url(redis_url, decode_responses=True)
    pattern = "sync:scheduler:store:*:state"
    fixed = 0
    try:
        for key in redis.scan_iter(pattern):
            raw = redis.get(key)
            if not raw:
                continue
            try:
                state = json.loads(raw)
            except Exception:
                continue
            if not isinstance(state, dict):
                continue

            changed = False
            if state.get("active") is not None:
                state["active"] = None
                changed = True

            cooldown_until = state.get("cooldown_until")
            if cooldown_until:
                try:
                    parsed = datetime.fromisoformat(cooldown_until)
                    if not parsed.tzinfo:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    if parsed <= datetime.now(timezone.utc):
                        state["cooldown_until"] = None
                        changed = True
                except Exception:
                    state["cooldown_until"] = None
                    changed = True

            if changed:
                state["updated_at"] = datetime.now(timezone.utc).isoformat()
                redis.set(key, json.dumps(state, ensure_ascii=False))
                fixed += 1
        if fixed:
            logger.warning(f"🧹 Cleared stale scheduler state for {fixed} stores on worker startup")
    except Exception as e:
        logger.error(f"Failed to clear stale scheduler states: {e}")
    finally:
        try:
            redis.close()
        except Exception:
            pass


def _normalize_sync_status_payloads() -> None:
    redis = Redis.from_url(redis_url, decode_responses=True)
    pattern = "sync:status:*"
    normalized = 0
    try:
        from app.services.sync_status import get_store_sync_status, set_store_sync_status

        for key in redis.scan_iter(pattern):
            try:
                store_id = int(str(key).rsplit(":", 1)[-1])
            except Exception:
                continue

            current = get_store_sync_status(store_id)
            if not current:
                continue

            set_store_sync_status(
                store_id,
                status=current.get("status"),
                message=current.get("message"),
                task_id=current.get("task_id"),
                queued_at=current.get("queued_at"),
                started_at=current.get("started_at"),
                finished_at=current.get("finished_at"),
                sync_kinds=current.get("sync_kinds") or {},
            )
            normalized += 1

        if normalized:
            logger.warning(f"🧹 Normalized sync status payloads for {normalized} stores on worker startup")
    except Exception as e:
        logger.error(f"Failed to normalize sync status payloads: {e}")
    finally:
        try:
            redis.close()
        except Exception:
            pass

@worker_ready.connect
def at_start(sender, **kwargs):
    _clear_stale_sync_locks()
    _clear_stale_scheduler_states()
    _normalize_sync_status_payloads()
    logger.info("🚀 Worker is ready, starting up...")

@worker_shutdown.connect
def at_shutdown(sender, **kwargs):
    logger.info("👋 Worker shutting down, cleaning up...")
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.close()
    except:
        pass
