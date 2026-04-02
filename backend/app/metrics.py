from __future__ import annotations

from datetime import datetime, timedelta
from time import perf_counter

from sqlalchemy import func, select
from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.middleware.base import BaseHTTPMiddleware

from app.database import SessionLocal
from app.models.ozon_warehouse import OzonStock
from app.models.store import Store
from app.models.supply import Supply
from app.models.supply_notification_event import SupplyNotificationEvent
from app.models.user_settings import UserSettings
from app.models.variant import Variant
from app.models.warehouse import Warehouse, WarehouseStock
from app.services.admin_notifications import _supply_status_label, get_recent_admin_events


HTTP_REQUESTS_TOTAL = Counter(
    "ozon_http_requests_total",
    "Total number of HTTP requests handled by the backend",
    ["method", "path", "status"],
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "ozon_http_request_duration_seconds",
    "HTTP request latency for the backend",
    ["method", "path"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

BUSINESS_STORE_COUNT = Gauge(
    "ozon_business_store_count",
    "Number of stores by state",
    ["state"],
)

BUSINESS_SUPPLY_STATUS_COUNT = Gauge(
    "ozon_business_supply_status_count",
    "Number of supplies grouped by status",
    ["status", "status_label"],
)

BUSINESS_STORE_SUPPLIES_COUNT = Gauge(
    "ozon_business_store_supplies_count",
    "Number of supplies grouped by store and kind",
    ["store_id", "store_name", "kind"],
)

BUSINESS_NEXT_SUPPLY_TIMESTAMP = Gauge(
    "ozon_business_next_supply_timestamp_seconds",
    "Unix timestamp of the nearest upcoming supply per store",
    ["store_id", "store_name"],
)

BUSINESS_NOTIFICATION_ENABLED_USERS = Gauge(
    "ozon_business_notification_enabled_users",
    "Number of users with notification type enabled",
    ["notification_type"],
)

BUSINESS_SUPPLY_SCHEDULE_COUNT = Gauge(
    "ozon_business_supply_schedule_count",
    "Number of supplies grouped by schedule bucket",
    ["kind"],
)

BUSINESS_ADMIN_EVENTS_RECENT = Gauge(
    "ozon_business_admin_events_recent",
    "Recent admin events grouped by type and severity",
    ["event_type", "severity"],
)

BUSINESS_RECENT_SUPPLY_EVENT = Gauge(
    "ozon_business_recent_supply_event_timestamp_seconds",
    "Recent supply creation/status events with timestamp as metric value",
    ["event_type", "store_name", "order_number", "status_before", "status_after", "delivery_state"],
)

BUSINESS_PENDING_SUPPLY_EVENTS = Gauge(
    "ozon_business_pending_supply_event_count",
    "Number of pending supply notification events by type",
    ["event_type"],
)

BUSINESS_STOCK_UNITS = Gauge(
    "ozon_business_stock_units",
    "Business stock units grouped by source and kind",
    ["source", "kind"],
)


ACTIVE_SUPPLY_STATUSES = {
    "DATA_FILLING",
    "READY_TO_SUPPLY",
    "ACCEPTED_AT_SUPPLY_WAREHOUSE",
    "IN_TRANSIT",
    "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
    "REPORTS_CONFIRMATION_AWAITING",
}


async def _refresh_business_metrics() -> None:
    now = datetime.now()
    tomorrow = now + timedelta(days=1)
    next_week = now + timedelta(days=7)

    BUSINESS_STORE_COUNT.clear()
    BUSINESS_SUPPLY_STATUS_COUNT.clear()
    BUSINESS_STORE_SUPPLIES_COUNT.clear()
    BUSINESS_NEXT_SUPPLY_TIMESTAMP.clear()
    BUSINESS_NOTIFICATION_ENABLED_USERS.clear()
    BUSINESS_SUPPLY_SCHEDULE_COUNT.clear()
    BUSINESS_ADMIN_EVENTS_RECENT.clear()
    BUSINESS_RECENT_SUPPLY_EVENT.clear()
    BUSINESS_PENDING_SUPPLY_EVENTS.clear()
    BUSINESS_STOCK_UNITS.clear()

    async with SessionLocal() as db:
        store_rows = (
            await db.execute(
                select(Store.is_active, func.count(Store.id)).group_by(Store.is_active)
            )
        ).all()
        store_counts = {bool(is_active): count for is_active, count in store_rows}
        BUSINESS_STORE_COUNT.labels(state="active").set(store_counts.get(True, 0))
        BUSINESS_STORE_COUNT.labels(state="inactive").set(store_counts.get(False, 0))

        supply_status_rows = (
            await db.execute(
                select(Supply.status, func.count(Supply.id)).group_by(Supply.status)
            )
        ).all()
        for status, count in supply_status_rows:
            safe_status = status or "-"
            BUSINESS_SUPPLY_STATUS_COUNT.labels(
                status=safe_status,
                status_label=_supply_status_label(safe_status),
            ).set(count)

        overdue_total = await db.scalar(
            select(func.count(Supply.id)).where(
                Supply.status == "READY_TO_SUPPLY",
                Supply.timeslot_from.is_not(None),
                Supply.timeslot_from < now,
            )
        ) or 0
        today_total = await db.scalar(
            select(func.count(Supply.id)).where(
                Supply.timeslot_from >= now,
                Supply.timeslot_from < tomorrow,
            )
        ) or 0
        next_7_days_total = await db.scalar(
            select(func.count(Supply.id)).where(
                Supply.timeslot_from >= now,
                Supply.timeslot_from < next_week,
            )
        ) or 0
        future_total = await db.scalar(
            select(func.count(Supply.id)).where(Supply.timeslot_from >= now)
        ) or 0
        unscheduled_active_total = await db.scalar(
            select(func.count(Supply.id)).where(
                Supply.status.in_(ACTIVE_SUPPLY_STATUSES),
                Supply.timeslot_from.is_(None),
            )
        ) or 0

        BUSINESS_SUPPLY_SCHEDULE_COUNT.labels(kind="overdue").set(overdue_total)
        BUSINESS_SUPPLY_SCHEDULE_COUNT.labels(kind="today").set(today_total)
        BUSINESS_SUPPLY_SCHEDULE_COUNT.labels(kind="next_7_days").set(next_7_days_total)
        BUSINESS_SUPPLY_SCHEDULE_COUNT.labels(kind="future").set(future_total)
        BUSINESS_SUPPLY_SCHEDULE_COUNT.labels(kind="unscheduled_active").set(unscheduled_active_total)

        store_supply_rows = (
            await db.execute(
                select(
                    Store.id,
                    Store.name,
                    func.count(Supply.id).label("all_count"),
                    func.count(Supply.id)
                    .filter(Supply.timeslot_from >= now, Supply.timeslot_from < tomorrow)
                    .label("today_count"),
                    func.count(Supply.id)
                    .filter(Supply.timeslot_from >= now, Supply.timeslot_from < next_week)
                    .label("next_7_days_count"),
                    func.count(Supply.id)
                    .filter(Supply.status.in_(ACTIVE_SUPPLY_STATUSES))
                    .label("active_count"),
                    func.count(Supply.id)
                    .filter(
                        Supply.status == "READY_TO_SUPPLY",
                        Supply.timeslot_from.is_not(None),
                        Supply.timeslot_from < now,
                    )
                    .label("overdue_count"),
                )
                .select_from(Store)
                .outerjoin(Supply, Supply.store_id == Store.id)
                .group_by(Store.id, Store.name)
            )
        ).all()

        for row in store_supply_rows:
            store_id = str(row.id)
            store_name = row.name
            BUSINESS_STORE_SUPPLIES_COUNT.labels(store_id=store_id, store_name=store_name, kind="all").set(
                row.all_count or 0
            )
            BUSINESS_STORE_SUPPLIES_COUNT.labels(store_id=store_id, store_name=store_name, kind="active").set(
                row.active_count or 0
            )
            BUSINESS_STORE_SUPPLIES_COUNT.labels(store_id=store_id, store_name=store_name, kind="today").set(
                row.today_count or 0
            )
            BUSINESS_STORE_SUPPLIES_COUNT.labels(
                store_id=store_id,
                store_name=store_name,
                kind="next_7_days",
            ).set(row.next_7_days_count or 0)
            BUSINESS_STORE_SUPPLIES_COUNT.labels(
                store_id=store_id,
                store_name=store_name,
                kind="overdue",
            ).set(row.overdue_count or 0)

        next_supply_rows = (
            await db.execute(
                select(Store.id, Store.name, func.min(Supply.timeslot_from))
                .select_from(Store)
                .outerjoin(
                    Supply,
                    (Supply.store_id == Store.id) & (Supply.timeslot_from >= now),
                )
                .group_by(Store.id, Store.name)
            )
        ).all()
        for store_id, store_name, next_supply_at in next_supply_rows:
            timestamp = next_supply_at.timestamp() if next_supply_at else 0
            BUSINESS_NEXT_SUPPLY_TIMESTAMP.labels(
                store_id=str(store_id),
                store_name=store_name,
            ).set(timestamp)

        notification_queries = {
            "today_supplies": UserSettings.notify_today_supplies,
            "daily_report": UserSettings.notify_daily_report,
            "losses": UserSettings.notify_losses,
            "rejection": UserSettings.notify_rejection,
            "acceptance_status": UserSettings.notify_acceptance_status,
        }
        for notification_type, column in notification_queries.items():
            enabled_count = await db.scalar(
                select(func.count(UserSettings.id)).where(column.is_(True))
            ) or 0
            BUSINESS_NOTIFICATION_ENABLED_USERS.labels(notification_type=notification_type).set(enabled_count)

        warehouse_stock_row = (
            await db.execute(
                select(
                    func.coalesce(func.sum(WarehouseStock.unpacked_quantity), 0),
                    func.coalesce(func.sum(WarehouseStock.packed_quantity * func.coalesce(Variant.pack_size, 1)), 0),
                    func.coalesce(func.sum(WarehouseStock.reserved_quantity), 0),
                )
                .select_from(WarehouseStock)
                .join(Variant, Variant.id == WarehouseStock.variant_id)
                .join(Warehouse, Warehouse.id == WarehouseStock.warehouse_id)
            )
        ).one()
        unpacked_units, packed_units, reserved_units = warehouse_stock_row
        BUSINESS_STOCK_UNITS.labels(source="warehouse", kind="unpacked_units").set(unpacked_units or 0)
        BUSINESS_STOCK_UNITS.labels(source="warehouse", kind="packed_units").set(packed_units or 0)
        BUSINESS_STOCK_UNITS.labels(source="warehouse", kind="reserved_units").set(reserved_units or 0)
        BUSINESS_STOCK_UNITS.labels(source="warehouse", kind="available_units").set(
            (unpacked_units or 0) + (packed_units or 0) - (reserved_units or 0)
        )

        ozon_stock_row = (
            await db.execute(
                select(
                    func.coalesce(func.sum(OzonStock.available_to_sell), 0),
                    func.coalesce(func.sum(OzonStock.in_supply), 0),
                    func.coalesce(func.sum(OzonStock.in_transit), 0),
                    func.coalesce(func.sum(OzonStock.returning), 0),
                )
            )
        ).one()
        available_to_sell, in_supply, in_transit, returning = ozon_stock_row
        BUSINESS_STOCK_UNITS.labels(source="ozon", kind="available_to_sell").set(available_to_sell or 0)
        BUSINESS_STOCK_UNITS.labels(source="ozon", kind="in_supply").set(in_supply or 0)
        BUSINESS_STOCK_UNITS.labels(source="ozon", kind="in_transit").set(in_transit or 0)
        BUSINESS_STOCK_UNITS.labels(source="ozon", kind="returning").set(returning or 0)

        recent_supply_events = (
            await db.execute(
                select(SupplyNotificationEvent)
                .where(
                    SupplyNotificationEvent.event_type.in_(("supply_created", "supply_status_changed")),
                )
                .order_by(SupplyNotificationEvent.created_at.desc())
                .limit(20)
            )
        ).scalars().all()
        for event in recent_supply_events:
            created_at = event.created_at
            timestamp = created_at.timestamp() if created_at else 0
            BUSINESS_RECENT_SUPPLY_EVENT.labels(
                event_type=event.event_type,
                store_name=event.store_name,
                order_number=event.order_number,
                status_before=_supply_status_label(event.status_before),
                status_after=_supply_status_label(event.status_after),
                delivery_state="sent" if event.telegram_sent_at else "pending",
            ).set(timestamp)

        pending_supply_rows = (
            await db.execute(
                select(
                    SupplyNotificationEvent.event_type,
                    func.count(SupplyNotificationEvent.id),
                )
                .where(SupplyNotificationEvent.telegram_sent_at.is_(None))
                .group_by(SupplyNotificationEvent.event_type)
            )
        ).all()
        for event_type, count in pending_supply_rows:
            BUSINESS_PENDING_SUPPLY_EVENTS.labels(event_type=event_type).set(count)

    recent_events = await get_recent_admin_events(50)
    grouped_events: dict[tuple[str, str], int] = {}
    for event in recent_events:
        event_type = str(event.get("event_type") or "-")
        severity = str(event.get("severity") or "-")
        grouped_events[(event_type, severity)] = grouped_events.get((event_type, severity), 0) + 1

    for (event_type, severity), count in grouped_events.items():
        BUSINESS_ADMIN_EVENTS_RECENT.labels(event_type=event_type, severity=severity).set(count)


def _path_label(request: Request) -> str:
    route = request.scope.get("route")
    route_path = getattr(route, "path", None)
    return route_path or request.url.path


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)

        started_at = perf_counter()
        status_code = 500
        path_label = request.url.path

        try:
            response = await call_next(request)
            status_code = response.status_code
            path_label = _path_label(request)
            return response
        except Exception:
            path_label = _path_label(request)
            raise
        finally:
            duration = perf_counter() - started_at
            HTTP_REQUESTS_TOTAL.labels(
                method=request.method,
                path=path_label,
                status=str(status_code),
            ).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(
                method=request.method,
                path=path_label,
            ).observe(duration)


def setup_metrics(app: FastAPI) -> None:
    app.add_middleware(MetricsMiddleware)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        await _refresh_business_metrics()
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
