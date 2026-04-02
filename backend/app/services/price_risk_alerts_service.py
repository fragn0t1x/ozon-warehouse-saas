from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.store import Store
from app.models.user import User
from app.models.user_settings import UserSettings
from app.services.dashboard_unit_economics_service import DashboardUnitEconomicsService
from app.services.economics_history_service import EconomicsHistoryService
from app.utils.redis_cache import get_redis


ALERT_WINDOW_HOURS = 72
ALERT_REPEAT_SECONDS = 24 * 60 * 60
ALERT_MAX_NOTIFICATIONS = 3


@dataclass
class PriceRiskCandidate:
    store: Store
    row: dict[str, Any]
    status: str
    severity: str
    title: str
    html_text: str
    plain_text: str
    action_url: str


class PriceRiskAlertsService:
    STATUS_ORDER = {
        "none": 0,
        "low_margin": 1,
        "break_even": 2,
        "loss": 3,
        "critical_loss": 4,
    }

    def __init__(self, db: AsyncSession):
        self.db = db
        self.unit_service = DashboardUnitEconomicsService()

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(UTC)

    @classmethod
    def _state_key(cls, store_id: int, offer_id: str) -> str:
        return f"unit-risk:state:{store_id}:{offer_id}"

    @staticmethod
    def _format_currency(value: float | int | None) -> str:
        amount = float(value or 0)
        sign = "-" if amount < 0 else ""
        return f"{sign}{abs(round(amount)):,} ₽".replace(",", " ")

    @staticmethod
    def _format_percent_ratio(value: float | int | None) -> str:
        return f"{round(float(value or 0) * 100, 1)}%"

    @classmethod
    def classify_row(cls, row: dict[str, Any]) -> str:
        if not row.get("current_profitability_available"):
            return "none"
        if float(row.get("unit_cost") or 0) <= 0:
            return "none"

        net_profit = float(row.get("current_estimated_net_profit") or 0)
        margin = float(row.get("current_margin_ratio") or 0)
        if net_profit < -50 or margin <= -0.05:
            return "critical_loss"
        if net_profit < 0:
            return "loss"
        if net_profit <= 10:
            return "break_even"
        if margin < 0.05:
            return "low_margin"
        return "none"

    @classmethod
    def _severity_for_status(cls, status: str) -> str:
        return "error" if status in {"loss", "critical_loss"} else "warning"

    @classmethod
    def _title_for_status(cls, status: str) -> str:
        return {
            "critical_loss": "Товар продается в сильный минус",
            "loss": "Товар начал продаваться в минус",
            "break_even": "Товар продается почти в ноль",
            "low_margin": "У товара стала очень низкая маржа",
        }.get(status, "Нужно проверить цену товара")

    @classmethod
    def _action_url(cls, store_id: int) -> str:
        return "/unit-economics-calculator"

    @classmethod
    def _build_text(cls, *, store: Store, row: dict[str, Any], status: str) -> tuple[str, str]:
        title = cls._title_for_status(status)
        offer_id = str(row.get("offer_id") or "-")
        product_title = str(row.get("title") or offer_id)
        current_price = cls._format_currency(row.get("current_price_gross"))
        net_profit = cls._format_currency(row.get("current_estimated_net_profit"))
        margin = cls._format_percent_ratio(row.get("current_margin_ratio"))
        commission = cls._format_currency(abs(float(row.get("current_allocated_commission") or 0)))
        logistics = cls._format_currency(abs(float(row.get("current_allocated_logistics") or 0)))
        marketing = cls._format_currency(abs(float(row.get("current_allocated_marketing") or 0)))
        acquiring = cls._format_currency(abs(float(row.get("current_allocated_compensation") or 0)))
        return_reserve = cls._format_currency(abs(float(row.get("current_return_reserve") or 0)))
        risk_label = {
            "critical_loss": "сильный минус",
            "loss": "минус",
            "break_even": "почти в ноль",
            "low_margin": "очень низкая маржа",
        }.get(status, status)

        plain = (
            f"{title}\n\n"
            f"Магазин: {store.name}\n"
            f"Товар: {product_title}\n"
            f"Артикул: {offer_id}\n"
            f"Статус: {risk_label}\n"
            f"Текущая цена: {current_price}\n"
            f"Чистая прибыль на 1 ед.: {net_profit}\n"
            f"Маржа: {margin}\n"
            f"Комиссия Ozon: {commission}\n"
            f"Логистика Ozon: {logistics}\n"
            f"Маркетинг: {marketing}\n"
            f"Эквайринг: {acquiring}\n"
            f"Резерв на возвраты: {return_reserve}\n\n"
            "Проверь цену на Ozon и юнит-экономику этого SKU."
        )
        html = (
            f"⚠️ <b>{title}</b>\n\n"
            f"<b>Магазин:</b> {store.name}\n"
            f"<b>Товар:</b> {product_title}\n"
            f"<b>Артикул:</b> <code>{offer_id}</code>\n"
            f"<b>Статус:</b> {risk_label}\n"
            f"<b>Текущая цена:</b> {current_price}\n"
            f"<b>Чистая прибыль на 1 ед.:</b> {net_profit}\n"
            f"<b>Маржа:</b> {margin}\n"
            f"<b>Комиссия Ozon:</b> {commission}\n"
            f"<b>Логистика Ozon:</b> {logistics}\n"
            f"<b>Маркетинг:</b> {marketing}\n"
            f"<b>Эквайринг:</b> {acquiring}\n"
            f"<b>Резерв на возвраты:</b> {return_reserve}\n\n"
            "Проверь цену на Ozon и юнит-экономику этого SKU."
        )
        return plain, html

    async def _load_target_stores(self, *, store_id: int | None = None) -> list[Store]:
        stmt = select(Store).where(Store.is_active == True)  # noqa: E712
        if store_id is not None:
            stmt = stmt.where(Store.id == store_id)
        result = await self.db.execute(stmt.order_by(Store.id.asc()))
        return list(result.scalars().all())

    async def _load_owner_and_targets(self, *, owner_user_id: int) -> tuple[User | None, list[tuple[User, UserSettings | None]]]:
        owner = await self.db.scalar(select(User).where(User.id == owner_user_id))
        result = await self.db.execute(
            select(User, UserSettings)
            .outerjoin(UserSettings, UserSettings.user_id == User.id)
            .where(
                User.is_active == True,  # noqa: E712
                User.is_admin == False,  # noqa: E712
                ((User.id == owner_user_id) | (User.owner_user_id == owner_user_id)),
            )
            .order_by(User.id.asc())
        )
        targets: list[tuple[User, UserSettings | None]] = []
        for user, settings in result.all():
            if settings is None or getattr(settings, "notify_losses", False):
                targets.append((user, settings))
        return owner, targets

    async def _build_store_rows(self, *, store: Store) -> list[dict[str, Any]]:
        economics_snapshot = await EconomicsHistoryService(self.db).get_store_economics_for_date(
            store=store,
            as_of=self._utcnow().date(),
        )
        economics_snapshot = await self.unit_service.build_live_rows(
            db=self.db,
            user_id=store.user_id,
            stores=[{
                "id": store.id,
                "name": store.name,
                "client_id": store.client_id,
                "api_key_encrypted": store.api_key_encrypted,
                "economics_vat_mode": economics_snapshot.vat_mode,
                "economics_tax_mode": economics_snapshot.tax_mode,
                "economics_tax_rate": economics_snapshot.tax_rate,
            }],
            store_id=store.id,
        )
        return economics_snapshot

    async def _get_state(self, *, store_id: int, offer_id: str) -> dict[str, Any] | None:
        redis = await get_redis()
        if not redis:
            return None
        raw = await redis.get(self._state_key(store_id, offer_id))
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    async def _set_state(self, *, store_id: int, offer_id: str, payload: dict[str, Any]) -> None:
        redis = await get_redis()
        if not redis:
            return
        await redis.set(self._state_key(store_id, offer_id), json.dumps(payload), ex=30 * 24 * 60 * 60)

    async def _clear_state(self, *, store_id: int, offer_id: str) -> None:
        redis = await get_redis()
        if not redis:
            return
        await redis.delete(self._state_key(store_id, offer_id))

    async def _should_send(self, *, store_id: int, offer_id: str, status: str) -> bool:
        state = await self._get_state(store_id=store_id, offer_id=offer_id)
        now = self._utcnow()
        if not state:
            await self._set_state(
                store_id=store_id,
                offer_id=offer_id,
                payload={
                    "status": status,
                    "first_seen_at": now.isoformat(),
                    "last_sent_at": now.isoformat(),
                    "sent_count": 1,
                },
            )
            return True

        previous_status = str(state.get("status") or "none")
        previous_order = self.STATUS_ORDER.get(previous_status, 0)
        current_order = self.STATUS_ORDER.get(status, 0)

        if current_order > previous_order:
            await self._set_state(
                store_id=store_id,
                offer_id=offer_id,
                payload={
                    "status": status,
                    "first_seen_at": now.isoformat(),
                    "last_sent_at": now.isoformat(),
                    "sent_count": 1,
                },
            )
            return True

        if current_order < previous_order:
            await self._set_state(
                store_id=store_id,
                offer_id=offer_id,
                payload={
                    "status": status,
                    "first_seen_at": now.isoformat(),
                    "last_sent_at": None,
                    "sent_count": 0,
                },
            )
            return False

        first_seen_at = state.get("first_seen_at")
        last_sent_at = state.get("last_sent_at")
        sent_count = int(state.get("sent_count") or 0)

        try:
            first_seen_dt = datetime.fromisoformat(first_seen_at) if first_seen_at else now
        except Exception:
            first_seen_dt = now
        try:
            last_sent_dt = datetime.fromisoformat(last_sent_at) if last_sent_at else None
        except Exception:
            last_sent_dt = None

        if (now - first_seen_dt).total_seconds() > ALERT_WINDOW_HOURS * 3600:
            await self._set_state(
                store_id=store_id,
                offer_id=offer_id,
                payload={
                    "status": status,
                    "first_seen_at": first_seen_dt.isoformat(),
                    "last_sent_at": last_sent_dt.isoformat() if last_sent_dt else None,
                    "sent_count": sent_count,
                },
            )
            return False

        if sent_count >= ALERT_MAX_NOTIFICATIONS:
            return False
        if last_sent_dt and (now - last_sent_dt).total_seconds() < ALERT_REPEAT_SECONDS:
            return False

        await self._set_state(
            store_id=store_id,
            offer_id=offer_id,
            payload={
                "status": status,
                "first_seen_at": first_seen_dt.isoformat(),
                "last_sent_at": now.isoformat(),
                "sent_count": sent_count + 1,
            },
        )
        return True

    async def evaluate_store(self, *, store: Store) -> list[PriceRiskCandidate]:
        rows = await self._build_store_rows(store=store)
        candidates: list[PriceRiskCandidate] = []
        for row in rows:
            status = self.classify_row(row)
            offer_id = str(row.get("offer_id") or "").strip()
            if not offer_id:
                continue
            if status == "none":
                await self._clear_state(store_id=store.id, offer_id=offer_id)
                continue
            if not await self._should_send(store_id=store.id, offer_id=offer_id, status=status):
                continue
            plain_text, html_text = self._build_text(store=store, row=row, status=status)
            candidates.append(
                PriceRiskCandidate(
                    store=store,
                    row=row,
                    status=status,
                    severity=self._severity_for_status(status),
                    title=self._title_for_status(status),
                    html_text=html_text,
                    plain_text=plain_text,
                    action_url=self._action_url(store.id),
                )
            )
        return candidates

    async def evaluate_all(self, *, store_id: int | None = None) -> list[PriceRiskCandidate]:
        candidates: list[PriceRiskCandidate] = []
        for store in await self._load_target_stores(store_id=store_id):
            candidates.extend(await self.evaluate_store(store=store))
        return candidates
