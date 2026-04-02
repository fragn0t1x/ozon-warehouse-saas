from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any

import httpx
from sqlalchemy import delete, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.store import Store
from app.models.store_month_finance import StoreMonthFinance, StoreMonthOfferFinance
from app.services.admin_notifications import notify_closed_month_issue
from app.services.economics_history_service import EconomicsHistoryService
from app.services.ozon.client import OzonClient
from app.services.ozon.finance_snapshot_service import OzonFinanceSnapshotService
from app.services.unit_economics_math import calculate_tax_amount, revenue_net_of_vat
from app.utils.encryption import decrypt_api_key


@dataclass
class ClosedMonthSyncResult:
    month: str
    status: str
    is_final: bool
    offers_total: int
    offers_with_cost: int


class ClosedMonthHistoryService:
    MAX_HISTORY_MONTHS = 24
    SYNC_BATCH_MONTHS = 6
    USER_VISIBLE_MONTH_STATUSES = ("ready", "needs_cost", "ozon_warning")

    def __init__(self, db: AsyncSession):
        self.db = db
        self._history_service = EconomicsHistoryService(db)

    @staticmethod
    def _parse_month(month: str) -> tuple[int, int]:
        try:
            year_str, month_str = str(month).strip().split("-", 1)
            year = int(year_str)
            month_number = int(month_str)
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("Month must be in YYYY-MM format") from exc
        if month_number < 1 or month_number > 12:
            raise ValueError("Month must be in YYYY-MM format")
        return year, month_number

    @staticmethod
    def _shift_month(month: str, offset: int) -> str:
        year, month_number = ClosedMonthHistoryService._parse_month(month)
        absolute = year * 12 + (month_number - 1) + offset
        shifted_year = absolute // 12
        shifted_month = absolute % 12 + 1
        return f"{shifted_year:04d}-{shifted_month:02d}"

    @staticmethod
    def _month_to_index(month: str) -> int:
        year, month_number = ClosedMonthHistoryService._parse_month(month)
        return year * 12 + (month_number - 1)

    @staticmethod
    def _index_to_month(value: int) -> str:
        year = value // 12
        month_number = value % 12 + 1
        return f"{year:04d}-{month_number:02d}"

    @staticmethod
    def _latest_closed_month() -> str:
        today = datetime.now(timezone.utc).date().replace(day=1)
        current_month = f"{today.year:04d}-{today.month:02d}"
        return ClosedMonthHistoryService._shift_month(current_month, -1)

    @classmethod
    def _earliest_allowed_closed_month(cls) -> str:
        return cls._shift_month(cls._latest_closed_month(), -(cls.MAX_HISTORY_MONTHS - 1))

    @classmethod
    def _previous_closed_months(cls, limit: int) -> list[str]:
        latest_closed_month = cls._latest_closed_month()
        latest_index = cls._month_to_index(latest_closed_month)
        requested_limit = min(max(int(limit), 0), cls.MAX_HISTORY_MONTHS)
        start_index = max(latest_index - requested_limit + 1, 0)
        return [cls._index_to_month(index) for index in range(latest_index, start_index - 1, -1)]

    @classmethod
    def _closed_months_from_start(cls, start_month: str) -> list[str]:
        latest_closed_month = cls._latest_closed_month()
        earliest_allowed_month = cls._earliest_allowed_closed_month()
        start_index = max(cls._month_to_index(start_month), cls._month_to_index(earliest_allowed_month))
        latest_index = cls._month_to_index(latest_closed_month)
        if start_index > latest_index:
            return []
        return [cls._index_to_month(index) for index in range(latest_index, start_index - 1, -1)]

    async def _get_store(self, store_id: int, owner_user_id: int) -> Store:
        result = await self.db.execute(
            select(Store).where(Store.id == store_id, Store.user_id == owner_user_id)
        )
        store = result.scalar_one_or_none()
        if not store:
            raise ValueError(f"Store {store_id} not found")
        return store

    @staticmethod
    def _month_bounds(month: str) -> tuple[date, date]:
        year, month_number = ClosedMonthHistoryService._parse_month(month)
        last_day = monthrange(year, month_number)[1]
        return date(year, month_number, 1), date(year, month_number, last_day)

    @staticmethod
    def _friendly_month_error_message(exc: Exception) -> str:
        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            if status_code == 404:
                return "Ozon не отдал отчет реализации за этот месяц"
            if status_code == 403:
                return "Ozon не дал доступ к отчету реализации за этот месяц"
            if status_code >= 500:
                return "Ozon временно не отдал отчет реализации"
            return f"Ozon вернул ошибку {status_code} при загрузке месяца"
        message = str(exc).strip()
        return message or "Не удалось загрузить закрытый месяц"

    @classmethod
    def _humanize_month_warning(cls, warning: dict[str, Any]) -> str:
        kind = str(warning.get("kind") or "").strip().lower()
        error = cls._friendly_month_error_message(
            Exception(str(warning.get("error") or "Не удалось получить часть данных Ozon"))
        )
        date_from = str(warning.get("date_from") or "").strip()
        date_to = str(warning.get("date_to") or "").strip()
        if date_from and date_to:
            return f"{date_from} -> {date_to}: {error}"
        if kind:
            return f"{kind}: {error}"
        return error

    @staticmethod
    def _is_noncritical_adjustment_warning(warning: dict[str, Any]) -> bool:
        kind = str(warning.get("kind") or "").strip().lower()
        missing_document = bool(warning.get("missing_document"))
        return missing_document and kind in {"compensation", "decompensation"}

    @classmethod
    def _classify_month_warnings(
        cls,
        *,
        adjustment_warnings: list[dict[str, Any]],
        transaction_warnings: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        critical: list[dict[str, Any]] = []
        informational: list[dict[str, Any]] = []

        for warning in adjustment_warnings:
            if cls._is_noncritical_adjustment_warning(warning):
                informational.append(warning)
            else:
                critical.append(warning)

        critical.extend(transaction_warnings)
        return critical, informational

    async def _record_failed_month(
        self,
        *,
        store: Store,
        month: str,
        message: str,
    ) -> ClosedMonthSyncResult:
        existing = await self.db.execute(
            select(StoreMonthFinance).where(
                StoreMonthFinance.store_id == store.id,
                StoreMonthFinance.month == month,
            )
        )
        month_row = existing.scalar_one_or_none()
        now = datetime.now(timezone.utc)

        if month_row is not None and month_row.realization_available:
            payload = dict(month_row.source_payload or {})
            payload["last_sync_error"] = message
            month_row.checked_at = now
            month_row.source_payload = payload
            await self.db.commit()
            await notify_closed_month_issue(
                self.db,
                store_id=store.id,
                store_name=store.name,
                month=month,
                issue_type="error",
                title=f"Ошибка пересборки закрытого месяца: {store.name} · {month}",
                summary=message,
                details=["В базе остался предыдущий успешный снимок месяца."],
            )
            await self.db.commit()
            return ClosedMonthSyncResult(
                month=month,
                status=month_row.status,
                is_final=month_row.is_final,
                offers_total=int(month_row.sold_units or 0),
                offers_with_cost=0,
            )

        if month_row is None:
            month_row = StoreMonthFinance(store_id=store.id, month=month)
            self.db.add(month_row)
            await self.db.flush()
        else:
            await self.db.execute(
                delete(StoreMonthOfferFinance).where(
                    StoreMonthOfferFinance.store_month_finance_id == month_row.id
                )
            )
            await self.db.flush()

        month_row.status = "error"
        month_row.is_final = False
        month_row.is_locked = False
        month_row.realization_available = False
        month_row.coverage_ratio = 0.0
        month_row.sold_units = 0
        month_row.sold_amount = 0.0
        month_row.returned_units = 0
        month_row.returned_amount = 0.0
        month_row.revenue_amount = 0.0
        month_row.revenue_net_of_vat = 0.0
        month_row.cogs = 0.0
        month_row.gross_profit = 0.0
        month_row.ozon_commission = 0.0
        month_row.ozon_logistics = 0.0
        month_row.ozon_services = 0.0
        month_row.ozon_acquiring = 0.0
        month_row.ozon_other_expenses = 0.0
        month_row.ozon_incentives = 0.0
        month_row.ozon_compensation = 0.0
        month_row.ozon_decompensation = 0.0
        month_row.ozon_adjustments_net = 0.0
        month_row.profit_before_tax = 0.0
        month_row.tax_amount = 0.0
        month_row.net_profit = 0.0
        month_row.vat_mode_used = None
        month_row.tax_mode_used = None
        month_row.tax_rate_used = None
        month_row.tax_effective_from_used = None
        month_row.cost_basis = None
        month_row.cost_snapshot_date = None
        month_row.generated_at = now
        month_row.checked_at = now
        month_row.source_payload = {
            "error": message,
            "realization": {"period": month, "available": False},
        }
        await self.db.commit()
        await notify_closed_month_issue(
            self.db,
            store_id=store.id,
            store_name=store.name,
            month=month,
            issue_type="error",
            title=f"Ошибка закрытого месяца: {store.name} · {month}",
            summary=message,
            details=[],
        )
        await self.db.commit()
        return ClosedMonthSyncResult(
            month=month,
            status="error",
            is_final=False,
            offers_total=0,
            offers_with_cost=0,
        )

    @staticmethod
    def _build_source_payload(
        *,
        realization: dict[str, Any],
        cashflow: dict[str, Any],
        adjustments: dict[str, Any],
        transactions: dict[str, Any],
        critical_warnings: list[dict[str, Any]] | None = None,
        informational_warnings: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return {
            "realization": {
                "period": realization.get("period"),
                "rows_count": realization.get("rows_count"),
                "sold_units": realization.get("sold_units"),
                "sold_amount": realization.get("sold_amount"),
                "returned_units": realization.get("returned_units"),
                "returned_amount": realization.get("returned_amount"),
                "net_units": realization.get("net_units"),
                "net_amount": realization.get("net_amount"),
                "net_fee": realization.get("net_fee"),
                "net_bonus": realization.get("net_bonus"),
                "net_incentives": realization.get("net_incentives"),
            },
            "cashflow": {
                "date_from": cashflow.get("date_from"),
                "date_to": cashflow.get("date_to"),
                "orders_amount": cashflow.get("orders_amount"),
                "returns_amount": cashflow.get("returns_amount"),
                "commission_amount": cashflow.get("commission_amount"),
                "services_amount": cashflow.get("services_amount"),
                "logistics_amount": cashflow.get("logistics_amount"),
                "periods_count": cashflow.get("periods_count"),
            },
            "adjustments": {
                "compensation_total": adjustments.get("compensation_total"),
                "decompensation_total": adjustments.get("decompensation_total"),
                "warnings": adjustments.get("warnings") or [],
                "adjustments_available": adjustments.get("adjustments_available"),
            },
            "transactions": {
                "period": transactions.get("period") or {},
                "totals": transactions.get("totals") or {},
                "service_buckets": transactions.get("service_buckets") or {},
                "warnings": transactions.get("warnings") or [],
                "details_available": transactions.get("details_available"),
            },
            "warnings_summary": {
                "critical": critical_warnings or [],
                "informational": informational_warnings or [],
                "has_critical": bool(critical_warnings),
                "has_informational": bool(informational_warnings),
            },
        }

    async def sync_store_month(
        self,
        *,
        store: Store,
        month: str,
        commit: bool = True,
    ) -> ClosedMonthSyncResult:
        year, month_number = self._parse_month(month)
        date_from, date_to = self._month_bounds(month)
        date_from_dt = datetime.combine(date_from, time.min, tzinfo=timezone.utc)
        date_to_dt = datetime.combine(date_to, time.max, tzinfo=timezone.utc)
        existing = await self.db.execute(
            select(StoreMonthFinance).where(
                StoreMonthFinance.store_id == store.id,
                StoreMonthFinance.month == month,
            )
        )
        existing_row = existing.scalar_one_or_none()
        if existing_row is not None and existing_row.is_locked:
            return self._result_from_month_row(existing_row)

        economics_snapshot = await self._history_service.get_store_economics_for_date(
            store=store,
            as_of=date_to,
        )
        variant_costs = await self._history_service.load_variant_costs_for_date(
            user_id=store.user_id,
            as_of=date_to,
        )

        client = OzonClient(
            str(store.client_id),
            decrypt_api_key(store.api_key_encrypted),
            store_name=store.name,
            emit_notifications=False,
        )
        try:
            finance_snapshot_service = OzonFinanceSnapshotService(client)
            realization_report = await client.get_realization_report(month=month_number, year=year)
            realization_snapshot = finance_snapshot_service._build_realization_snapshot(
                realization_report,
                year=year,
                month=month_number,
            )
            cashflow_summary = await finance_snapshot_service.fetch_range_summary(
                date_from=date_from,
                date_to=date_to,
            )
            adjustments = await finance_snapshot_service._load_adjustment_reports([month])
            try:
                transaction_snapshot = await finance_snapshot_service._load_recent_transaction_snapshot(
                    date_from=date_from_dt,
                    date_to=date_to_dt,
                )
            except Exception as exc:
                transaction_snapshot = {
                    "period": {
                        "from": date_from_dt.isoformat().replace("+00:00", "Z"),
                        "to": date_to_dt.isoformat().replace("+00:00", "Z"),
                        "days": max((date_to - date_from).days + 1, 1),
                    },
                    "totals": {
                        "accruals_for_sale": 0.0,
                        "compensation_amount": 0.0,
                        "money_transfer": 0.0,
                        "others_amount": 0.0,
                        "processing_and_delivery": 0.0,
                        "refunds_and_cancellations": 0.0,
                        "sale_commission": 0.0,
                        "services_amount": 0.0,
                    },
                    "service_buckets": {
                        "marketing": 0.0,
                        "storage": 0.0,
                        "acquiring": 0.0,
                        "returns": 0.0,
                        "logistics": 0.0,
                        "other": 0.0,
                    },
                    "warnings": [
                        {
                            "kind": "transactions",
                            "error": self._friendly_month_error_message(exc),
                        }
                    ],
                    "details_available": False,
                }
        finally:
            await client.close()

        adjustment_total = round(
            float(adjustments.get("compensation_total") or 0)
            - float(adjustments.get("decompensation_total") or 0),
            2,
        )
        total_revenue = round(float(realization_snapshot.get("net_amount") or 0), 2)
        total_revenue_net_of_vat = round(revenue_net_of_vat(total_revenue, economics_snapshot.vat_mode), 2)
        total_commission = round(abs(float(realization_snapshot.get("net_fee") or 0)), 2)
        total_incentives = round(float(realization_snapshot.get("net_incentives") or 0), 2)
        transaction_totals = transaction_snapshot.get("totals") or {}
        transaction_buckets = transaction_snapshot.get("service_buckets") or {}
        total_services = round(abs(float(transaction_totals.get("services_amount") or 0)), 2)
        if total_services <= 0:
            total_services = round(abs(float(cashflow_summary.get("services_amount") or 0)), 2)
        total_logistics = round(
            abs(float(transaction_totals.get("processing_and_delivery") or 0))
            + abs(float(transaction_totals.get("refunds_and_cancellations") or 0)),
            2,
        )
        if total_logistics <= 0:
            total_logistics = round(abs(float(cashflow_summary.get("logistics_amount") or 0)), 2)
        total_other_expenses = round(abs(float(transaction_totals.get("others_amount") or 0)), 2)
        total_acquiring = round(
            min(
                total_other_expenses,
                abs(float(transaction_buckets.get("acquiring") or 0)),
            ),
            2,
        )
        total_other_misc = round(max(total_other_expenses - total_acquiring, 0.0), 2)

        offers_total = 0
        offers_with_cost = 0
        cogs_total = 0.0
        gross_profit_total = 0.0
        profit_before_tax_total = 0.0
        tax_total = 0.0
        net_profit_total = 0.0
        offer_rows: list[StoreMonthOfferFinance] = []

        for item in realization_snapshot.get("items") or []:
            offer_id = str(item.get("offer_id") or "").strip()
            if not offer_id:
                continue

            offers_total += 1
            net_amount = round(float(item.get("net_amount") or 0), 2)
            sold_units = int(item.get("sold_units") or 0)
            returned_units = int(item.get("returned_units") or 0)
            net_units = int(item.get("net_units") or 0)
            revenue_ex_vat = round(revenue_net_of_vat(net_amount, economics_snapshot.vat_mode), 2)
            revenue_share = (net_amount / total_revenue) if total_revenue > 0 else 0.0
            cost_snapshot = variant_costs.get((store.id, offer_id))
            unit_cost = cost_snapshot.unit_cost if cost_snapshot is not None else None
            cost_effective_from = cost_snapshot.effective_from if cost_snapshot is not None else None
            has_cost = unit_cost is not None
            if has_cost:
                offers_with_cost += 1

            allocated_services = round(total_services * revenue_share, 2)
            allocated_logistics = round(total_logistics * revenue_share, 2)
            allocated_other_expenses = round(total_other_expenses * revenue_share, 2)
            allocated_acquiring = round(total_acquiring * revenue_share, 2)
            allocated_adjustments = round(adjustment_total * revenue_share, 2)
            realized_commission = round(abs(float(item.get("net_fee") or 0)), 2)
            realized_incentives = round(float(item.get("net_incentives") or 0), 2)

            cogs = None
            gross_profit = None
            profit_before_tax = None
            tax_amount = None
            net_profit = None
            margin_ratio = None

            if has_cost:
                cogs = round(float(unit_cost or 0) * max(net_units, 0), 2)
                gross_profit = round(revenue_ex_vat - cogs, 2)
                profit_before_tax = round(
                    gross_profit
                    + realized_incentives
                    - realized_commission
                    - allocated_services
                    - allocated_logistics
                    - allocated_other_expenses
                    + allocated_adjustments,
                    2,
                )
                tax_amount = round(
                    calculate_tax_amount(
                        revenue_net=revenue_ex_vat,
                        profit_before_tax=profit_before_tax,
                        tax_mode=economics_snapshot.tax_mode,
                        tax_rate=economics_snapshot.tax_rate,
                    ),
                    2,
                )
                net_profit = round(profit_before_tax - tax_amount, 2)
                margin_ratio = round((net_profit / revenue_ex_vat), 4) if revenue_ex_vat > 0 else None

                cogs_total += cogs
                gross_profit_total += gross_profit
                profit_before_tax_total += profit_before_tax
                tax_total += tax_amount
                net_profit_total += net_profit

            offer_rows.append(
                StoreMonthOfferFinance(
                    store_id=store.id,
                    month=month,
                    offer_id=offer_id,
                    title=str(item.get("title") or "").strip() or None,
                    basis="realization_closed_month",
                    sold_units=sold_units,
                    sold_amount=round(float(item.get("sold_amount") or 0), 2),
                    returned_units=returned_units,
                    returned_amount=round(float(item.get("returned_amount") or 0), 2),
                    net_units=net_units,
                    revenue_amount=net_amount,
                    revenue_net_of_vat=revenue_ex_vat,
                    ozon_commission=realized_commission,
                    ozon_logistics=allocated_logistics,
                    ozon_services=allocated_services,
                    ozon_acquiring=allocated_acquiring,
                    ozon_other_expenses=round(max(allocated_other_expenses - allocated_acquiring, 0.0), 2),
                    ozon_incentives=realized_incentives,
                    ozon_adjustments_net=allocated_adjustments,
                    unit_cost=float(unit_cost) if unit_cost is not None else None,
                    cogs=cogs,
                    gross_profit=gross_profit,
                    profit_before_tax=profit_before_tax,
                    tax_amount=tax_amount,
                    net_profit=net_profit,
                    margin_ratio=margin_ratio,
                    vat_mode_used=economics_snapshot.vat_mode,
                    tax_mode_used=economics_snapshot.tax_mode,
                    tax_rate_used=economics_snapshot.tax_rate,
                    tax_effective_from_used=economics_snapshot.effective_from,
                    cost_effective_from_used=cost_effective_from,
                    has_cost=has_cost,
                )
            )

        coverage_ratio = round((offers_with_cost / offers_total), 4) if offers_total else 0.0
        adjustment_warnings = list(adjustments.get("warnings") or [])
        transaction_warnings = list(transaction_snapshot.get("warnings") or [])
        critical_warnings, informational_warnings = self._classify_month_warnings(
            adjustment_warnings=adjustment_warnings,
            transaction_warnings=transaction_warnings,
        )
        status = "ready"
        if coverage_ratio < 1.0:
            status = "needs_cost"
        elif critical_warnings:
            status = "ozon_warning"

        month_row = existing_row
        if month_row is None:
            month_row = StoreMonthFinance(store_id=store.id, month=month)
            self.db.add(month_row)
            await self.db.flush()
        else:
            await self.db.execute(
                delete(StoreMonthOfferFinance).where(
                    StoreMonthOfferFinance.store_month_finance_id == month_row.id
                )
            )
            await self.db.flush()

        month_row.status = status
        month_row.is_final = not bool(critical_warnings)
        month_row.is_locked = status == "ready"
        month_row.realization_available = True
        month_row.coverage_ratio = coverage_ratio
        month_row.sold_units = int(realization_snapshot.get("sold_units") or 0)
        month_row.sold_amount = round(float(realization_snapshot.get("sold_amount") or 0), 2)
        month_row.returned_units = int(realization_snapshot.get("returned_units") or 0)
        month_row.returned_amount = round(float(realization_snapshot.get("returned_amount") or 0), 2)
        month_row.revenue_amount = total_revenue
        month_row.revenue_net_of_vat = total_revenue_net_of_vat
        month_row.cogs = round(cogs_total, 2)
        month_row.gross_profit = round(gross_profit_total, 2)
        month_row.ozon_commission = total_commission
        month_row.ozon_logistics = total_logistics
        month_row.ozon_services = total_services
        month_row.ozon_acquiring = total_acquiring
        month_row.ozon_other_expenses = total_other_misc
        month_row.ozon_incentives = total_incentives
        month_row.ozon_compensation = round(float(adjustments.get("compensation_total") or 0), 2)
        month_row.ozon_decompensation = round(float(adjustments.get("decompensation_total") or 0), 2)
        month_row.ozon_adjustments_net = adjustment_total
        month_row.profit_before_tax = round(profit_before_tax_total, 2)
        month_row.tax_amount = round(tax_total, 2)
        month_row.net_profit = round(net_profit_total, 2)
        month_row.vat_mode_used = economics_snapshot.vat_mode
        month_row.tax_mode_used = economics_snapshot.tax_mode
        month_row.tax_rate_used = economics_snapshot.tax_rate
        month_row.tax_effective_from_used = economics_snapshot.effective_from
        month_row.cost_basis = "month_end_cost"
        month_row.cost_snapshot_date = date_to
        month_row.generated_at = datetime.now(timezone.utc)
        month_row.checked_at = datetime.now(timezone.utc)
        month_row.source_payload = self._build_source_payload(
            realization=realization_snapshot,
            cashflow=cashflow_summary,
            adjustments=adjustments,
            transactions=transaction_snapshot,
            critical_warnings=critical_warnings,
            informational_warnings=informational_warnings,
        )

        await self.db.flush()
        for row in offer_rows:
            row.store_month_finance_id = month_row.id
            self.db.add(row)

        if commit:
            await self.db.commit()
            if status == "ozon_warning":
                warning_details = [self._humanize_month_warning(warning) for warning in critical_warnings[:5]]
                await notify_closed_month_issue(
                    self.db,
                    store_id=store.id,
                    store_name=store.name,
                    month=month,
                    issue_type="ozon_warning",
                    title=f"Ограничения Ozon в закрытом месяце: {store.name} · {month}",
                    summary="Ozon отдал месяц с техническими ограничениями. Основные цифры собраны, но часть технических данных пришла неидеально.",
                    details=warning_details,
                )
                await self.db.commit()

        return ClosedMonthSyncResult(
            month=month,
            status=status,
            is_final=month_row.is_final,
            offers_total=offers_total,
            offers_with_cost=offers_with_cost,
        )

    async def sync_recent_closed_months(
        self,
        *,
        store: Store,
        months_back: int = 3,
        commit_each: bool = True,
    ) -> list[ClosedMonthSyncResult]:
        results: list[ClosedMonthSyncResult] = []
        for month in self._previous_closed_months(months_back):
            try:
                results.append(await self.sync_store_month(store=store, month=month, commit=commit_each))
            except Exception as exc:
                results.append(
                    await self._record_failed_month(
                        store=store,
                        month=month,
                        message=self._friendly_month_error_message(exc),
                    )
                )
        return results

    async def list_months(
        self,
        *,
        store_id: int,
        owner_user_id: int,
        limit: int = 12,
        include_non_ready: bool = False,
    ) -> list[StoreMonthFinance]:
        await self._get_store(store_id, owner_user_id)
        filters = [
            StoreMonthFinance.store_id == store_id,
            StoreMonthFinance.realization_available.is_(True),
            or_(
                StoreMonthFinance.sold_units > 0,
                StoreMonthFinance.sold_amount > 0,
                StoreMonthFinance.revenue_amount > 0,
            ),
        ]
        if not include_non_ready:
            filters.append(StoreMonthFinance.status.in_(self.USER_VISIBLE_MONTH_STATUSES))
        effective_limit = min(max(int(limit), 1), self.MAX_HISTORY_MONTHS)
        result = await self.db.execute(
            select(StoreMonthFinance)
            .where(*filters)
            .order_by(StoreMonthFinance.month.desc())
            .limit(effective_limit)
        )
        return list(result.scalars().all())

    async def get_month(
        self,
        *,
        store_id: int,
        owner_user_id: int,
        month: str,
        include_non_ready: bool = False,
    ) -> StoreMonthFinance | None:
        await self._get_store(store_id, owner_user_id)
        filters = [
            StoreMonthFinance.store_id == store_id,
            StoreMonthFinance.month == month,
        ]
        if not include_non_ready:
            filters.append(StoreMonthFinance.status.in_(self.USER_VISIBLE_MONTH_STATUSES))
        result = await self.db.execute(select(StoreMonthFinance).where(*filters))
        return result.scalar_one_or_none()

    async def list_month_offers(
        self,
        *,
        store_id: int,
        owner_user_id: int,
        month: str,
        include_non_ready: bool = False,
    ) -> list[StoreMonthOfferFinance]:
        month_row = await self.get_month(
            store_id=store_id,
            owner_user_id=owner_user_id,
            month=month,
            include_non_ready=include_non_ready,
        )
        if month_row is None:
            return []
        result = await self.db.execute(
            select(StoreMonthOfferFinance)
            .where(
                StoreMonthOfferFinance.store_id == store_id,
                StoreMonthOfferFinance.month == month,
            )
            .order_by(
                StoreMonthOfferFinance.net_profit.desc().nullslast(),
                StoreMonthOfferFinance.revenue_amount.desc(),
            )
        )
        return list(result.scalars().all())
    @staticmethod
    def _result_from_month_row(month_row: StoreMonthFinance) -> ClosedMonthSyncResult:
        offers_total = int((month_row.source_payload or {}).get("realization", {}).get("rows_count") or 0)
        offers_with_cost = int(round(float(month_row.coverage_ratio or 0) * offers_total)) if offers_total else 0
        return ClosedMonthSyncResult(
            month=month_row.month,
            status=month_row.status,
            is_final=month_row.is_final,
            offers_total=offers_total,
            offers_with_cost=offers_with_cost,
        )
