
# backend/app/services/sync_service.py
import asyncio
import json
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set

import httpx
from dateutil import parser
from httpx import HTTPStatusError
from loguru import logger
from sqlalchemy import delete, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.ozon_warehouse import Cluster, OzonStock, OzonWarehouse
from app.models.product import Product
from app.models.store import Store
from app.models.supply import Supply, SupplyItem
from app.models.user import User
from app.models.variant import Variant
from app.models.variant_attribute import VariantAttribute
from app.models.warehouse_product import WarehouseProduct
from app.services.admin_notifications import (
    _should_notify_timeslot_change,
    deliver_pending_supply_notification_events,
    queue_supply_created,
    queue_supply_status_changed,
    queue_supply_timeslot_changed,
)
from app.services.ozon.client import OzonClient, get_stocks_retry_wait_seconds
from app.services.product_grouping import (
    ATTR_ID_COLOR,
    ATTR_ID_COLOR_ALT,
    ATTR_ID_PACK_SIZE,
    ATTR_ID_SIZE,
    build_ozon_product_groups,
    extract_base_product_name,
    extract_pack_size_from_attribute,
    extract_pack_size_from_text,
    serialize_group_key,
)
from app.utils.encryption import decrypt_api_key
from app.utils.redis_cache import get_redis

SUPPLIES_FIRST_SYNC_COMPLETED_KEY = "store:{store_id}:supplies_first_sync_completed"

ATTR_ID_GENDER = 9163  # Пол (Мужской, Женский)

IGNORED_ATTR_IDS = {
    31, 8292, 23171, 4191, 11254, 4508, 4509, 4506, 4503, 4389, 4496, 4604,
    4309, 4501, 4495, 9437, 23077, 23074, 4596, 11071, 13164, 4300, 9661,
    4655, 22232, 4180, 4497, 22390, 9024, 4295, 4508, 13164,
}

ATTRIBUTE_MAPPING = {
    ATTR_ID_COLOR: "Цвет",
    ATTR_ID_COLOR_ALT: "Цвет",
    ATTR_ID_SIZE: "Размер",
    ATTR_ID_GENDER: "Пол",
    ATTR_ID_PACK_SIZE: "Упаковка",
}

FINAL_SUPPLY_STATUSES = {
    "COMPLETED",
    "CANCELLED",
    "REJECTED_AT_SUPPLY_WAREHOUSE",
}

HISTORICAL_FINAL_SUPPLY_NOTIFICATION_GRACE = timedelta(hours=24)
SUPPLY_ORDER_DETAILS_BATCH_SIZE = 50
SUPPLY_BUNDLE_BATCH_SIZE = 100


def normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.lower() in {"none", "null"}:
        return None
    return normalized


def build_unique_string_index(rows: list[tuple[str | None, int]]) -> dict[str, int]:
    candidates: dict[str, set[int]] = defaultdict(set)
    for raw_key, row_id in rows:
        key = normalize_optional_string(raw_key)
        if not key:
            continue
        candidates[key].add(int(row_id))
    return {key: next(iter(ids)) for key, ids in candidates.items() if len(ids) == 1}


def normalize_product_link_map(
    product_link_map: Optional[Dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    normalized: dict[str, list[dict[str, Any]]] = {}
    if not product_link_map:
        return normalized

    for raw_key, raw_value in product_link_map.items():
        key = str(raw_key)
        values = raw_value if isinstance(raw_value, list) else [raw_value]
        for value in values:
            if not isinstance(value, dict):
                continue
            offer_ids = [
                offer_id
                for item in value.get("offer_ids", []) or []
                if (offer_id := normalize_optional_string(item))
            ]
            normalized.setdefault(key, []).append(
                {
                    "warehouse_product_id": value.get("warehouse_product_id"),
                    "warehouse_product_name": normalize_optional_string(value.get("warehouse_product_name")),
                    "offer_ids": offer_ids,
                }
            )

    return normalized


def select_product_link_plan(
    product_link_map: dict[str, list[dict[str, Any]]],
    *,
    group_key: str,
    base_name: str,
    offer_id: str | None,
) -> dict[str, Any]:
    candidates = product_link_map.get(group_key) or product_link_map.get(base_name) or []
    if not candidates:
        return {}

    normalized_offer_id = normalize_optional_string(offer_id)
    if normalized_offer_id:
        for candidate in candidates:
            if normalized_offer_id in candidate.get("offer_ids", []):
                return candidate

    for candidate in candidates:
        if not candidate.get("offer_ids"):
            return candidate

    return candidates[0]


def chunked(seq: list[Any], size: int) -> list[list[Any]]:
    if size <= 0:
        raise ValueError("size must be positive")
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def parse_ozon_date(date_str):
    if not date_str:
        return None
    try:
        dt = parser.isoparse(date_str)
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except Exception:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            return dt
        except Exception:
            logger.warning(f"Failed to parse date: {date_str}")
            return None


def should_notify_supply_created(*, status: str, created_at: datetime | None) -> bool:
    if status not in FINAL_SUPPLY_STATUSES:
        return True
    if created_at is None:
        return False
    return created_at >= datetime.now() - HISTORICAL_FINAL_SUPPLY_NOTIFICATION_GRACE


async def _is_supplies_first_sync_completed(store_id: int) -> bool:
    redis = await get_redis()
    if not redis:
        return False
    try:
        return bool(await redis.get(SUPPLIES_FIRST_SYNC_COMPLETED_KEY.format(store_id=store_id)))
    except Exception as e:
        logger.warning(f"Failed to read supplies first sync flag for store {store_id}: {e}")
        return False


async def _mark_supplies_first_sync_completed(store_id: int) -> None:
    redis = await get_redis()
    if not redis:
        return
    try:
        await redis.set(SUPPLIES_FIRST_SYNC_COMPLETED_KEY.format(store_id=store_id), "1")
    except Exception as e:
        logger.warning(f"Failed to persist supplies first sync flag for store {store_id}: {e}")


def is_valid_attribute_value(value: str) -> bool:
    if not value or not isinstance(value, str):
        return False
    value = value.strip()
    if len(value) > 100:
        return False
    if value.startswith("{") or value.startswith("["):
        return False
    if "<" in value and ">" in value:
        return False
    return True


def get_size_order(size: str) -> tuple:
    if not size:
        return (999, 0, "")

    size_lower = size.lower()
    letter_sizes = {
        "xs": 1, "s": 2, "m": 3, "l": 4, "xl": 5,
        "xxl": 6, "xxxl": 7, "2xl": 6, "3xl": 7,
        "4xl": 8, "5xl": 9,
    }

    for key, value in letter_sizes.items():
        if key in size_lower:
            return (1, value, size)

    if "-" in size:
        try:
            first_num = int(size.split("-")[0])
            return (2, first_num, size)
        except Exception:
            pass

    match = re.search(r"\d+", size)
    if match:
        try:
            num = int(match.group())
            return (2, num, size)
        except Exception:
            pass

    return (3, 0, size)


class SyncService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self._archived_catalog_cache: dict[int, dict[str, dict[str, Any]]] = {}

    @staticmethod
    def _cluster_refresh_guard_key(store_id: int) -> str:
        return f"ozon:cluster-refresh:{store_id}"

    @staticmethod
    def _cluster_ozon_attr_name() -> Optional[str]:
        for attr_name in ("ozon_id", "cluster_ozon_id", "external_id"):
            if hasattr(Cluster, attr_name):
                return attr_name
        return None

    @staticmethod
    def _safe_json_sample(data: Any, limit: int = 20) -> str:
        try:
            return json.dumps(data[:limit] if isinstance(data, list) else data, ensure_ascii=False, default=str)
        except Exception:
            return str(data)[:5000]

    async def _get_live_store(self, store_id: int) -> Store | None:
        store = await self.db.get(Store, store_id)
        if not store:
            logger.warning("⏹️ Store {} no longer exists, skipping sync task", store_id)
            return None
        if not store.is_active:
            logger.warning("⏹️ Store {} is inactive, skipping sync task", store_id)
            return None
        return store

    async def _get_or_create_warehouse_product(
        self,
        *,
        user_id: int,
        name: str,
        cache: Optional[Dict[str, int]] = None,
    ) -> int:
        normalized_name = (name or "").strip()
        if not normalized_name:
            raise ValueError("Warehouse product name must not be empty")

        cache_key = normalized_name.casefold()
        if cache is not None and cache_key in cache:
            return cache[cache_key]

        existing_result = await self.db.execute(
            select(WarehouseProduct)
            .where(
                WarehouseProduct.user_id == user_id,
                WarehouseProduct.name == normalized_name,
            )
            .order_by(WarehouseProduct.id)
        )
        existing = existing_result.scalars().first()
        if existing:
            if existing.is_archived:
                existing.is_archived = False
                await self.db.flush()
            if cache is not None:
                cache[cache_key] = existing.id
            return existing.id

        warehouse_product = WarehouseProduct(
            user_id=user_id,
            name=normalized_name,
            is_archived=False,
        )
        self.db.add(warehouse_product)
        await self.db.flush()
        if cache is not None:
            cache[cache_key] = warehouse_product.id
        return warehouse_product.id

    @staticmethod
    def _is_deadlock_error(error: Exception) -> bool:
        pending: list[BaseException] = [error]
        visited: set[int] = set()

        while pending:
            current = pending.pop()
            current_id = id(current)
            if current_id in visited:
                continue
            visited.add(current_id)

            if getattr(current, "sqlstate", None) == "40P01" or getattr(current, "pgcode", None) == "40P01":
                return True

            if "deadlock detected" in str(current).lower():
                return True

            for nested in (
                getattr(current, "orig", None),
                getattr(current, "__cause__", None),
                getattr(current, "__context__", None),
            ):
                if isinstance(nested, BaseException):
                    pending.append(nested)

        return False

    async def _refresh_clusters_before_stock_sync(
        self,
        client: OzonClient,
        *,
        store_id: int,
        store_name: str,
    ) -> bool:
        redis = None
        try:
            redis = await get_redis()
            if redis:
                ttl_seconds = max(int(settings.OZON_CLUSTER_REFRESH_TTL_SECONDS), 0)
                if ttl_seconds > 0:
                    acquired = await redis.set(
                        self._cluster_refresh_guard_key(store_id),
                        datetime.now().isoformat(),
                        ex=ttl_seconds,
                        nx=True,
                    )
                    if not acquired:
                        logger.info(
                            "🧊 Skipping cluster refresh before stock sync for store {} ({}): TTL guard is active",
                            store_id,
                            store_name,
                        )
                        return False
        except Exception as redis_error:
            logger.warning(
                "⚠️ Redis guard is unavailable before stock sync for store {} ({}): {}",
                store_id,
                store_name,
                redis_error,
            )
            redis = None

        max_attempts = 2

        for attempt in range(1, max_attempts + 1):
            try:
                await self._sync_cluster_payloads(await client.get_clusters("CLUSTER_TYPE_OZON"))
                await self._sync_cluster_payloads(await client.get_clusters("CLUSTER_TYPE_CIS"))
                await self.db.commit()
                return True
            except Exception as e:
                await self.db.rollback()
                if redis:
                    try:
                        await redis.delete(self._cluster_refresh_guard_key(store_id))
                    except Exception as redis_error:
                        logger.warning(
                            "⚠️ Failed to release cluster refresh guard for store {} ({}): {}",
                            store_id,
                            store_name,
                            redis_error,
                        )

                if self._is_deadlock_error(e) and attempt < max_attempts:
                    logger.warning(
                        "Deadlock while refreshing Ozon clusters before stock sync for store {} ({}), retrying {}/{}",
                        store_id,
                        store_name,
                        attempt + 1,
                        max_attempts,
                    )
                    await asyncio.sleep(0.5)
                    continue

                logger.warning(
                    "⚠️ Failed to refresh Ozon clusters before stock sync for store {} ({}): {}",
                    store_id,
                    store_name,
                    e,
                )
                return False

        return False

    def _normalize_supply_dates(
        self,
        status: str,
        timeslot_from: Optional[datetime],
        timeslot_to: Optional[datetime],
        completed_at: Optional[datetime],
    ) -> tuple[Optional[datetime], Optional[datetime], Optional[datetime]]:
        if (
            status == "COMPLETED"
            and completed_at is not None
            and timeslot_from is not None
            and completed_at < timeslot_from
        ):
            logger.warning(
                "Ignoring inconsistent future timeslot for completed supply: "
                f"completed_at={completed_at}, timeslot_from={timeslot_from}"
            )
            return None, None, completed_at

        return timeslot_from, timeslot_to, completed_at

    def _extract_supply_timeslot(self, order: dict) -> tuple[Optional[datetime], Optional[datetime]]:
        raw_timeslot = order.get("timeslot")
        if not isinstance(raw_timeslot, dict):
            return None, None

        timeslot_payload = raw_timeslot.get("timeslot") if isinstance(raw_timeslot.get("timeslot"), dict) else raw_timeslot
        return (
            parse_ozon_date(timeslot_payload.get("from")),
            parse_ozon_date(timeslot_payload.get("to")),
        )

    async def _find_ozon_warehouse(
        self,
        *,
        warehouse_ozon_id: Optional[str] = None,
        warehouse_name: Optional[str] = None,
    ) -> Optional[OzonWarehouse]:
        if warehouse_ozon_id:
            stmt = select(OzonWarehouse).where(OzonWarehouse.ozon_id == str(warehouse_ozon_id))
            result = await self.db.execute(stmt)
            warehouse = result.scalar_one_or_none()
            if warehouse:
                return warehouse

        if warehouse_name:
            stmt = select(OzonWarehouse).where(OzonWarehouse.name == warehouse_name)
            result = await self.db.execute(stmt)
            warehouse = result.scalar_one_or_none()
            if warehouse:
                return warehouse

        return None

    @staticmethod
    def _warehouse_lock_key_from_payload(warehouse_payload: Optional[dict]) -> Optional[str]:
        if not isinstance(warehouse_payload, dict):
            return None

        warehouse_ozon_id = warehouse_payload.get("warehouse_id") or warehouse_payload.get("id")
        warehouse_name = warehouse_payload.get("name")

        if warehouse_ozon_id:
            return str(warehouse_ozon_id)

        normalized_name = normalize_optional_string(warehouse_name)
        if normalized_name:
            return f"name:{normalized_name}"

        return None

    async def _acquire_warehouse_advisory_lock(self, warehouse_key: str) -> None:
        await self.db.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:warehouse_key))"),
            {"warehouse_key": f"ozon_warehouse:{warehouse_key}"},
        )

    async def _acquire_warehouse_advisory_locks(self, warehouse_keys: Iterable[str]) -> None:
        normalized_keys = sorted({str(key).strip() for key in warehouse_keys if str(key).strip()})
        for warehouse_key in normalized_keys:
            await self._acquire_warehouse_advisory_lock(warehouse_key)

    def _collect_supply_order_warehouse_lock_keys(self, order: Optional[dict]) -> list[str]:
        if not isinstance(order, dict):
            return []

        supplies = order.get("supply_order" , []) or []
        primary_supply = supplies[0] if supplies else {}
        return [
            key
            for key in (
                self._warehouse_lock_key_from_payload(order.get("drop_off_warehouse")),
                self._warehouse_lock_key_from_payload(primary_supply.get("storage_warehouse")),
            )
            if key
        ]

    async def _get_or_create_ozon_warehouse(self, warehouse_payload: Optional[dict], *, acquire_lock: bool = True) -> Optional[int]:
        if not isinstance(warehouse_payload, dict):
            return None

        warehouse_ozon_id = warehouse_payload.get("warehouse_id") or warehouse_payload.get("id")
        warehouse_name = warehouse_payload.get("name")

        if not warehouse_ozon_id and not warehouse_name:
            return None

        warehouse_key = self._warehouse_lock_key_from_payload(warehouse_payload)
        if acquire_lock and warehouse_key:
            await self._acquire_warehouse_advisory_lock(warehouse_key)

        warehouse = await self._find_ozon_warehouse(
            warehouse_ozon_id=str(warehouse_ozon_id) if warehouse_ozon_id else None,
            warehouse_name=warehouse_name,
        )

        if not warehouse:
            if not warehouse_ozon_id:
                return None
            warehouse = OzonWarehouse(
                ozon_id=str(warehouse_ozon_id),
                name=warehouse_name or f"Ozon warehouse {warehouse_ozon_id}",
            )
            self.db.add(warehouse)
            await self.db.flush()
            return warehouse.id

        changed = False
        desired_ozon_id = str(warehouse_ozon_id) if warehouse_ozon_id else None
        if desired_ozon_id and warehouse.ozon_id != desired_ozon_id:
            warehouse.ozon_id = desired_ozon_id
            changed = True
        if warehouse_name and warehouse.name != warehouse_name:
            warehouse.name = warehouse_name
            changed = True

        if changed:
            await self.db.flush()

        return warehouse.id

    async def _get_or_create_cluster(self, cluster_ozon_id: int, cluster_name: str) -> Cluster:
        cluster_attr = self._cluster_ozon_attr_name()
        cluster = None

        if cluster_attr:
            cluster_stmt = select(Cluster).where(getattr(Cluster, cluster_attr) == str(cluster_ozon_id))
            cluster_result = await self.db.execute(cluster_stmt)
            cluster = cluster_result.scalar_one_or_none()

        if not cluster:
            cluster_name_stmt = select(Cluster).where(Cluster.name == cluster_name)
            cluster_name_result = await self.db.execute(cluster_name_stmt)
            cluster = cluster_name_result.scalar_one_or_none()

        if cluster:
            if cluster.name != cluster_name:
                cluster.name = cluster_name
            if cluster_attr and getattr(cluster, cluster_attr, None) != str(cluster_ozon_id):
                setattr(cluster, cluster_attr, str(cluster_ozon_id))
            await self.db.flush()
            return cluster

        payload = {"name": cluster_name}
        if cluster_attr:
            payload[cluster_attr] = str(cluster_ozon_id)

        cluster = Cluster(**payload)
        self.db.add(cluster)
        await self.db.flush()
        return cluster

    async def _sync_cluster_payloads(self, clusters_payload: list[dict]):
        for cluster_payload in clusters_payload:
            cluster_ozon_id = cluster_payload.get("id")
            cluster_name = cluster_payload.get("name") or f"Cluster {cluster_ozon_id}"

            if cluster_ozon_id is None:
                continue

            cluster = await self._get_or_create_cluster(cluster_ozon_id, cluster_name)

            warehouses_payload: list[dict] = []
            direct_warehouses = cluster_payload.get("warehouses") or []
            if isinstance(direct_warehouses, list):
                warehouses_payload.extend(
                    warehouse_payload
                    for warehouse_payload in direct_warehouses
                    if isinstance(warehouse_payload, dict)
                )

            logistic_clusters = cluster_payload.get("logistic_clusters") or []
            if isinstance(logistic_clusters, list):
                for logistic_cluster in logistic_clusters:
                    if not isinstance(logistic_cluster, dict):
                        continue
                    nested_warehouses = logistic_cluster.get("warehouses") or []
                    if not isinstance(nested_warehouses, list):
                        continue
                    warehouses_payload.extend(
                        warehouse_payload
                        for warehouse_payload in nested_warehouses
                        if isinstance(warehouse_payload, dict)
                    )

            seen_warehouse_ids: set[str] = set()
            for warehouse_payload in warehouses_payload:
                warehouse_ozon_id = warehouse_payload.get("warehouse_id") or warehouse_payload.get("id")
                warehouse_name = warehouse_payload.get("name") or f"Warehouse {warehouse_ozon_id}"

                if warehouse_ozon_id is None:
                    continue

                warehouse_ozon_id_str = str(warehouse_ozon_id)
                if warehouse_ozon_id_str in seen_warehouse_ids:
                    continue
                seen_warehouse_ids.add(warehouse_ozon_id_str)

                warehouse = await self._find_ozon_warehouse(
                    warehouse_ozon_id=warehouse_ozon_id_str,
                    warehouse_name=warehouse_name,
                )

                if not warehouse:
                    warehouse = OzonWarehouse(
                        ozon_id=warehouse_ozon_id_str,
                        name=warehouse_name,
                        cluster_id=cluster.id,
                    )
                    self.db.add(warehouse)
                    try:
                        await self.db.flush()
                    except IntegrityError:
                        await self.db.rollback()
                        warehouse = await self._find_ozon_warehouse(
                            warehouse_ozon_id=warehouse_ozon_id_str,
                            warehouse_name=warehouse_name,
                        )
                        if not warehouse:
                            raise
                else:
                    warehouse.ozon_id = warehouse_ozon_id_str
                    warehouse.name = warehouse_name
                    warehouse.cluster_id = cluster.id
                    await self.db.flush()

    async def sync_clusters_for_store(self, store: Store):
        store = await self._get_live_store(store.id)
        if not store:
            return

        decrypted_key = decrypt_api_key(store.api_key_encrypted)
        client = OzonClient(store.client_id, decrypted_key, store.name)

        try:
            await self._sync_cluster_payloads(await client.get_clusters("CLUSTER_TYPE_OZON"))
            await self._sync_cluster_payloads(await client.get_clusters("CLUSTER_TYPE_CIS"))
            await self.db.commit()
            logger.info(f"✅ Clusters sync completed for store {store.id}")
        except Exception:
            await self.db.rollback()
            raise
        finally:
            await client.close()

    async def _fetch_products_list_page(
        self,
        client: OzonClient,
        *,
        last_id: Optional[str] = None,
        visibility: str = "ALL",
    ) -> dict[str, Any]:
        """
        Совместимый вызов /v3/product/list.
        Сначала пробуем публичный клиентский метод с visibility, если он уже поддержан.
        Если нет — откатываемся к прямому _post().
        """
        try:
            return await client.get_products_list(last_id=last_id, visibility=visibility)  # type: ignore[misc]
        except TypeError:
            payload: dict[str, Any] = {
                "filter": {"visibility": visibility},
                "limit": 1000,
            }
            if last_id:
                payload["last_id"] = last_id
            return await client._post("/v3/product/list", payload)  # noqa: SLF001
        except Exception:
            raise

    async def _load_products_payloads_by_visibility(
        self,
        client: OzonClient,
        *,
        visibility: str,
        store_id: int,
    ) -> list[dict[str, Any]]:
        last_id = None
        page = 1
        all_products_payloads: list[dict[str, Any]] = []

        while True:
            try:
                data = await self._fetch_products_list_page(client, last_id=last_id, visibility=visibility)
            except HTTPStatusError as e:
                if e.response.status_code == 429:
                    logger.warning(
                        "Rate limited while loading products visibility={} page {} for store {}, waiting 5 seconds...",
                        visibility,
                        page,
                        store_id,
                    )
                    await asyncio.sleep(5)
                    continue
                raise

            result = data.get("result", {})
            items = result.get("items", []) or []
            if not items:
                break

            all_products_payloads.extend(items)
            for item in all_products_payloads[-len(items):]:
                item["_ozon_visibility"] = visibility
            last_id = result.get("last_id")
            page += 1
            if not last_id:
                break

        return all_products_payloads

    async def _load_products_payloads_with_archived(
        self,
        client: OzonClient,
        *,
        store_id: int,
    ) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen_product_ids: set[int] = set()

        for visibility in ("ALL", "ARCHIVED"):
            payloads = await self._load_products_payloads_by_visibility(
                client,
                visibility=visibility,
                store_id=store_id,
            )
            for item in payloads:
                product_id = item.get("product_id")
                if not product_id or product_id in seen_product_ids:
                    continue
                seen_product_ids.add(product_id)
                merged.append(item)

        return merged

    async def _load_attributes_for_products(
        self,
        client: OzonClient,
        *,
        store_id: int,
        all_products_payloads: list[dict[str, Any]],
        debug_prefix: str = "PRODUCT ATTR FETCH DEBUG",
    ) -> list[dict[str, Any]]:
        all_attributes_payloads: list[dict[str, Any]] = []
        product_attribute_batch_size = 100

        for i in range(0, len(all_products_payloads), product_attribute_batch_size):
            batch_payloads = all_products_payloads[i:i + product_attribute_batch_size]
            product_ids_in_batch = [p["product_id"] for p in batch_payloads if p.get("product_id")]
            if not product_ids_in_batch:
                continue

            batch_attrs = await client.get_product_attributes(product_ids_in_batch)
            all_attributes_payloads.extend(batch_attrs)

            logger.warning(
                "🧪 {} store={} input_product_ids={} fetched_attr_rows={} sample_input_ids={} sample_attr_ids={}",
                debug_prefix,
                store_id,
                len(product_ids_in_batch),
                len(batch_attrs),
                product_ids_in_batch[:20],
                [row.get("id") for row in batch_attrs[:20]],
            )
            await asyncio.sleep(0.25)

        return all_attributes_payloads

    def _log_catalog_debug_summary(
        self,
        *,
        store_id: int,
        all_products_payloads: list[dict[str, Any]],
        all_attributes_payloads: list[dict[str, Any]],
        grouped_products: list[dict[str, Any]],
    ) -> None:
        product_ids_from_list = {p.get("product_id") for p in all_products_payloads if p.get("product_id")}
        product_ids_from_attrs = {p.get("id") for p in all_attributes_payloads if p.get("id")}
        missing_attr_rows = sorted(product_ids_from_list - product_ids_from_attrs)
        with_sku = sum(1 for p in all_attributes_payloads if normalize_optional_string(p.get("sku")))
        with_offer_id = sum(1 for p in all_attributes_payloads if normalize_optional_string(p.get("offer_id")))
        with_barcode = sum(1 for p in all_attributes_payloads if normalize_optional_string(p.get("barcode")))
        without_sku = len(all_attributes_payloads) - with_sku

        logger.warning(
            "🧪 PRODUCT SYNC DEBUG store={} raw_products={} attr_rows={} grouped_products={} "
            "unique_product_ids_from_list={} unique_attr_product_ids={} missing_attr_rows={} "
            "with_sku={} without_sku={} with_offer_id={} with_barcode={}",
            store_id,
            len(all_products_payloads),
            len(all_attributes_payloads),
            len(grouped_products),
            len(product_ids_from_list),
            len(product_ids_from_attrs),
            len(missing_attr_rows),
            with_sku,
            without_sku,
            with_offer_id,
            with_barcode,
        )

        if missing_attr_rows:
            logger.warning(
                "🧪 PRODUCT SYNC DEBUG store={} product_ids present in get_products_list but missing in get_product_attributes sample={}",
                store_id,
                missing_attr_rows[:100],
            )

        attrs_without_sku = [
            {
                "product_id": row.get("id"),
                "offer_id": row.get("offer_id"),
                "barcode": row.get("barcode"),
                "name": row.get("name"),
            }
            for row in all_attributes_payloads
            if not normalize_optional_string(row.get("sku"))
        ]
        if attrs_without_sku:
            logger.warning(
                "🧪 PRODUCT SYNC DEBUG store={} attributes without SKU sample={}",
                store_id,
                self._safe_json_sample(attrs_without_sku, limit=50),
            )

        attrs_sample = [
            {
                "product_id": row.get("id"),
                "sku": row.get("sku"),
                "offer_id": row.get("offer_id"),
                "barcode": row.get("barcode"),
                "name": row.get("name"),
                "attributes_count": len(row.get("attributes") or []),
            }
            for row in all_attributes_payloads[:50]
        ]
        logger.warning(
            "🧪 PRODUCT SYNC DEBUG store={} attributes sample={}",
            store_id,
            self._safe_json_sample(attrs_sample, limit=50),
        )

    async def sync_products_for_store(
        self,
        store: Store,
        product_link_map: Optional[Dict[str, Any]] = None,
        preloaded_attributes_payloads: Optional[List[Dict[str, Any]]] = None,
    ):
        store = await self._get_live_store(store.id)
        if not store:
            return

        decrypted_key = decrypt_api_key(store.api_key_encrypted)
        client = OzonClient(store.client_id, decrypted_key, store.name)
        warehouse_product_cache: Dict[str, int] = {}
        normalized_product_link_map = normalize_product_link_map(product_link_map)

        logger.info(f"🔄 Starting products sync for store {store.id} ({store.name})")

        try:
            existing_variants_query = select(
                Variant.id,
                Variant.sku,
                Variant.offer_id,
                Variant.barcode,
                Variant.product_id,
                Product.base_name,
                Product.warehouse_product_id,
            ).join(
                Product, Product.id == Variant.product_id
            ).where(Product.store_id == store.id)
            existing_variants_rows = (await self.db.execute(existing_variants_query)).all()

            sku_to_variant_id = {
                sku: int(variant_id)
                for variant_id, raw_sku, _raw_offer_id, _raw_barcode, _product_id, _base_name, _warehouse_product_id in existing_variants_rows
                if (sku := normalize_optional_string(raw_sku))
            }
            offer_id_to_variant_id = build_unique_string_index([
                (raw_offer_id, variant_id)
                for variant_id, _raw_sku, raw_offer_id, _raw_barcode, _product_id, _base_name, _warehouse_product_id in existing_variants_rows
            ])
            barcode_to_variant_id = build_unique_string_index([
                (raw_barcode, variant_id)
                for variant_id, _raw_sku, _raw_offer_id, raw_barcode, _product_id, _base_name, _warehouse_product_id in existing_variants_rows
            ])
            variant_context_by_id = {
                int(variant_id): {
                    "product_id": int(product_id),
                    "base_name": normalize_optional_string(base_name),
                    "warehouse_product_id": warehouse_product_id,
                }
                for variant_id, _raw_sku, _raw_offer_id, _raw_barcode, product_id, base_name, warehouse_product_id in existing_variants_rows
            }
            logger.info(
                "Loaded {} existing variants into memory (sku={}, unique offer_id={}, unique barcode={})",
                len(existing_variants_rows),
                len(sku_to_variant_id),
                len(offer_id_to_variant_id),
                len(barcode_to_variant_id),
            )

            if preloaded_attributes_payloads is not None:
                all_attributes_payloads = preloaded_attributes_payloads
                all_products_payloads = [{"product_id": row.get("id")} for row in all_attributes_payloads if row.get("id")]
                logger.info(
                    "♻️ Reusing onboarding catalog snapshot for store {} ({} attribute rows)",
                    store.id,
                    len(all_attributes_payloads),
                )
            else:
                all_products_payloads = await self._load_products_payloads_with_archived(
                    client,
                    store_id=store.id,
                )
                logger.info(f"✅ Total products loaded from API: {len(all_products_payloads)}")

                if not all_products_payloads:
                    logger.warning(f"⚠️ No products returned from Ozon API for store {store.id}")
                    return

                logger.info("🧠 Loading full attribute snapshot for grouping...")
                all_attributes_payloads = await self._load_attributes_for_products(
                    client,
                    store_id=store.id,
                    all_products_payloads=all_products_payloads,
                )

            if not all_attributes_payloads:
                logger.warning(f"⚠️ No product attributes available for store {store.id}")
                return

            attrs_by_product_id = {attr["id"]: attr for attr in all_attributes_payloads if attr.get("id")}
            product_visibility_by_product_id = {
                int(payload["product_id"]): str(payload.get("_ozon_visibility") or "ALL")
                for payload in all_products_payloads
                if payload.get("product_id")
            }
            if preloaded_attributes_payloads is not None:
                for payload in all_attributes_payloads:
                    product_id = payload.get("id")
                    if product_id and payload.get("_ozon_visibility"):
                        product_visibility_by_product_id[int(product_id)] = str(payload.get("_ozon_visibility"))
            grouped_products = build_ozon_product_groups(all_attributes_payloads)
            self._log_catalog_debug_summary(
                store_id=store.id,
                all_products_payloads=all_products_payloads,
                all_attributes_payloads=all_attributes_payloads,
                grouped_products=grouped_products,
            )

            group_plan_by_product_id: dict[int, dict[str, Any]] = {}
            for group in grouped_products:
                base_name = group["base_name"]
                group_key = serialize_group_key(group.get("key"))
                group_product_name = group["product_name"]
                for variant in group["variants"]:
                    product_id = variant.get("product_id")
                    if not product_id:
                        continue
                    link_plan = select_product_link_plan(
                        normalized_product_link_map,
                        group_key=group_key,
                        base_name=base_name,
                        offer_id=normalize_optional_string(variant.get("offer_id")),
                    )
                    group_plan_by_product_id[product_id] = {
                        "group_key": group_key,
                        "base_name": base_name,
                        "product_name": group_product_name,
                        "image_url": group.get("image_url"),
                        "link_plan": link_plan,
                    }

            logger.info(
                f"🧩 Built {len(grouped_products)} catalog groups for store {store.id} before DB sync"
            )

            batch_size = 100
            all_product_ids = [attr_id for attr_id in attrs_by_product_id.keys()]
            total_batches = (len(all_product_ids) + batch_size - 1) // batch_size
            created_variant_debug: list[dict[str, Any]] = []
            updated_variant_debug: list[dict[str, Any]] = []
            skipped_variant_debug: list[dict[str, Any]] = []

            for i in range(0, len(all_product_ids), batch_size):
                batch_product_ids = all_product_ids[i:i + batch_size]
                batch_num = i // batch_size + 1
                logger.info(f"🔍 Processing batch {batch_num}/{total_batches} ({len(batch_product_ids)} products)")

                if not batch_product_ids:
                    logger.warning(f"   ⚠️ No product IDs in batch {batch_num}")
                    continue

                try:
                    products_to_update_data = []
                    variants_to_update_data = []
                    all_variant_attributes_to_insert = []

                    for product_id_ozon in batch_product_ids:
                        attrs_payload = attrs_by_product_id.get(product_id_ozon)
                        if not attrs_payload:
                            logger.warning(f"   Skipping product {product_id_ozon}: no attributes found")
                            continue

                        group_plan = group_plan_by_product_id.get(product_id_ozon, {})
                        group_key = group_plan.get("group_key")
                        product_name = group_plan.get("product_name") or attrs_payload.get("name", "Unknown")
                        base_name = group_plan.get("base_name") or extract_base_product_name(product_name)
                        image_url = group_plan.get("image_url")
                        if image_url is None:
                            images = attrs_payload.get("images", [])
                            image_url = images[0] if images else None

                        sku = normalize_optional_string(attrs_payload.get("sku"))
                        offer_id = normalize_optional_string(attrs_payload.get("offer_id")) or ""
                        barcode = normalize_optional_string(attrs_payload.get("barcode"))
                        is_archived = product_visibility_by_product_id.get(int(product_id_ozon), "ALL") == "ARCHIVED"

                        if not sku:
                            skipped_variant_debug.append({
                                "product_id_ozon": product_id_ozon,
                                "product_id": (group_plan or {}).get("product_id"),
                                "name": product_name,
                                "offer_id": offer_id,
                                "barcode": barcode,
                                "reason": "empty sku",
                            })
                            logger.warning(
                                "   ⚠️ Skipping variant sync for product {} in store {}: Ozon returned empty SKU (offer_id='{}', barcode='{}')",
                                product_id_ozon,
                                store.id,
                                offer_id or "—",
                                barcode or "—",
                            )
                            continue

                        pack_size = 1
                        for attr in attrs_payload.get("attributes", []):
                            if attr.get("id") == ATTR_ID_PACK_SIZE:
                                values = attr.get("values", [])
                                if values:
                                    attr_value = str(values[0].get("value", ""))
                                    pack_size = extract_pack_size_from_attribute(attr_value)
                                    break
                        if pack_size == 1 and offer_id:
                            pack_size = extract_pack_size_from_text(offer_id)
                        if pack_size == 1:
                            pack_size = extract_pack_size_from_text(product_name)

                        variant_id = sku_to_variant_id.get(sku)
                        matched_by = "sku" if variant_id else None

                        if not variant_id and offer_id:
                            variant_id = offer_id_to_variant_id.get(offer_id)
                            if variant_id:
                                matched_by = "offer_id"
                                logger.info(
                                    "   🔁 Matched existing variant by offer_id for store {}: offer_id='{}' -> SKU {}",
                                    store.id,
                                    offer_id,
                                    sku,
                                )

                        if not variant_id and barcode:
                            variant_id = barcode_to_variant_id.get(barcode)
                            if variant_id:
                                matched_by = "barcode"
                                logger.info(
                                    "   🔁 Matched existing variant by barcode for store {}: barcode='{}' -> SKU {}",
                                    store.id,
                                    barcode,
                                    sku,
                                )

                        existing_variant_context = variant_context_by_id.get(variant_id) if variant_id else None

                        link_plan = group_plan.get("link_plan") or select_product_link_plan(
                            normalized_product_link_map,
                            group_key=group_key or "",
                            base_name=base_name,
                            offer_id=offer_id,
                        )
                        has_explicit_link_target = bool(
                            link_plan.get("warehouse_product_id")
                            or normalize_optional_string(link_plan.get("warehouse_product_name"))
                        )
                        resolved_base_name = (
                            normalize_optional_string(link_plan.get("warehouse_product_name"))
                            or (existing_variant_context or {}).get("base_name")
                            or base_name
                        )
                        resolved_warehouse_product_id = (
                            link_plan.get("warehouse_product_id")
                            or (existing_variant_context or {}).get("warehouse_product_id")
                        )

                        existing_product_id = (existing_variant_context or {}).get("product_id")
                        existing_product_base_name = (existing_variant_context or {}).get("base_name")

                        if existing_product_id and existing_product_base_name == resolved_base_name:
                            product_id = existing_product_id
                            product_row = (existing_product_id, resolved_warehouse_product_id)
                        else:
                            stmt = select(Product.id, Product.warehouse_product_id).where(
                                Product.store_id == store.id,
                                Product.base_name == resolved_base_name,
                            )
                            existing_product = await self.db.execute(stmt)
                            product_row = existing_product.first()

                        if product_row:
                            product_id = product_row[0]
                            existing_warehouse_product_id = product_row[1]
                            target_warehouse_product_id = resolved_warehouse_product_id or existing_warehouse_product_id

                            if target_warehouse_product_id:
                                if existing_warehouse_product_id != target_warehouse_product_id:
                                    await self.db.execute(
                                        update(Product)
                                        .where(Product.id == product_id)
                                        .values(warehouse_product_id=target_warehouse_product_id)
                                    )
                            elif not existing_warehouse_product_id and has_explicit_link_target:
                                warehouse_product_id = await self._get_or_create_warehouse_product(
                                    user_id=store.user_id,
                                    name=normalize_optional_string(link_plan.get("warehouse_product_name")) or resolved_base_name,
                                    cache=warehouse_product_cache,
                                )
                                await self.db.execute(
                                    update(Product)
                                    .where(Product.id == product_id)
                                    .values(warehouse_product_id=warehouse_product_id)
                                )
                            products_to_update_data.append({
                                "id": product_id,
                                "name": product_name,
                                "base_name": resolved_base_name,
                                "image_url": image_url,
                            })
                        else:
                            target_warehouse_product_id = resolved_warehouse_product_id
                            if not target_warehouse_product_id and has_explicit_link_target:
                                target_warehouse_product_id = await self._get_or_create_warehouse_product(
                                    user_id=store.user_id,
                                    name=normalize_optional_string(link_plan.get("warehouse_product_name")) or resolved_base_name,
                                    cache=warehouse_product_cache,
                                )

                            new_product = Product(
                                store_id=store.id,
                                warehouse_product_id=target_warehouse_product_id,
                                name=product_name,
                                base_name=resolved_base_name,
                                image_url=image_url,
                            )
                            self.db.add(new_product)
                            await self.db.flush()
                            product_id = new_product.id
                            logger.info(f"   ✨ Created new product: {product_name} (ID: {product_id})")
                            target_warehouse_product_id = new_product.warehouse_product_id

                        final_warehouse_product_id = target_warehouse_product_id
                        variant_debug = {
                            "product_id_ozon": product_id_ozon,
                            "product_id": product_id,
                            "name": product_name,
                            "sku": sku,
                            "offer_id": offer_id,
                            "barcode": barcode,
                            "pack_size": pack_size,
                        }

                        if variant_id:
                            variants_to_update_data.append({
                                "id": variant_id,
                                "product_id": product_id,
                                "sku": sku,
                                "offer_id": offer_id,
                                "barcode": barcode,
                                "pack_size": pack_size,
                                "is_archived": is_archived,
                            })
                            sku_to_variant_id[sku] = variant_id
                            if offer_id:
                                offer_id_to_variant_id[offer_id] = variant_id
                            if barcode:
                                barcode_to_variant_id[barcode] = variant_id
                            variant_context_by_id[variant_id] = {
                                "product_id": product_id,
                                "base_name": resolved_base_name,
                                "warehouse_product_id": final_warehouse_product_id,
                            }
                            updated_variant_debug.append({
                                **variant_debug,
                                "variant_id": variant_id,
                                "matched_by": matched_by or "sku",
                            })
                        else:
                            new_variant = Variant(
                                product_id=product_id,
                                sku=sku,
                                offer_id=offer_id,
                                barcode=barcode,
                                pack_size=pack_size,
                                is_archived=is_archived,
                            )
                            self.db.add(new_variant)
                            await self.db.flush()
                            variant_id = new_variant.id
                            sku_to_variant_id[sku] = variant_id
                            if offer_id:
                                offer_id_to_variant_id[offer_id] = variant_id
                            if barcode:
                                barcode_to_variant_id[barcode] = variant_id
                            variant_context_by_id[variant_id] = {
                                "product_id": product_id,
                                "base_name": resolved_base_name,
                                "warehouse_product_id": final_warehouse_product_id,
                            }
                            created_variant_debug.append({
                                **variant_debug,
                                "variant_id": variant_id,
                            })
                            logger.info(f"   ✨ Created new variant: SKU {sku} (pack_size: {pack_size})")

                        color_value = None
                        size_value = None
                        gender_value = None
                        pack_value = None

                        for attr in attrs_payload.get("attributes", []):
                            attr_id = attr.get("id")
                            if attr_id in IGNORED_ATTR_IDS:
                                continue
                            values = attr.get("values", [])
                            if not values:
                                continue
                            attr_value = str(values[0].get("value", "")).strip()
                            if not is_valid_attribute_value(attr_value):
                                continue
                            if attr_id == ATTR_ID_COLOR and not color_value:
                                color_value = attr_value
                            elif attr_id == ATTR_ID_COLOR_ALT and not color_value:
                                color_value = attr_value
                            elif attr_id == ATTR_ID_SIZE and not size_value:
                                size_value = attr_value
                            elif attr_id == ATTR_ID_GENDER and not gender_value:
                                gender_value = attr_value
                            elif attr_id == ATTR_ID_PACK_SIZE and not pack_value:
                                pack_value = attr_value

                        if color_value:
                            all_variant_attributes_to_insert.append({"variant_id": variant_id, "name": "Цвет", "value": color_value})
                        if size_value:
                            all_variant_attributes_to_insert.append({"variant_id": variant_id, "name": "Размер", "value": size_value})
                        if gender_value:
                            all_variant_attributes_to_insert.append({"variant_id": variant_id, "name": "Пол", "value": gender_value})
                        if pack_value:
                            all_variant_attributes_to_insert.append({"variant_id": variant_id, "name": "Упаковка", "value": pack_value})

                    if products_to_update_data:
                        await self.db.execute(update(Product), products_to_update_data)
                        logger.info(f"   Updated {len(products_to_update_data)} products")

                    if variants_to_update_data:
                        await self.db.execute(update(Variant), variants_to_update_data)
                        logger.info(f"   Updated {len(variants_to_update_data)} variants")

                    if all_variant_attributes_to_insert:
                        variant_ids_for_attr = {attr["variant_id"] for attr in all_variant_attributes_to_insert}
                        await self.db.execute(
                            delete(VariantAttribute).where(
                                VariantAttribute.variant_id.in_(variant_ids_for_attr)
                            )
                        )
                        await self.db.execute(
                            insert(VariantAttribute).values(all_variant_attributes_to_insert)
                        )
                        logger.info(f"   Replaced attributes for {len(variant_ids_for_attr)} variants")

                    await self.db.commit()
                    logger.info(f"   ✅ Batch {batch_num}/{total_batches} committed")

                except Exception as e:
                    logger.error(f"   ❌ Error processing batch {batch_num}: {e}")
                    await self.db.rollback()
                    continue

                await asyncio.sleep(0.5)

            logger.warning(
                "🧪 PRODUCT SYNC RESULT store={} created_variants={} updated_variants={} skipped_variants={}",
                store.id,
                len(created_variant_debug),
                len(updated_variant_debug),
                len(skipped_variant_debug),
            )
            if created_variant_debug:
                logger.warning(
                    "🧪 PRODUCT SYNC RESULT store={} created_variant_sample={}",
                    store.id,
                    self._safe_json_sample(created_variant_debug, limit=30),
                )
            if updated_variant_debug:
                logger.warning(
                    "🧪 PRODUCT SYNC RESULT store={} updated_variant_sample={}",
                    store.id,
                    self._safe_json_sample(updated_variant_debug, limit=30),
                )
            if skipped_variant_debug:
                logger.warning(
                    "🧪 PRODUCT SYNC RESULT store={} skipped_variant_sample={}",
                    store.id,
                    self._safe_json_sample(skipped_variant_debug, limit=30),
                )

        except Exception as e:
            logger.error(f"❌ Fatal error in products sync: {e}")
            raise
        finally:
            await client.close()

        logger.info(f"✅ Products sync completed for store {store.id}")

    @staticmethod
    def _extract_orders_from_batch_response(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        candidates: list[Any] = []
        if isinstance(payload, dict):
            result = payload.get("result")
            if isinstance(result, list):
                candidates.extend(result)
            elif isinstance(result, dict):
                for key in ("orders", "supply_orders", "items"):
                    value = result.get(key)
                    if isinstance(value, list):
                        candidates.extend(value)

            for key in ("orders", "supply_orders", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    candidates.extend(value)

        return [item for item in candidates if isinstance(item, dict)]

    @staticmethod
    def _extract_bundle_rows_from_batch_response(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        candidates: list[Any] = []
        if isinstance(payload, dict):
            result = payload.get("result")
            if isinstance(result, list):
                candidates.extend(result)
            elif isinstance(result, dict):
                for key in ("bundles", "items", "results"):
                    value = result.get(key)
                    if isinstance(value, list):
                        candidates.extend(value)

            for key in ("bundles", "items", "results"):
                value = payload.get(key)
                if isinstance(value, list):
                    candidates.extend(value)

        return [item for item in candidates if isinstance(item, dict)]

    @staticmethod
    def _extract_products_from_bundle_row(bundle_row: dict[str, Any]) -> list[dict[str, Any]]:
        for key in ("items", "products", "bundle_items", "result"):
            value = bundle_row.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

    async def _get_supply_order_details_batch_with_retry(
        self,
        client: OzonClient,
        order_ids: list[int],
        max_retries: int = 3,
    ) -> dict[str, dict[str, Any]]:
        normalized_order_ids = [int(order_id) for order_id in order_ids if str(order_id).isdigit()]
        if not normalized_order_ids:
            return {}

        async def _load_single_chunk(chunk_order_ids: list[int]) -> dict[str, dict[str, Any]]:
            if hasattr(client, "get_supply_orders_details"):
                for attempt in range(max_retries):
                    try:
                        response = await client.get_supply_orders_details(chunk_order_ids)
                        rows = self._extract_orders_from_batch_response(response)
                        return {str(row.get("order_id")): row for row in rows if row.get("order_id") is not None}
                    except HTTPStatusError as e:
                        if e.response.status_code == 429 and attempt < max_retries - 1:
                            wait_time = (attempt + 1) * 2
                            logger.warning(
                                "Rate limited on supply details chunk {}, waiting {}s (attempt {}/{})",
                                chunk_order_ids[:5],
                                wait_time,
                                attempt + 1,
                                max_retries,
                            )
                            await asyncio.sleep(wait_time)
                            continue
                        raise
                    except Exception as e:
                        if attempt < max_retries - 1:
                            wait_time = (attempt + 1) * 2
                            logger.warning("Batch order details chunk error, retrying in {}s: {}", wait_time, e)
                            await asyncio.sleep(wait_time)
                            continue
                        raise

            if not hasattr(client, "_post"):
                details: dict[str, dict[str, Any]] = {}
                for order_id in chunk_order_ids:
                    detail = await self._get_supply_order_detail_with_retry(client, order_id, max_retries=max_retries)
                    if detail and detail.get("order_id") is not None:
                        details[str(detail.get("order_id"))] = detail
                return details

            for attempt in range(max_retries):
                try:
                    response = await client._post("/v3/supply-order/get", {"order_ids": chunk_order_ids})
                    rows = self._extract_orders_from_batch_response(response)
                    return {str(row.get("order_id")): row for row in rows if row.get("order_id") is not None}
                except HTTPStatusError as e:
                    if e.response.status_code == 429 and attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2
                        logger.warning(
                            "Rate limited on supply details chunk {}, waiting {}s (attempt {}/{})",
                            chunk_order_ids[:5],
                            wait_time,
                            attempt + 1,
                            max_retries,
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                except Exception as e:
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2
                        logger.warning("Batch order details chunk error, retrying in {}s: {}", wait_time, e)
                        await asyncio.sleep(wait_time)
                        continue
                    raise

        details: dict[str, dict[str, Any]] = {}
        for chunk_order_ids in chunked(normalized_order_ids, SUPPLY_ORDER_DETAILS_BATCH_SIZE):
            chunk_result = await _load_single_chunk(chunk_order_ids)
            details.update(chunk_result)

        return details

    async def _get_bundle_products_batch_with_retry(
        self,
        client: OzonClient,
        bundle_ids: list[str],
        max_retries: int = 3,
    ) -> dict[str, list[dict[str, Any]]]:
        normalized_bundle_ids = [str(bundle_id) for bundle_id in bundle_ids if bundle_id]
        if not normalized_bundle_ids:
            return {}

        if not hasattr(client, "_post"):
            result: dict[str, list[dict[str, Any]]] = {}
            for bundle_id in normalized_bundle_ids:
                result[bundle_id] = await client.get_supply_bundle_products(bundle_id)
            return result

        for attempt in range(max_retries):
            try:
                response = await client._post(
                    "/v1/supply-order/bundle",
                    {"bundle_ids": normalized_bundle_ids, "limit": SUPPLY_BUNDLE_BATCH_SIZE},
                )
                rows = self._extract_bundle_rows_from_batch_response(response)
                by_bundle_id: dict[str, list[dict[str, Any]]] = {}
                for row in rows:
                    bundle_id = str(
                        row.get("bundle_id")
                        or row.get("id")
                        or row.get("bundleId")
                        or ""
                    ).strip()
                    if not bundle_id:
                        continue
                    by_bundle_id[bundle_id] = self._extract_products_from_bundle_row(row)
                return by_bundle_id
            except HTTPStatusError as e:
                if e.response.status_code in {429, 500, 502, 503, 504} and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * (2 if e.response.status_code == 429 else 3)
                    logger.warning(
                        "Retryable bundle batch error {} on {}, waiting {}s (attempt {}/{})",
                        e.response.status_code,
                        normalized_bundle_ids[:5],
                        wait_time,
                        attempt + 1,
                        max_retries,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                raise
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.warning("Batch bundle error, retrying in {}s: {}", wait_time, e)
                    await asyncio.sleep(wait_time)
                    continue
                raise

    async def _prefetch_bundle_products_for_orders(
        self,
        client: OzonClient,
        orders: list[dict[str, Any]],
    ) -> dict[str, list[dict[str, Any]]]:
        bundle_ids: list[str] = []
        seen: set[str] = set()
        for order in orders:
            supplies_data = order.get("supplies", [])
            if not isinstance(supplies_data, list):
                continue
            for sup in supplies_data:
                if not isinstance(sup, dict):
                    continue
                bundle_id = str(sup.get("bundle_id") or "").strip()
                if not bundle_id or bundle_id in seen:
                    continue
                seen.add(bundle_id)
                bundle_ids.append(bundle_id)

        if not bundle_ids:
            return {}

        bundle_products_by_id: dict[str, list[dict[str, Any]]] = {}
        for batch_bundle_ids in chunked(bundle_ids, SUPPLY_BUNDLE_BATCH_SIZE):
            batch_result = await self._get_bundle_products_batch_with_retry(client, batch_bundle_ids)
            bundle_products_by_id.update(batch_result)

        logger.info(
            "📦 Prefetched bundle products for store {}: bundles={} resolved={} missing={}",
            client.store_name if hasattr(client, "store_name") else "?",
            len(bundle_ids),
            len(bundle_products_by_id),
            max(len(bundle_ids) - len(bundle_products_by_id), 0),
        )
        return bundle_products_by_id

    async def _get_supply_order_detail_with_retry(self, client, order_id: int, max_retries: int = 3):
        for attempt in range(max_retries):
            try:
                return await client.get_supply_order_detail(order_id)
            except HTTPStatusError as e:
                if e.response.status_code == 429 and attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.warning(f"Rate limited, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                else:
                    raise
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    logger.warning(f"Error, retrying in {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                else:
                    raise

    async def sync_supplies_for_store(self, store: Store, months_back: int = 2):
        store = await self._get_live_store(store.id)
        if not store:
            return

        decrypted_key = decrypt_api_key(store.api_key_encrypted)
        client = OzonClient(store.client_id, decrypted_key, store.name)

        logger.info(f"🔄 Starting supplies sync for store {store.id} ({store.name})")
        logger.info(f"📅 Will fetch COMPLETED supplies from last {months_back} months")

        successful = 0
        failed = 0
        skipped = 0
        refreshed_existing = 0
        queued_event_ids: set[int] = set()
        first_sync_completed = await _is_supplies_first_sync_completed(store.id)
        allow_created_notifications = first_sync_completed

        if allow_created_notifications:
            logger.info(f"🔔 Supply created notifications enabled for store {store.id}")
        else:
            logger.info(
                f"🔕 First supplies sync for store {store.id}: historical supply_created notifications are muted"
            )

        try:
            last_id = None
            all_orders = []
            page = 1

            cutoff_date = datetime.now() - timedelta(days=30 * months_back)
            if cutoff_date.tzinfo is not None:
                cutoff_date = cutoff_date.replace(tzinfo=None)
            logger.info(f"📅 Cutoff date for COMPLETED supplies: {cutoff_date.strftime('%Y-%m-%d')}")

            refreshed_order_ids, refreshed_existing, refreshed_event_ids = await self._refresh_tracked_supplies_for_store(store.id, client)
            queued_event_ids.update(refreshed_event_ids)

            while True:
                logger.info(f"📦 Loading supplies page {page}...")
                try:
                    data = await client.get_supply_orders(last_id=last_id)
                except HTTPStatusError as e:
                    if e.response.status_code == 429:
                        logger.warning(f"Rate limited on page {page}, waiting 5 seconds...")
                        await asyncio.sleep(5)
                        continue
                    raise

                order_ids = data.get("order_ids", [])
                if not order_ids:
                    break

                logger.info(f"   Found {len(order_ids)} orders")

                try:
                    detail_map = await self._get_supply_order_details_batch_with_retry(
                        client,
                        [int(oid) for oid in order_ids if str(oid).isdigit()],
                    )
                except Exception as e:
                    logger.error(f"   ❌ Error loading order batch on page {page}: {e}")
                    failed += len(order_ids)
                    detail_map = {}

                for oid in order_ids:
                    detail = detail_map.get(str(oid))
                    if not detail:
                        logger.warning(f"   ⚠️ Detail for order {oid} was not returned in batch response")
                        failed += 1
                        continue

                    status = detail.get("state", "")
                    created_date = parse_ozon_date(detail.get("created_date"))

                    if status == "COMPLETED" and created_date and created_date < cutoff_date:
                        logger.debug(f"   ⏭️ Skipping old COMPLETED supply {oid} from {created_date}")
                        skipped += 1
                        continue

                    all_orders.append(detail)

                last_id = data.get("last_id")
                page += 1
                if not last_id:
                    break

            logger.info(f"✅ Total supplies loaded from API: {len(all_orders)} (skipped {skipped} old COMPLETED)")

            supply_order_lock_keys = sorted({
                key
                for order in all_orders
                for key in self._collect_supply_order_warehouse_lock_keys(order)
            })
            if supply_order_lock_keys:
                logger.info(
                    f"🔒 Pre-acquiring {len(supply_order_lock_keys)} warehouse advisory locks "
                    f"for paginated supplies sync in deterministic order"
                )
                await self._acquire_warehouse_advisory_locks(supply_order_lock_keys)

            bundle_products_by_bundle_id = await self._prefetch_bundle_products_for_orders(client, all_orders)

            for idx, order in enumerate(all_orders, 1):
                if str(order.get("order_id")) in refreshed_order_ids:
                    logger.debug(f"   ⏭️ Supply {order.get('order_id')} already refreshed directly, skipping duplicate page item")
                    continue

                logger.info(f"🔍 Processing supply {idx}/{len(all_orders)}")

                try:
                    event_ids = await self._process_supply_order(
                        store.id,
                        order,
                        client,
                        allow_created_notifications=allow_created_notifications,
                        bundle_products_by_bundle_id=bundle_products_by_bundle_id,
                    )
                    await self.db.commit()
                    queued_event_ids.update(event_ids)
                    successful += 1
                    logger.info(f"   ✅ Processed supply {idx}")
                except Exception as e:
                    await self.db.rollback()
                    logger.error(f"   ❌ Error processing supply {idx}: {e}")
                    failed += 1

            if successful > 0 or refreshed_existing > 0:
                logger.info(
                    f"✅ Supplies persisted to database "
                    f"(page_processed={successful}, tracked_refreshed={refreshed_existing})"
                )
                if queued_event_ids:
                    sent_count, failed_count = await deliver_pending_supply_notification_events(
                        self.db,
                        event_ids=queued_event_ids,
                    )
                    logger.info(
                        f"📨 Supply notification delivery after sync: "
                        f"sent={sent_count}, failed={failed_count}"
                    )
            else:
                logger.warning("⚠️ No supplies were successfully processed")
                await self.db.rollback()

            if not first_sync_completed and failed == 0:
                await _mark_supplies_first_sync_completed(store.id)
                logger.info(f"✅ Marked first supplies sync as completed for store {store.id}")

            logger.info(
                f"📊 Supplies sync summary: {successful} from pages, "
                f"{refreshed_existing} tracked refreshed, {failed} failed, {skipped} skipped"
            )

        except Exception as e:
            logger.error(f"❌ Fatal error in supplies sync: {e}")
            await self.db.rollback()
            raise
        finally:
            await client.close()

        logger.info("✅ Supplies sync completed")

    async def _refresh_tracked_supplies_for_store(self, store_id: int, client: OzonClient) -> tuple[Set[str], int, set[int]]:
        result = await self.db.execute(
            select(Supply.ozon_order_id)
            .where(
                Supply.store_id == store_id,
                Supply.status.notin_(FINAL_SUPPLY_STATUSES),
            )
            .order_by(Supply.created_at.asc())
        )
        tracked_order_ids = [order_id for order_id in result.scalars().all() if order_id]
        refreshed_order_ids: Set[str] = set()
        queued_event_ids: set[int] = set()

        if not tracked_order_ids:
            return refreshed_order_ids, 0, queued_event_ids

        logger.info(
            f"🔁 Refreshing {len(tracked_order_ids)} tracked active supplies directly "
            f"before paginated sync"
        )

        refreshed_count = 0
        bundle_products_by_bundle_id: dict[str, list[dict[str, Any]]] = {}
        tracked_details: list[tuple[str, dict[str, Any]]] = []

        for raw_order_id in tracked_order_ids:
            if not str(raw_order_id).isdigit():
                logger.warning(f"   ⚠️ Skipping tracked supply with non-numeric order_id={raw_order_id}")
                continue

            try:
                detail = await self._get_supply_order_detail_with_retry(client, int(raw_order_id))
                if not detail:
                    logger.warning(f"   ⚠️ Empty detail for tracked supply {raw_order_id}")
                    continue
                tracked_details.append((str(raw_order_id), detail))
            except Exception as e:
                if isinstance(e, httpx.RequestError):
                    logger.error(
                        "   ❌ Network failure while refreshing tracked supplies for store {}. "
                        "Stopping bulk refresh early so task retry can recover cleanly: {}",
                        store_id,
                        e,
                    )
                    raise
                logger.error(f"   ❌ Failed to load tracked supply {raw_order_id}: {e}")

        tracked_lock_keys = sorted({
            key
            for _, detail in tracked_details
            for key in self._collect_supply_order_warehouse_lock_keys(detail)
        })
        if tracked_lock_keys:
            logger.info(
                f"🔒 Pre-acquiring {len(tracked_lock_keys)} warehouse advisory locks "
                f"for tracked supplies refresh in deterministic order"
            )
            await self._acquire_warehouse_advisory_locks(tracked_lock_keys)

        for raw_order_id, detail in tracked_details:
            try:
                event_ids = await self._process_supply_order(store_id, detail, client, bundle_products_by_bundle_id=bundle_products_by_bundle_id)
                await self.db.commit()
                queued_event_ids.update(event_ids)
                refreshed_order_ids.add(str(detail.get("order_id") or raw_order_id))
                refreshed_count += 1
            except Exception as e:
                await self.db.rollback()
                if isinstance(e, httpx.RequestError):
                    logger.error(
                        "   ❌ Network failure while refreshing tracked supplies for store {}. "
                        "Stopping bulk refresh early so task retry can recover cleanly: {}",
                        store_id,
                        e,
                    )
                    raise
                logger.error(f"   ❌ Failed to refresh tracked supply {raw_order_id}: {e}")

        return refreshed_order_ids, refreshed_count, queued_event_ids

    async def _process_supply_order(
        self,
        store_id: int,
        order: dict,
        client: OzonClient,
        *,
        allow_created_notifications: bool = True,
        bundle_products_by_bundle_id: Optional[dict[str, list[dict[str, Any]]]] = None,
    ) -> list[int]:
        try:
            queued_event_ids: list[int] = []
            ozon_order_id = str(order.get("order_id"))
            order_number = order.get("order_number", "")
            status = order.get("state", "")
            store = await self.db.get(Store, store_id)
            store_name = store.name if store else f"Store {store_id}"
            user_email = None
            if store:
                user_result = await self.db.execute(select(User.email).where(User.id == store.user_id))
                user_email = user_result.scalar_one_or_none()

            created_at = parse_ozon_date(order.get("created_date"))
            status_updated_at = parse_ozon_date(order.get("state_updated_date"))
            completed_at = status_updated_at if status == "COMPLETED" else None
            timeslot_from, timeslot_to = self._extract_supply_timeslot(order)
            timeslot_from, timeslot_to, completed_at = self._normalize_supply_dates(
                status=status,
                timeslot_from=timeslot_from,
                timeslot_to=timeslot_to,
                completed_at=completed_at,
            )

            supplies_data = order.get("supplies", [])
            primary_supply = next(
                (supply_payload for supply_payload in supplies_data if isinstance(supply_payload, dict)),
                {},
            )

            dropoff_warehouse_id = await self._get_or_create_ozon_warehouse(
                order.get("drop_off_warehouse"),
                acquire_lock=False,
            )
            storage_warehouse_id = await self._get_or_create_ozon_warehouse(
                primary_supply.get("storage_warehouse"),
                acquire_lock=False,
            )
            eta_date = None
            storage_warehouse_payload = primary_supply.get("storage_warehouse") if isinstance(primary_supply, dict) else {}
            if isinstance(storage_warehouse_payload, dict) and storage_warehouse_payload.get("arrival_date"):
                parsed_eta = parse_ozon_date(storage_warehouse_payload.get("arrival_date"))
                eta_date = parsed_eta.date() if parsed_eta else None

            stmt = select(Supply).where(
                Supply.store_id == store_id,
                Supply.ozon_order_id == ozon_order_id,
            )
            result = await self.db.execute(stmt)
            supply = result.scalar_one_or_none()
            previous_status = supply.status if supply else None
            previous_timeslot_from = supply.timeslot_from if supply else None
            previous_timeslot_to = supply.timeslot_to if supply else None
            acceptance_at_storage_at = supply.acceptance_at_storage_at if supply else None

            if status == "ACCEPTANCE_AT_STORAGE_WAREHOUSE" and status_updated_at:
                acceptance_at_storage_at = status_updated_at
            elif (
                status in {"REPORTS_CONFIRMATION_AWAITING", "COMPLETED"}
                and acceptance_at_storage_at is None
                and status_updated_at
            ):
                # Fallback for cases where we first observe the supply after it already reached OZON.
                acceptance_at_storage_at = status_updated_at

            if not supply:
                supply = Supply(
                    store_id=store_id,
                    ozon_order_id=ozon_order_id,
                    order_number=order_number,
                    status=status,
                    dropoff_warehouse_id=dropoff_warehouse_id,
                    storage_warehouse_id=storage_warehouse_id,
                    timeslot_from=timeslot_from,
                    timeslot_to=timeslot_to,
                    created_at=created_at,
                    completed_at=completed_at,
                    acceptance_at_storage_at=acceptance_at_storage_at,
                    eta_date=eta_date,
                )
                self.db.add(supply)
                await self.db.flush()
                if allow_created_notifications and should_notify_supply_created(status=status, created_at=created_at):
                    event_id = await queue_supply_created(
                        self.db,
                        supply_id=supply.id,
                        order_number=order_number,
                        store_id=store_id,
                        store_name=store_name,
                        status=status,
                        user_email=user_email,
                        timeslot_from=timeslot_from,
                        timeslot_to=timeslot_to,
                    )
                    if event_id:
                        queued_event_ids.append(event_id)
                else:
                    reason = "first sync muted" if not allow_created_notifications else "historical final status"
                    logger.info(
                        f"   ⏭️ Skipping supply_created notification "
                        f"for order {order_number or supply.id} ({reason}, status={status})"
                    )
            else:
                supply.status = status
                supply.dropoff_warehouse_id = dropoff_warehouse_id
                supply.storage_warehouse_id = storage_warehouse_id
                supply.timeslot_from = timeslot_from
                supply.timeslot_to = timeslot_to
                supply.completed_at = completed_at
                supply.acceptance_at_storage_at = acceptance_at_storage_at
                supply.eta_date = eta_date
                if created_at:
                    supply.created_at = created_at
                await self.db.flush()
                if previous_status != status:
                    logger.info(
                        f"   📣 Supply {supply.id} status changed: "
                        f"{previous_status or '-'} -> {status}"
                    )
                    event_id = await queue_supply_status_changed(
                        self.db,
                        supply_id=supply.id,
                        order_number=order_number,
                        store_id=store_id,
                        store_name=store_name,
                        old_status=previous_status or "-",
                        new_status=status,
                        user_email=user_email,
                        timeslot_from=timeslot_from,
                        timeslot_to=timeslot_to,
                    )
                    if event_id:
                        queued_event_ids.append(event_id)
                if (
                    (previous_timeslot_from != timeslot_from or previous_timeslot_to != timeslot_to)
                    and _should_notify_timeslot_change(previous_status, status)
                ):
                    event_id = await queue_supply_timeslot_changed(
                        self.db,
                        supply_id=supply.id,
                        order_number=order_number,
                        store_id=store_id,
                        store_name=store_name,
                        status=status,
                        old_timeslot_from=previous_timeslot_from,
                        old_timeslot_to=previous_timeslot_to,
                        new_timeslot_from=timeslot_from,
                        new_timeslot_to=timeslot_to,
                        user_email=user_email,
                    )
                    if event_id:
                        queued_event_ids.append(event_id)

            if supplies_data:
                await self._process_supply_items(
                    supply.id,
                    store_id,
                    store.user_id if store else None,
                    supplies_data,
                    client,
                    bundle_products_by_bundle_id=bundle_products_by_bundle_id,
                )

            return queued_event_ids

        except Exception as e:
            logger.error(f"      ❌ Error in _process_supply_order: {e}")
            raise

    async def _load_archived_catalog_index(self, store_id: int, user_id: int | None, client: OzonClient) -> dict[str, dict[str, Any]]:
        cached = self._archived_catalog_cache.get(store_id)
        if cached is not None:
            return cached

        logger.warning("🗄️ Loading ARCHIVED catalog fallback for store {} because supply SKU was not found locally", store_id)
        archived_products = await self._load_products_payloads_by_visibility(
            client,
            visibility="ARCHIVED",
            store_id=store_id,
        )
        if not archived_products:
            self._archived_catalog_cache[store_id] = {}
            logger.warning("🗄️ ARCHIVED catalog for store {} is empty", store_id)
            return {}

        archived_attrs = await self._load_attributes_for_products(
            client,
            store_id=store_id,
            all_products_payloads=archived_products,
            debug_prefix="ARCHIVED PRODUCT ATTR FETCH DEBUG",
        )

        index: dict[str, dict[str, Any]] = {}
        for row in archived_attrs:
            sku = normalize_optional_string(row.get("sku"))
            if sku:
                index[sku] = row

        self._archived_catalog_cache[store_id] = index
        logger.warning(
            "🗄️ ARCHIVED catalog loaded for store {}: archived_products={} archived_attr_rows={} archived_sku_index={}",
            store_id,
            len(archived_products),
            len(archived_attrs),
            len(index),
        )
        return index

    async def _ensure_archived_variant_for_supply_item(
        self,
        *,
        store_id: int,
        user_id: int | None,
        sku: str,
        client: OzonClient,
    ) -> Optional[Variant]:
        archived_index = await self._load_archived_catalog_index(store_id, user_id, client)
        attrs_payload = archived_index.get(str(sku))
        if not attrs_payload:
            return None

        existing_variant_result = await self.db.execute(select(Variant).where(Variant.sku == str(sku)))
        existing_variant = existing_variant_result.scalar_one_or_none()
        if existing_variant:
            if not existing_variant.is_archived:
                existing_variant.is_archived = True
                await self.db.flush()
            return existing_variant

        offer_id = normalize_optional_string(attrs_payload.get("offer_id")) or ""
        barcode = normalize_optional_string(attrs_payload.get("barcode"))
        product_name = attrs_payload.get("name", "Archived Ozon product")
        base_name = extract_base_product_name(product_name)
        images = attrs_payload.get("images", [])
        image_url = images[0] if images else None
        warehouse_product_id = await self._get_or_create_warehouse_product(
            user_id=user_id or 0,
            name=base_name or product_name,
        )

        product_result = await self.db.execute(
            select(Product).where(
                Product.store_id == store_id,
                Product.base_name == base_name,
            )
        )
        product = product_result.scalars().first()
        if not product:
            product = Product(
                store_id=store_id,
                warehouse_product_id=warehouse_product_id,
                name=product_name,
                base_name=base_name,
                image_url=image_url,
            )
            self.db.add(product)
            await self.db.flush()

        pack_size = 1
        for attr in attrs_payload.get("attributes", []):
            if attr.get("id") == ATTR_ID_PACK_SIZE:
                values = attr.get("values", [])
                if values:
                    pack_size = extract_pack_size_from_attribute(str(values[0].get("value", "")))
                    break
        if pack_size == 1 and offer_id:
            pack_size = extract_pack_size_from_text(offer_id)
        if pack_size == 1:
            pack_size = extract_pack_size_from_text(product_name)

        variant = Variant(
            product_id=product.id,
            sku=str(sku),
            offer_id=offer_id,
            barcode=barcode,
            pack_size=pack_size,
            is_archived=True,
        )
        self.db.add(variant)
        await self.db.flush()

        attrs_to_insert: list[dict[str, Any]] = []
        color_value = None
        size_value = None
        gender_value = None
        pack_value = None

        for attr in attrs_payload.get("attributes", []):
            attr_id = attr.get("id")
            if attr_id in IGNORED_ATTR_IDS:
                continue
            values = attr.get("values", [])
            if not values:
                continue
            attr_value = str(values[0].get("value", "")).strip()
            if not is_valid_attribute_value(attr_value):
                continue
            if attr_id == ATTR_ID_COLOR and not color_value:
                color_value = attr_value
            elif attr_id == ATTR_ID_COLOR_ALT and not color_value:
                color_value = attr_value
            elif attr_id == ATTR_ID_SIZE and not size_value:
                size_value = attr_value
            elif attr_id == ATTR_ID_GENDER and not gender_value:
                gender_value = attr_value
            elif attr_id == ATTR_ID_PACK_SIZE and not pack_value:
                pack_value = attr_value

        if color_value:
            attrs_to_insert.append({"variant_id": variant.id, "name": "Цвет", "value": color_value})
        if size_value:
            attrs_to_insert.append({"variant_id": variant.id, "name": "Размер", "value": size_value})
        if gender_value:
            attrs_to_insert.append({"variant_id": variant.id, "name": "Пол", "value": gender_value})
        if pack_value:
            attrs_to_insert.append({"variant_id": variant.id, "name": "Упаковка", "value": pack_value})

        if attrs_to_insert:
            await self.db.execute(insert(VariantAttribute).values(attrs_to_insert))

        logger.warning(
            "🗄️ Restored archived variant for supply SKU {} in store {} (variant_id={}, offer_id='{}', barcode='{}')",
            sku,
            store_id,
            variant.id,
            offer_id or "—",
            barcode or "—",
        )
        return variant

    async def _process_supply_items(
        self,
        supply_id: int,
        store_id: int,
        user_id: int | None,
        supplies_data: list,
        client: OzonClient,
        *,
        bundle_products_by_bundle_id: Optional[dict[str, list[dict[str, Any]]]] = None,
    ):
        bundle_products_by_bundle_id = bundle_products_by_bundle_id or {}

        for sup in supplies_data:
            if not isinstance(sup, dict):
                logger.warning("         ⚠️ Skipping malformed supply payload without object body")
                continue
            bundle_id = str(sup.get("bundle_id") or "").strip()
            if not bundle_id:
                continue

            products = bundle_products_by_bundle_id.get(bundle_id)
            if products is None:
                for attempt in range(3):
                    try:
                        products = await self._get_bundle_products_with_retry(client, bundle_id, attempt)
                        bundle_products_by_bundle_id[bundle_id] = products
                        break
                    except HTTPStatusError as e:
                        if e.response.status_code == 429 and attempt < 2:
                            wait_time = (attempt + 1) * 2
                            logger.warning(f"         Rate limited, waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"         ❌ Error processing bundle {bundle_id}: {e}")
                            raise
                    except Exception as e:
                        if attempt < 2:
                            wait_time = (attempt + 1) * 2
                            logger.warning(f"         Error, retrying in {wait_time}s: {e}")
                            await asyncio.sleep(wait_time)
                        else:
                            logger.error(f"         ❌ Error processing bundle {bundle_id}: {e}")
                            raise

            products = products or []
            logger.info(f"         Found {len(products)} products in bundle {bundle_id}")

            for item in products:
                sku = item.get("sku")
                if not sku:
                    continue

                stmt = select(Variant).where(Variant.sku == str(sku))
                result = await self.db.execute(stmt)
                variant = result.scalar_one_or_none()

                if not variant:
                    variant = await self._ensure_archived_variant_for_supply_item(
                        store_id=store_id,
                        user_id=user_id,
                        sku=str(sku),
                        client=client,
                    )

                if not variant:
                    logger.warning(f"         ⚠️ Variant not found for SKU {sku}")
                    continue

                stmt = select(SupplyItem).where(
                    SupplyItem.supply_id == supply_id,
                    SupplyItem.variant_id == variant.id,
                )
                result = await self.db.execute(stmt)
                supply_item = result.scalar_one_or_none()

                quantity = item.get("quantity", 0)

                if not supply_item:
                    supply_item = SupplyItem(
                        supply_id=supply_id,
                        variant_id=variant.id,
                        quantity=quantity,
                    )
                    self.db.add(supply_item)
                elif supply_item.quantity != quantity:
                    supply_item.quantity = quantity

    async def _get_bundle_products_with_retry(self, client, bundle_id: str, attempt: int):
        try:
            return await client.get_supply_bundle_products(bundle_id)
        except HTTPStatusError as e:
            if e.response.status_code == 429 and attempt < 2:
                raise
            raise

    async def _get_stocks_with_retry(self, client, skus: list[str], max_retries: int = 4):
        for attempt in range(max_retries):
            try:
                return await client.get_stocks(skus)
            except HTTPStatusError as e:
                wait_time = get_stocks_retry_wait_seconds(e.response.status_code, attempt)
                if wait_time is not None and attempt < max_retries - 1:
                    logger.warning(
                        "Stocks request failed with HTTP {}, waiting {}s before retry (attempt {}/{})",
                        e.response.status_code,
                        wait_time,
                        attempt + 1,
                        max_retries,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                raise
            except Exception:
                raise

    async def sync_stocks_for_store(self, store: Store):
        store = await self._get_live_store(store.id)
        if not store:
            return {
                "total_batches": 0,
                "successful_batches": 0,
                "failed_batches": 0,
                "batch_errors": ["store missing or inactive"],
            }

        store_id = store.id
        store_name = store.name
        decrypted_key = decrypt_api_key(store.api_key_encrypted)
        client = OzonClient(store.client_id, decrypted_key, store_name)

        logger.info(f"🔄 Starting stocks sync for store {store_id} ({store_name})")

        await self._refresh_clusters_before_stock_sync(
            client,
            store_id=store_id,
            store_name=store_name,
        )

        stmt = select(Variant.sku, Variant.id).join(Product).where(Product.store_id == store_id)
        result = await self.db.execute(stmt)
        sku_data = [(str(row[0]), row[1]) for row in result if row[0]]
        sku_to_variant_id = {sku: variant_id for sku, variant_id in sku_data}

        if not sku_data:
            logger.info(f"⚠️ No SKUs found for store {store_id}")
            return {
                "total_batches": 0,
                "successful_batches": 0,
                "failed_batches": 0,
                "batch_errors": [],
            }

        skus = [s[0] for s in sku_data]
        logger.info(f"📦 Found {len(skus)} SKUs to sync")

        try:
            batch_size = 100
            total_batches = (len(skus) + batch_size - 1) // batch_size
            successful_batches = 0
            failed_batches = 0
            batch_errors: list[str] = []
            sync_started_at = datetime.now()

            for i in range(0, len(skus), batch_size):
                batch_num = i // batch_size + 1
                batch_skus = skus[i:i + batch_size]

                logger.info(f"🔍 Processing stocks batch {batch_num}/{total_batches}")

                try:
                    stocks_data = await self._get_stocks_with_retry(client, batch_skus)

                    warehouse_cache_by_ozon_id: dict[str, int] = {}
                    warehouse_cache_by_name: dict[str, int] = {}
                    collapsed_rows: dict[tuple[int, int], dict[str, Any]] = {}
                    duplicates_collapsed = 0
                    current_ts = datetime.now()

                    stock_warehouse_lock_keys = sorted({
                        key
                        for item in stocks_data
                        for key in [
                            self._warehouse_lock_key_from_payload(
                                {
                                    "warehouse_id": str(item.get("warehouse_id") or "0"),
                                    "name": (item.get("warehouse_name") or "Unknown").strip(),
                                }
                            )
                        ]
                        if key
                    })
                    if stock_warehouse_lock_keys:
                        logger.info(
                            f"🔒 Pre-acquiring {len(stock_warehouse_lock_keys)} warehouse advisory locks "
                            f"for stocks batch {batch_num}/{total_batches} in deterministic order"
                        )
                        await self._acquire_warehouse_advisory_locks(stock_warehouse_lock_keys)

                    for item in stocks_data:
                        sku = str(item.get("sku"))
                        if not sku:
                            continue

                        variant_id = sku_to_variant_id.get(sku)
                        if not variant_id:
                            continue

                        warehouse_name = (item.get("warehouse_name") or "Unknown").strip()
                        warehouse_id_ozon = str(item.get("warehouse_id") or "0")

                        warehouse_id = warehouse_cache_by_ozon_id.get(warehouse_id_ozon)
                        if warehouse_id is None and warehouse_name:
                            warehouse_id = warehouse_cache_by_name.get(warehouse_name)

                        if warehouse_id is None:
                            warehouse_id = await self._get_or_create_ozon_warehouse(
                                {
                                    "warehouse_id": warehouse_id_ozon,
                                    "name": warehouse_name,
                                }
                            )
                            if warehouse_id is None:
                                logger.warning(
                                    "   ⚠️ Skipping stock row for SKU {} because warehouse could not be resolved",
                                    sku,
                                )
                                continue
                            warehouse_cache_by_ozon_id[warehouse_id_ozon] = warehouse_id
                            warehouse_cache_by_name[warehouse_name] = warehouse_id

                        key = (variant_id, warehouse_id)
                        valid_stock_count = item.get("valid_stock_count")
                        requested_stock_count = item.get("requested_stock_count")
                        in_supply_value = (
                            valid_stock_count
                            if valid_stock_count is not None
                            else requested_stock_count
                        )

                        row = {
                            "variant_id": variant_id,
                            "warehouse_id": warehouse_id,
                            "available_to_sell": int(item.get("available_stock_count") or 0),
                            "in_supply": int(in_supply_value or 0),
                            "requested_to_supply": int(requested_stock_count or 0),
                            "in_transit": int(item.get("transit_stock_count") or 0),
                            "returning": int(item.get("return_from_customer_stock_count") or 0),
                            "updated_at": current_ts,
                        }

                        if key in collapsed_rows:
                            duplicates_collapsed += 1
                            existing = collapsed_rows[key]
                            existing["available_to_sell"] = max(existing["available_to_sell"], row["available_to_sell"])
                            existing["in_supply"] = max(existing["in_supply"], row["in_supply"])
                            existing["requested_to_supply"] = max(existing["requested_to_supply"], row["requested_to_supply"])
                            existing["in_transit"] = max(existing["in_transit"], row["in_transit"])
                            existing["returning"] = max(existing["returning"], row["returning"])
                            existing["updated_at"] = current_ts
                        else:
                            collapsed_rows[key] = row

                    if duplicates_collapsed:
                        logger.warning(
                            "   ⚠️ Collapsed {} duplicate stock rows in batch {} for store {} before upsert",
                            duplicates_collapsed,
                            batch_num,
                            store_id,
                        )

                    stocks_to_upsert = list(collapsed_rows.values())

                    if stocks_to_upsert:
                        stmt = insert(OzonStock).values(stocks_to_upsert)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["variant_id", "warehouse_id"],
                            set_=dict(
                                available_to_sell=stmt.excluded.available_to_sell,
                                in_supply=stmt.excluded.in_supply,
                                requested_to_supply=stmt.excluded.requested_to_supply,
                                in_transit=stmt.excluded.in_transit,
                                returning=stmt.excluded.returning,
                                updated_at=stmt.excluded.updated_at,
                            ),
                        )
                        await self.db.execute(stmt)

                    await self.db.commit()
                    successful_batches += 1
                    logger.info(f"   ✅ Batch {batch_num} completed")

                except Exception as e:
                    logger.error(f"   ❌ Error processing stocks batch {batch_num}: {e}")
                    failed_batches += 1
                    batch_errors.append(f"batch {batch_num}: {e}")
                    await self.db.rollback()

                await asyncio.sleep(0.5)

            logger.info(
                f"✅ Stocks sync completed: {successful_batches}/{total_batches} batches successful, "
                f"{failed_batches} failed"
            )

            if failed_batches == 0:
                variant_ids = list(sku_to_variant_id.values())
                if variant_ids:
                    cleanup_stmt = delete(OzonStock).where(
                        OzonStock.variant_id.in_(variant_ids),
                        or_(
                            OzonStock.updated_at.is_(None),
                            OzonStock.updated_at < sync_started_at,
                        ),
                    )
                    cleanup_result = await self.db.execute(cleanup_stmt)
                    await self.db.commit()
                    removed_rows = int(cleanup_result.rowcount or 0)
                    if removed_rows:
                        logger.info(
                            "🧹 Removed {} stale Ozon stock rows for store {} after successful stocks sync",
                            removed_rows,
                            store_id,
                        )

        except Exception as e:
            logger.error(f"❌ Fatal error in stocks sync: {e}")
            raise
        finally:
            await client.close()

        logger.info(f"✅ Stocks sync completed for store {store_id}")
        return {
            "total_batches": total_batches,
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
            "batch_errors": batch_errors,
        }
