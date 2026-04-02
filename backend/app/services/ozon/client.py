import asyncio
import httpx
import hashlib
import json
import time
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional
from loguru import logger
from app.config import settings
from app.services.admin_notifications import (
    notify_ozon_api_error,
    notify_ozon_rate_limit_pressure,
    notify_ozon_schema_change,
)
from app.utils.redis_cache import cache_get_json, cache_set_json, get_redis


class OzonSchemaError(RuntimeError):
    pass


def get_stocks_retry_wait_seconds(status_code: int, attempt: int) -> int | None:
    if status_code == 429:
        return max(settings.OZON_STOCKS_RETRY_BASE_SECONDS * (attempt + 1), 1)
    if status_code in {500, 502, 503, 504}:
        return max(settings.OZON_STOCKS_RETRY_BASE_SECONDS * (attempt + 2), 1)
    return None


def get_stocks_endpoint_cooldown_seconds(status_code: int) -> int | None:
    if status_code == 429:
        return max(int(settings.OZON_STOCKS_429_COOLDOWN_SECONDS), 1)
    if status_code in {500, 502, 503, 504}:
        return max(int(settings.OZON_STOCKS_5XX_COOLDOWN_SECONDS), 1)
    return None


def get_transaction_retry_wait_seconds(status_code: int, attempt: int) -> int | None:
    if status_code == 429:
        return max(attempt + 1, 1)
    if status_code in {500, 502, 503, 504}:
        return max((attempt + 1) * 2, 2)
    return None


def get_bundle_retry_wait_seconds(status_code: int, attempt: int) -> int | None:
    if status_code == 429:
        return max((attempt + 1) * 2, 2)
    if status_code in {500, 502, 503, 504}:
        return max((attempt + 1) * 3, 3)
    return None


class OzonClient:
    EXPECTED_TOP_LEVEL_KEYS = {
        "/v3/product/list": "result",
        "/v3/product/info/list": "items",
        "/v4/product/info/attributes": "result",
        "/v5/product/info/prices": "items",
        "/v1/analytics/stocks": "items",
        "/v1/cluster/list": "clusters",
        "/v3/supply-order/list": "order_ids",
        "/v3/supply-order/get": "orders",
        "/v1/supply-order/bundle": "items",
        "/v1/report/info": "result",
        "/v1/report/list": "result",
        "/v1/report/products/create": "result",
        "/v1/report/postings/create": "result",
        "/v2/report/returns/create": "result",
        "/v1/finance/cash-flow-statement/list": "result",
        "/v2/finance/realization": "result",
        "/v3/finance/transaction/list": "result",
        "/v3/finance/transaction/totals": "result",
        "/v1/finance/mutual-settlement": "result",
        "/v1/finance/compensation": "result",
        "/v1/finance/decompensation": "result",
        "/v1/removal/from-supply/list": "returns_summary_report_rows",
        "/v1/removal/from-stock/list": "returns_summary_report_rows",
    }

    def __init__(self, client_id: str, api_key: str, store_name: str | None = None, emit_notifications: bool = True):
        self.client_id = client_id
        self.api_key = api_key
        self.store_name = store_name
        self.emit_notifications = emit_notifications
        logger.info("🔑 OzonClient initialized with client_id: {}", client_id)

        self.base_url = "https://api-seller.ozon.ru"
        self.headers = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json",
        }
        self._client = httpx.AsyncClient(base_url=self.base_url, headers=self.headers, timeout=30.0)
        self._request_error_retries = 3

    @staticmethod
    def _is_retryable_request_error(error: Exception) -> bool:
        return isinstance(error, httpx.RequestError)

    @staticmethod
    def _should_defer_http_error_notification(endpoint: str, status_code: int) -> bool:
        # Stocks sync has an outer retry layer in SyncService, so temporary rate limits
        # and short-lived Ozon 5xx responses should not hit Telegram until that outer
        # layer is also exhausted.
        if endpoint == "/v1/analytics/stocks" and status_code in {429, 500, 502, 503, 504}:
            return True
        return False

    @staticmethod
    def _describe_error(error: Exception | str) -> str:
        if isinstance(error, Exception):
            message = str(error).strip()
            if message:
                return f"{type(error).__name__}: {message}"
            return type(error).__name__
        text = str(error).strip()
        return text or "Unknown error"

    def _endpoint_cooldown_key(self, endpoint: str) -> str:
        return f"ozon-cooldown:{self.client_id}:{endpoint}"

    async def _wait_for_endpoint_cooldown(self, redis, endpoint: str) -> None:
        if not redis:
            return

        cooldown_key = self._endpoint_cooldown_key(endpoint)
        while True:
            ttl_ms = await redis.pttl(cooldown_key)
            if ttl_ms <= 0:
                return

            sleep_for = max(ttl_ms / 1000, 0.1)
            logger.warning(
                "🧊 Ozon cooldown engaged for client_id={} endpoint={} ({} ms left), sleeping {:.3f}s",
                self.client_id,
                endpoint,
                ttl_ms,
                sleep_for,
            )
            await asyncio.sleep(sleep_for)

    async def _set_endpoint_cooldown(self, endpoint: str, status_code: int) -> None:
        redis = await get_redis()
        cooldown_seconds = get_stocks_endpoint_cooldown_seconds(status_code) if endpoint == "/v1/analytics/stocks" else None
        if not redis or not cooldown_seconds:
            return

        await redis.set(
            self._endpoint_cooldown_key(endpoint),
            str(status_code),
            ex=cooldown_seconds,
        )

    async def _acquire_endpoint_spacing(self, redis, endpoint: str) -> None:
        min_interval_ms = 0
        if endpoint == "/v1/supply-order/bundle":
            min_interval_ms = max(int(settings.OZON_BUNDLE_MIN_INTERVAL_MS), 0)
        elif endpoint == "/v1/analytics/stocks":
            min_interval_ms = max(int(settings.OZON_STOCKS_MIN_INTERVAL_MS), 0)

        if not redis or min_interval_ms <= 0:
            return

        spacing_key = f"ozon-spacing:{self.client_id}:{endpoint}"
        while True:
            acquired = await redis.set(spacing_key, "1", nx=True, px=min_interval_ms)
            if acquired:
                return

            sleep_for = max(min_interval_ms / 1000, 0.05)
            logger.warning(
                "⏱️ Ozon endpoint spacing engaged for client_id={} endpoint={} ({} ms), sleeping {:.3f}s",
                self.client_id,
                endpoint,
                min_interval_ms,
                sleep_for,
            )
            await asyncio.sleep(sleep_for)

    async def _acquire_rate_limit_slot(self, endpoint: str) -> None:
        redis = await get_redis()
        global_limit = max(int(settings.OZON_RATE_LIMIT_PER_CLIENT_ID), 1)
        endpoint_limit = global_limit
        if endpoint == "/v1/supply-order/bundle":
            endpoint_limit = max(int(settings.OZON_BUNDLE_RATE_LIMIT_PER_CLIENT_ID), 1)
        elif endpoint == "/v1/analytics/stocks":
            endpoint_limit = max(int(settings.OZON_STOCKS_RATE_LIMIT_PER_CLIENT_ID), 1)
        if not redis:
            return

        await self._wait_for_endpoint_cooldown(redis, endpoint)
        await self._acquire_endpoint_spacing(redis, endpoint)

        while True:
            window = int(time.time())
            global_key = f"ozon-rate:{self.client_id}:{window}"
            global_count = await redis.incr(global_key)
            if global_count == 1:
                await redis.expire(global_key, 2)

            endpoint_key = f"ozon-rate:{self.client_id}:{endpoint}:{window}"
            endpoint_count = await redis.incr(endpoint_key)
            if endpoint_count == 1:
                await redis.expire(endpoint_key, 2)

            if global_count <= global_limit and endpoint_count <= endpoint_limit:
                return

            pressure_key = f"ozon-rate-pressure:{self.client_id}:{int(time.time() // 60)}"
            pressure_hits = await redis.incr(pressure_key)
            if pressure_hits == 1:
                await redis.expire(pressure_key, 120)
            if pressure_hits == settings.OZON_RATE_LIMIT_ALERT_THRESHOLD_PER_MINUTE:
                await notify_ozon_rate_limit_pressure(
                    client_id=self.client_id,
                    store_name=self.store_name,
                    limited_hits_last_minute=pressure_hits,
                    limit_per_second=global_limit,
                )

            sleep_for = max((window + 1) - time.time(), 0.05)
            logger.warning(
                "⏳ Ozon rate limiter engaged for client_id={} endpoint={} (global {} rps, endpoint {} rps), sleeping {:.3f}s",
                self.client_id,
                endpoint,
                global_limit,
                endpoint_limit,
                sleep_for,
            )
            await asyncio.sleep(sleep_for)

    async def _validate_response_shape(self, endpoint: str, data: Dict[str, Any], result: Any) -> Dict[str, Any]:
        if not isinstance(result, dict):
            if self.emit_notifications:
                await notify_ozon_api_error(
                    endpoint,
                    self.client_id,
                    error=f"Ожидали JSON-объект, получили {type(result).__name__}",
                    payload=data,
                )
            raise OzonSchemaError(f"Ozon API returned unexpected payload type for {endpoint}")

        expected_key = self.EXPECTED_TOP_LEVEL_KEYS.get(endpoint)
        if expected_key and expected_key not in result:
            if self.emit_notifications:
                await notify_ozon_schema_change(
                    endpoint,
                    self.client_id,
                    expected_key=expected_key,
                    actual_keys=sorted(result.keys()),
                    payload=data,
                )
            raise OzonSchemaError(f"Ozon API schema changed for {endpoint}: missing key '{expected_key}'")

        return result

    async def _post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        cache_ttl = {
            "/v3/product/list": 300,
            "/v3/product/info/list": 120,
            "/v4/product/info/attributes": 300,
            "/v5/product/info/prices": 120,
            "/v1/analytics/stocks": 120,
            "/v1/cluster/list": 21600,
            "/v3/supply-order/list": 120,
            "/v3/supply-order/get": 120,
            "/v1/supply-order/bundle": 120,
            "/v1/report/info": 5,
            "/v1/report/list": 30,
        }.get(endpoint, 0)

        cache_key = None
        if cache_ttl:
            payload_hash = hashlib.sha1(json.dumps(data, sort_keys=True).encode("utf-8")).hexdigest()
            cache_key = f"ozon:{self.client_id}:{endpoint}:{payload_hash}"
            cached = await cache_get_json(cache_key)
            if cached is not None:
                logger.info(f"✅ Cache hit for {endpoint}")
                return cached

        last_request_error: Exception | None = None
        last_http_error: httpx.HTTPStatusError | None = None
        request_timeout: float | httpx.Timeout | None = None
        if endpoint == "/v3/finance/transaction/list":
            request_timeout = httpx.Timeout(60.0, connect=30.0, read=60.0, write=30.0, pool=30.0)

        for attempt in range(self._request_error_retries):
            try:
                await self._acquire_rate_limit_slot(endpoint)
                logger.info("➡️ Ozon request {} client_id={} payload={}", endpoint, self.client_id, data)

                response = await self._client.post(endpoint, json=data, timeout=request_timeout)
                response.raise_for_status()

                result = response.json()
                result = await self._validate_response_shape(endpoint, data, result)

                if cache_ttl and cache_key:
                    await cache_set_json(cache_key, result, cache_ttl)

                return result

            except httpx.HTTPStatusError as e:
                status_code = e.response.status_code
                response_text = e.response.text

                logger.error("Ozon API error {} on {}: {}", status_code, endpoint, response_text)
                await self._set_endpoint_cooldown(endpoint, status_code)

                # Retry analytics/stocks and finance transaction list for transient HTTP statuses.
                if endpoint == "/v1/analytics/stocks":
                    wait_time = get_stocks_retry_wait_seconds(status_code, attempt)
                    if wait_time is not None and attempt < self._request_error_retries - 1:
                        logger.warning(
                            "Retryable Ozon HTTP error {} for {} (attempt {}/{}), retrying in {}s",
                            status_code,
                            endpoint,
                            attempt + 1,
                            self._request_error_retries,
                            wait_time,
                        )
                        last_http_error = e
                        await asyncio.sleep(wait_time)
                        continue

                if endpoint == "/v3/finance/transaction/list":
                    wait_time = get_transaction_retry_wait_seconds(status_code, attempt)
                    if wait_time is not None and attempt < self._request_error_retries - 1:
                        logger.warning(
                            "Retryable Ozon HTTP error {} for {} (attempt {}/{}), retrying in {}s",
                            status_code,
                            endpoint,
                            attempt + 1,
                            self._request_error_retries,
                            wait_time,
                        )
                        last_http_error = e
                        await asyncio.sleep(wait_time)
                        continue

                if endpoint == "/v1/supply-order/bundle":
                    wait_time = get_bundle_retry_wait_seconds(status_code, attempt)
                    if wait_time is not None and attempt < self._request_error_retries - 1:
                        logger.warning(
                            "Retryable Ozon HTTP error {} for {} (attempt {}/{}), retrying in {}s",
                            status_code,
                            endpoint,
                            attempt + 1,
                            self._request_error_retries,
                            wait_time,
                        )
                        last_http_error = e
                        await asyncio.sleep(wait_time)
                        continue

                if self.emit_notifications and not self._should_defer_http_error_notification(endpoint, status_code):
                    await notify_ozon_api_error(
                        endpoint,
                        self.client_id,
                        error=e,
                        payload=data,
                        status_code=status_code,
                        response_text=response_text,
                    )
                raise

            except OzonSchemaError:
                raise

            except Exception as e:
                error_text = self._describe_error(e)

                if self._is_retryable_request_error(e) and attempt < self._request_error_retries - 1:
                    wait_time = attempt + 1
                    logger.warning(
                        "Transient request failure for {} (attempt {}/{}), retrying in {}s: {}",
                        endpoint,
                        attempt + 1,
                        self._request_error_retries,
                        wait_time,
                        error_text,
                    )
                    last_request_error = e
                    await asyncio.sleep(wait_time)
                    continue

                logger.error("Request failed for {}: {}", endpoint, error_text)
                should_notify = self.emit_notifications and not isinstance(e, httpx.RequestError)
                if should_notify:
                    await notify_ozon_api_error(endpoint, self.client_id, error=e, payload=data)
                raise

        if last_http_error:
            if self.emit_notifications and not self._should_defer_http_error_notification(endpoint, last_http_error.response.status_code):
                await notify_ozon_api_error(
                    endpoint,
                    self.client_id,
                    error=last_http_error,
                    payload=data,
                    status_code=last_http_error.response.status_code,
                    response_text=last_http_error.response.text,
                )
            raise last_http_error

        if last_request_error:
            raise last_request_error

        raise RuntimeError(f"Failed to call Ozon endpoint {endpoint}")

    async def get_products_list(
        self,
        limit: int = 1000,
        last_id: str = None,
        visibility: str = "ALL",
    ) -> Dict[str, Any]:
        payload = {
            "filter": {"visibility": visibility},
            "limit": limit,
            "sort_dir": "ASC"
        }
        if last_id:
            payload["last_id"] = last_id
        response = await self._post("/v3/product/list", payload)
        return response

    async def get_product_attributes(self, product_ids: List[int]) -> List[Dict]:
        payload = {
            "filter": {
                "product_id": product_ids
            },
            "limit": len(product_ids)
        }
        logger.info(f"Requesting attributes with payload: {payload}")
        result = await self._post("/v4/product/info/attributes", payload)
        return result.get("result", [])

    async def get_product_prices(
        self,
        *,
        offer_ids: List[str] | None = None,
        product_ids: List[int] | None = None,
        visibility: str = "ALL",
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        normalized_offer_ids = [str(value).strip() for value in (offer_ids or []) if str(value).strip()]
        normalized_product_ids = [str(int(value)) for value in (product_ids or [])]
        if not normalized_offer_ids and not normalized_product_ids:
            return []

        items: List[Dict[str, Any]] = []
        cursor = ""
        while True:
            payload: Dict[str, Any] = {
                "cursor": cursor,
                "filter": {
                    "offer_id": normalized_offer_ids,
                    "product_id": normalized_product_ids,
                    "visibility": visibility,
                },
                "limit": min(max(limit, 1), 1000),
            }
            result = await self._post("/v5/product/info/prices", payload)
            items.extend(result.get("items", []) or [])
            cursor = str(result.get("cursor") or "").strip()
            if not cursor:
                break

        return items

    async def get_product_info_list(
        self,
        *,
        offer_ids: List[str] | None = None,
        product_ids: List[int] | None = None,
        skus: List[int | str] | None = None,
    ) -> List[Dict[str, Any]]:
        normalized_offer_ids = [str(value).strip() for value in (offer_ids or []) if str(value).strip()]
        normalized_product_ids = [str(int(value)) for value in (product_ids or [])]
        normalized_skus = [str(int(value)) for value in (skus or [])]

        identifiers_total = len(normalized_offer_ids) + len(normalized_product_ids) + len(normalized_skus)
        if identifiers_total <= 0:
            return []

        items: List[Dict[str, Any]] = []
        chunk_limit = 1000
        for chunk_start in range(0, max(identifiers_total, 1), chunk_limit):
            offer_chunk = normalized_offer_ids[chunk_start:chunk_start + chunk_limit]
            remaining = max(chunk_limit - len(offer_chunk), 0)
            product_chunk = normalized_product_ids[chunk_start:chunk_start + remaining]
            remaining = max(remaining - len(product_chunk), 0)
            sku_chunk = normalized_skus[chunk_start:chunk_start + remaining]
            payload: Dict[str, Any] = {}
            if offer_chunk:
                payload["offer_id"] = offer_chunk
            if product_chunk:
                payload["product_id"] = product_chunk
            if sku_chunk:
                payload["sku"] = sku_chunk
            if not payload:
                continue
            result = await self._post("/v3/product/info/list", payload)
            items.extend(result.get("items", []) or [])

        return items

    async def get_stocks(self, skus: List[str]) -> List[Dict]:
        payload = {"skus": skus}
        result = await self._post("/v1/analytics/stocks", payload)
        return result.get("items", [])

    async def get_clusters(self, cluster_type: str, cluster_ids: Optional[List[int]] = None) -> List[Dict]:
        payload: Dict[str, Any] = {
            "cluster_type": cluster_type,
        }
        if cluster_ids:
            payload["cluster_ids"] = cluster_ids

        result = await self._post("/v1/cluster/list", payload)
        return result.get("clusters", [])

    async def get_supply_orders(self, states: List[str] = None, limit: int = 100, last_id: str = None) -> Dict[str, Any]:
        if states is None:
            states = [
                "DATA_FILLING",
                "READY_TO_SUPPLY",
                "ACCEPTED_AT_SUPPLY_WAREHOUSE",
                "IN_TRANSIT",
                "ACCEPTANCE_AT_STORAGE_WAREHOUSE",
                "REPORTS_CONFIRMATION_AWAITING",
                "REPORT_REJECTED",
                "REJECTED_AT_SUPPLY_WAREHOUSE",
                "CANCELLED",
                "OVERDUE",
                "COMPLETED",
            ]

        payload = {
            "filter": {
                "states": states
            },
            "limit": limit,
            "sort_by": "ORDER_CREATION",
            "sort_dir": "DESC"
        }

        if last_id:
            payload["last_id"] = last_id

        logger.info(f"Requesting supplies with payload: {payload}")

        return await self._post("/v3/supply-order/list", payload)

    async def get_supply_order_detail(self, order_id: int) -> Dict:
        payload = {"order_ids": [order_id]}
        result = await self._post("/v3/supply-order/get", payload)
        orders = result.get("orders", [])
        return orders[0] if orders else {}

    async def get_supply_bundle_products(self, bundle_id: str) -> List[Dict]:
        payload = {"bundle_ids": [bundle_id], "limit": 100}
        result = await self._post("/v1/supply-order/bundle", payload)
        return result.get("items", [])

    async def list_reports(self, *, page: int = 1, page_size: int = 100, report_type: str = "ALL") -> Dict[str, Any]:
        payload = {
            "page": page,
            "page_size": page_size,
            "report_type": report_type,
        }
        return await self._post("/v1/report/list", payload)

    async def get_report_info(self, code: str) -> Dict[str, Any]:
        result = await self._post("/v1/report/info", {"code": code})
        return result.get("result", {})

    async def create_products_report(
        self,
        *,
        visibility: str = "ALL",
        language: str = "DEFAULT",
        offer_id: Optional[List[str]] = None,
        sku: Optional[List[int]] = None,
        search: str = "",
    ) -> str:
        payload = {
            "language": language,
            "offer_id": offer_id or [],
            "search": search,
            "sku": sku or [],
            "visibility": visibility,
        }
        result = await self._post("/v1/report/products/create", payload)
        return ((result.get("result") or {}).get("code")) or ""

    async def create_postings_report(
        self,
        *,
        processed_at_from: str,
        processed_at_to: str,
        language: str = "DEFAULT",
        offer_id: str = "",
        title: str = "",
        sku: Optional[List[int]] = None,
        status_alias: Optional[List[str]] = None,
        statuses: Optional[List[int]] = None,
        analytics_data: bool = False,
    ) -> str:
        payload = {
            "filter": {
                "processed_at_from": processed_at_from,
                "processed_at_to": processed_at_to,
                "delivery_schema": ["fbo"],
                "is_express": False,
                "sku": sku or [],
                "cancel_reason_id": [],
                "offer_id": offer_id,
                "status_alias": status_alias or [],
                "statuses": statuses or [],
                "title": title,
            },
            "language": language,
            "with": {
                "additional_data": False,
                "analytics_data": analytics_data,
                "customer_data": False,
                "jewelry_codes": False,
            },
        }
        result = await self._post("/v1/report/postings/create", payload)
        return ((result.get("result") or {}).get("code")) or ""

    async def create_returns_report(
        self,
        *,
        date_from: str,
        date_to: str,
        status: str = "",
        language: str = "DEFAULT",
    ) -> str:
        payload = {
            "filter": {
                "delivery_schema": "fbo",
                "date_from": date_from,
                "date_to": date_to,
            },
            "language": language,
        }
        if status:
            payload["filter"]["status"] = status
        result = await self._post("/v2/report/returns/create", payload)
        return ((result.get("result") or {}).get("code")) or ""

    async def get_cash_flow_statement(
        self,
        *,
        date_from: str,
        date_to: str,
        page: int = 1,
        page_size: int = 100,
        with_details: bool = False,
    ) -> Dict[str, Any]:
        payload = {
            "date": {
                "from": date_from,
                "to": date_to,
            },
            "with_details": with_details,
            "page": page,
            "page_size": page_size,
        }
        result = await self._post("/v1/finance/cash-flow-statement/list", payload)
        return result.get("result", {})

    async def get_realization_report(
        self,
        *,
        month: int,
        year: int,
    ) -> Dict[str, Any]:
        payload = {
            "month": month,
            "year": year,
        }
        result = await self._post("/v2/finance/realization", payload)
        return result.get("result", {})

    async def get_transaction_list(
        self,
        *,
        date_from: str,
        date_to: str,
        page: int = 1,
        page_size: int = 1000,
        transaction_type: str = "all",
        operation_type: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "filter": {
                "date": {
                    "from": date_from,
                    "to": date_to,
                }
            },
            "page": page,
            "page_size": page_size,
            "transaction_type": transaction_type,
        }
        if operation_type:
            payload["operation_type"] = operation_type
        result = await self._post("/v3/finance/transaction/list", payload)
        return result.get("result", {})

    async def get_transaction_totals(
        self,
        *,
        date_from: str,
        date_to: str,
        transaction_type: str = "all",
    ) -> Dict[str, Any]:
        payload = {
            "date": {
                "from": date_from,
                "to": date_to,
            },
            "transaction_type": transaction_type,
        }
        result = await self._post("/v3/finance/transaction/totals", payload)
        return result.get("result", {})

    async def create_placement_by_products_report(
        self,
        *,
        date_from: str,
        date_to: str,
    ) -> str:
        payload = {
            "date_from": date_from,
            "date_to": date_to,
        }
        result = await self._post("/v1/report/placement/by-products/create", payload)
        return (result.get("code") or ((result.get("result") or {}).get("code")) or "")

    async def create_placement_by_supplies_report(
        self,
        *,
        date_from: str,
        date_to: str,
    ) -> str:
        payload = {
            "date_from": date_from,
            "date_to": date_to,
        }
        result = await self._post("/v1/report/placement/by-supplies/create", payload)
        return (result.get("code") or ((result.get("result") or {}).get("code")) or "")

    async def get_removal_from_supply_list(
        self,
        *,
        date_from: str,
        date_to: str,
        limit: int = 500,
        last_id: str = "",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
        }
        if last_id:
            payload["last_id"] = last_id
        result = await self._post("/v1/removal/from-supply/list", payload)
        return result

    async def get_removal_from_stock_list(
        self,
        *,
        date_from: str,
        date_to: str,
        limit: int = 500,
        last_id: str = "",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
        }
        if last_id:
            payload["last_id"] = last_id
        result = await self._post("/v1/removal/from-stock/list", payload)
        return result

    async def create_compensation_report(
        self,
        *,
        date: str,
        language: str = "RU",
    ) -> str:
        payload = {
            "date": date,
            "language": language,
        }
        result = await self._post("/v1/finance/compensation", payload)
        return ((result.get("result") or {}).get("code")) or ""

    async def create_decompensation_report(
        self,
        *,
        date: str,
        language: str = "RU",
    ) -> str:
        payload = {
            "date": date,
            "language": language,
        }
        result = await self._post("/v1/finance/decompensation", payload)
        return ((result.get("result") or {}).get("code")) or ""

    async def download_report_file(self, file_url: str) -> bytes:
        parsed = urlparse(file_url)
        if parsed.netloc.endswith("api-seller.ozon.ru") or not parsed.netloc:
            response = await self._client.get(file_url)
        else:
            async with httpx.AsyncClient(timeout=60.0) as download_client:
                response = await download_client.get(file_url)
        response.raise_for_status()
        return response.content

    async def close(self):
        await self._client.aclose()
