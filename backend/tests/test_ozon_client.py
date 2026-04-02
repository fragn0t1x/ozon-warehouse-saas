import pytest

from app.services.ozon.client import (
    OzonClient,
    OzonSchemaError,
    get_stocks_endpoint_cooldown_seconds,
    get_stocks_retry_wait_seconds,
)


def test_get_stocks_retry_wait_seconds_for_rate_limit_and_5xx():
    assert get_stocks_retry_wait_seconds(429, 0) == 3
    assert get_stocks_retry_wait_seconds(429, 1) == 6
    assert get_stocks_retry_wait_seconds(500, 0) == 6
    assert get_stocks_retry_wait_seconds(503, 1) == 9
    assert get_stocks_retry_wait_seconds(404, 0) is None


def test_get_stocks_endpoint_cooldown_seconds():
    assert get_stocks_endpoint_cooldown_seconds(429) == 12
    assert get_stocks_endpoint_cooldown_seconds(500) == 20
    assert get_stocks_endpoint_cooldown_seconds(502) == 20
    assert get_stocks_endpoint_cooldown_seconds(400) is None


@pytest.mark.asyncio
async def test_validate_response_shape_rejects_missing_expected_top_level_key():
    client = OzonClient("client", "api-key", emit_notifications=False)
    try:
        with pytest.raises(OzonSchemaError):
            await client._validate_response_shape("/v1/analytics/stocks", {"limit": 1}, {"result": {}})
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_validate_response_shape_rejects_non_object_payload():
    client = OzonClient("client", "api-key", emit_notifications=False)
    try:
        with pytest.raises(OzonSchemaError):
            await client._validate_response_shape("/v1/analytics/stocks", {"limit": 1}, [])
    finally:
        await client.close()
