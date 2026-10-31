import json
from collections.abc import Callable
from unittest.mock import Mock

import pytest
from httpx import AsyncClient
from pydantic import JsonValue
from sqlalchemy import func, select

from payment_processor.core.config import settings
from payment_processor.db.models import Outbox, Payment
from payment_processor.db.session import Database
from payment_processor.services.payment_service import PaymentService

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "api_key",
    [None, b"wrong-api-key", b"\xff"],
    ids=["missing", "wrong", "non-ascii"],
)
async def test_healthcheck_requires_valid_api_key(
    client: AsyncClient,
    api_key: bytes | None,
) -> None:
    response = await client.get(
        "/health",
        headers=[] if api_key is None else [(b"X-API-Key", api_key)],
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid API key"}


@pytest.mark.asyncio
async def test_missing_api_key_returns_unauthorized(
    client: AsyncClient,
    idempotency_key: str,
    payment_payload: dict,
) -> None:
    response = await client.post(
        "/api/v1/payments",
        headers={"Idempotency-Key": idempotency_key},
        json=payment_payload,
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_missing_idempotency_key_is_rejected_before_creation(
    client: AsyncClient,
    override_payment_service: Callable[[PaymentService], None],
    payment_payload: dict,
) -> None:
    service = Mock(spec=PaymentService)
    override_payment_service(service)

    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": settings.api_key},
        json=payment_payload,
    )

    assert response.status_code == 422
    errors = response.json()["detail"]
    assert len(errors) == 1
    assert errors[0]["loc"] == ["header", "Idempotency-Key"]
    assert errors[0]["type"] == "missing"
    service.create_payment.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        ("currency", "GBP", "enum"),
        ("amount", "10.123", "decimal_max_places"),
    ],
    ids=["unsupported-currency", "excess-decimal-places"],
)
async def test_invalid_currency_or_precision_is_rejected_before_creation(
    database: Database,
    client: AsyncClient,
    idempotency_key: str,
    override_payment_service: Callable[[PaymentService], None],
    field: str,
    value: str,
    error_type: str,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    service = Mock(spec=PaymentService)
    override_payment_service(service)
    payload = payment_payload.copy()
    payload[field] = value

    response = await client.post(
        "/api/v1/payments", headers=payment_headers, json=payload
    )

    assert response.status_code == 422
    errors = response.json()["detail"]
    assert len(errors) == 1
    assert errors[0]["loc"] == ["body", field]
    assert errors[0]["type"] == error_type
    service.create_payment.assert_not_awaited()
    await assert_no_payment_or_outbox(database, idempotency_key)


@pytest.mark.asyncio
async def test_invalid_payload_returns_validation_error(
    client: AsyncClient,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    invalid_payload = payment_payload.copy()
    invalid_payload["amount"] = "-1.00"

    response = await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=invalid_payload,
    )

    assert response.status_code == 422


@pytest.mark.asyncio
@pytest.mark.parametrize("normalized_length", [2048, 2049])
@pytest.mark.parametrize("unicode_path", [False, True], ids=["ascii", "unicode"])
async def test_webhook_url_length_matches_database_limit(
    database: Database,
    client: AsyncClient,
    idempotency_key: str,
    normalized_length: int,
    unicode_path: bool,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    prefix = "https://example.com/"
    raw_character = "я" if unicode_path else "a"
    encoded_character = "%D1%8F" if unicode_path else "a"
    padding = "a" * (normalized_length - len(prefix) - len(encoded_character))
    payload = payment_payload.copy()
    payload["webhook_url"] = prefix + raw_character + padding
    normalized_url = prefix + encoded_character + padding

    response = await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=payload,
    )

    expected_status = 202 if normalized_length == 2048 else 422
    assert response.status_code == expected_status

    async with database.session_factory() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        outbox_count = await session.scalar(
            select(func.count())
            .select_from(Outbox)
            .join(Payment, Outbox.payment_id == Payment.id)
            .where(Payment.idempotency_key == idempotency_key)
        )
        if expected_status == 202:
            assert payment is not None
            assert payment.webhook_url == normalized_url
            assert len(payment.webhook_url) == 2048
            assert outbox_count == 1
        else:
            assert payment is None
            assert outbox_count == 0
            assert any(
                error["loc"] == ["body", "webhook_url"]
                for error in response.json()["detail"]
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [
        {"nested": [{"value": float("nan")}]},
        {"nested": [{"value": float("inf")}]},
        {"nested": [{"value": float("-inf")}]},
        {"nested": [{"value": "before\x00after"}]},
        {"nested": [{"before\x00after": "value"}]},
        {"nested": [{"value": "\ud800"}]},
        {"nested": [{"value": "\udfff"}]},
        {"nested": [{"\ud800": "value"}]},
    ],
    ids=[
        "nan",
        "positive-infinity",
        "negative-infinity",
        "null-in-value",
        "null-in-key",
        "high-surrogate-value",
        "low-surrogate-value",
        "surrogate-key",
    ],
)
async def test_invalid_nested_metadata_returns_422_without_creating_records(
    database: Database,
    client: AsyncClient,
    idempotency_key: str,
    metadata: dict[str, JsonValue],
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    payload = payment_payload.copy()
    payload["metadata"] = metadata

    response = await client.post(
        "/api/v1/payments",
        headers=payment_headers | {"Content-Type": "application/json"},
        content=json.dumps(payload),
    )

    assert response.status_code == 422
    assert any(
        error["loc"] == ["body", "metadata"] for error in response.json()["detail"]
    )
    await assert_no_payment_or_outbox(database, idempotency_key)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "description",
    ["before\x00after", "\ud800", "\udfff"],
    ids=["null-character", "high-surrogate", "low-surrogate"],
)
async def test_invalid_description_returns_422_without_creating_records(
    database: Database,
    client: AsyncClient,
    idempotency_key: str,
    description: str,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    payload = payment_payload.copy()
    payload["description"] = description

    response = await client.post(
        "/api/v1/payments",
        headers=payment_headers | {"Content-Type": "application/json"},
        content=json.dumps(payload),
    )

    assert response.status_code == 422
    assert any(
        error["loc"] == ["body", "description"] for error in response.json()["detail"]
    )
    await assert_no_payment_or_outbox(database, idempotency_key)


@pytest.mark.asyncio
async def test_null_in_idempotency_key_returns_422(
    client: AsyncClient,
    idempotency_key: str,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    response = await client.post(
        "/api/v1/payments",
        headers=payment_headers | {"Idempotency-Key": idempotency_key + "\x00"},
        json=payment_payload,
    )

    assert response.status_code == 422
    assert any(
        error["loc"] == ["header", "Idempotency-Key"]
        for error in response.json()["detail"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("api_key", [b"wrong-api-key", b"\xff"])
async def test_invalid_api_key_returns_401(
    database: Database,
    client: AsyncClient,
    idempotency_key: str,
    api_key: bytes,
    payment_payload: dict,
) -> None:
    response = await client.post(
        "/api/v1/payments",
        headers=[
            (b"X-API-Key", api_key),
            (b"Idempotency-Key", idempotency_key.encode("ascii")),
        ],
        json=payment_payload,
    )

    assert response.status_code == 401
    await assert_no_payment_or_outbox(database, idempotency_key)


async def assert_no_payment_or_outbox(database: Database, idempotency_key: str) -> None:
    async with database.session_factory() as session:
        payment_count = await session.scalar(
            select(func.count())
            .select_from(Payment)
            .where(Payment.idempotency_key == idempotency_key)
        )
        outbox_count = await session.scalar(
            select(func.count())
            .select_from(Outbox)
            .join(Payment, Outbox.payment_id == Payment.id)
            .where(Payment.idempotency_key == idempotency_key)
        )
        assert payment_count == 0
        assert outbox_count == 0
