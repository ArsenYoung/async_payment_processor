from collections.abc import Callable
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient

from payment_processor.core.config import settings
from payment_processor.db.models import Payment, PaymentStatus
from payment_processor.db.session import Database
from payment_processor.services.payment_service import PaymentService


@pytest.mark.asyncio
@pytest.mark.parametrize("payment_status", list(PaymentStatus))
async def test_get_payment_returns_saved_details(
    database: Database,
    client: AsyncClient,
    payment_payload: dict,
    payment_headers: dict[str, str],
    payment_status: PaymentStatus,
) -> None:
    created = await client.post(
        "/api/v1/payments", headers=payment_headers, json=payment_payload
    )
    assert created.status_code == 202
    payment_id = UUID(created.json()["payment_id"])
    processed_at = (
        None if payment_status == PaymentStatus.PENDING else datetime.now(UTC)
    )
    async with database.session_scope() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        payment.status = payment_status
        payment.processed_at = processed_at
        created_at = payment.created_at

    response = await client.get(
        f"/api/v1/payments/{payment_id}",
        headers={"X-API-Key": payment_headers["X-API-Key"]},
    )

    assert response.status_code == 200
    body = response.json()
    assert datetime.fromisoformat(body.pop("created_at")) == created_at
    returned_processed_at = body.pop("processed_at")
    assert (
        None
        if returned_processed_at is None
        else datetime.fromisoformat(returned_processed_at)
    ) == processed_at
    assert body == {
        "payment_id": str(payment_id),
        **payment_payload,
        "idempotency_key": payment_headers["Idempotency-Key"],
        "status": payment_status.value,
    }


@pytest.mark.asyncio
async def test_get_unknown_payment_returns_not_found(client: AsyncClient) -> None:
    response = await client.get(
        f"/api/v1/payments/{uuid4()}",
        headers={"X-API-Key": settings.api_key},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Payment not found"}


@pytest.mark.asyncio
@pytest.mark.parametrize("api_key", [None, "wrong-api-key"], ids=["missing", "wrong"])
async def test_get_payment_rejects_invalid_api_key_before_lookup(
    client: AsyncClient,
    override_payment_service: Callable[[PaymentService], None],
    api_key: str | None,
) -> None:
    service = Mock(spec=PaymentService)
    override_payment_service(service)

    response = await client.get(
        f"/api/v1/payments/{uuid4()}",
        headers={} if api_key is None else {"X-API-Key": api_key},
    )

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid API key"}
    service.get_payment.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_payment_rejects_invalid_uuid_before_lookup(
    client: AsyncClient,
    override_payment_service: Callable[[PaymentService], None],
) -> None:
    service = Mock(spec=PaymentService)
    override_payment_service(service)

    response = await client.get(
        "/api/v1/payments/not-a-uuid",
        headers={"X-API-Key": settings.api_key},
    )

    assert response.status_code == 422
    errors = response.json()["detail"]
    assert len(errors) == 1
    assert errors[0]["loc"] == ["path", "payment_id"]
    service.get_payment.assert_not_awaited()
