from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from payment_processor.db.models import Outbox, Payment
from payment_processor.db.session import Database


@pytest.mark.asyncio
async def test_create_payment_returns_accepted_and_creates_outbox(
    database: Database,
    client: AsyncClient,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    response = await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=payment_payload,
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    payment_id = UUID(body["payment_id"])

    async with database.session_factory() as session:
        payment_count = await session.scalar(
            select(func.count()).select_from(Payment).where(Payment.id == payment_id)
        )
        outbox_count = await session.scalar(
            select(func.count())
            .select_from(Outbox)
            .where(Outbox.payment_id == payment_id)
        )
        assert payment_count == 1
        assert outbox_count == 1


@pytest.mark.asyncio
async def test_valid_nested_metadata_and_unicode_are_preserved(
    database: Database,
    client: AsyncClient,
    idempotency_key: str,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    metadata = {
        "заказ": [
            {"товар": "Книга", "количество": 2, "скидка": 0.5},
            True,
            False,
            None,
            {},
            [],
        ]
    }
    payload = payment_payload.copy()
    payload["description"] = "Оплата книги"
    payload["metadata"] = metadata

    response = await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=payload,
    )

    assert response.status_code == 202
    async with database.session_factory() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        assert payment is not None
        assert payment.description == payload["description"]
        assert payment.payment_metadata == metadata
        outbox = await session.scalar(
            select(Outbox).where(Outbox.payment_id == payment.id)
        )
        assert outbox is not None
        assert outbox.payload["metadata"] == metadata
