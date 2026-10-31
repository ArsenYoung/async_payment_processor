from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from payment_processor.bootstrap import create_payment_service
from payment_processor.db.models import Outbox, Payment, PaymentCurrency, PaymentStatus
from payment_processor.db.session import Database


@pytest.mark.asyncio
@pytest.mark.parametrize("status", list(PaymentStatus))
async def test_get_payment_returns_saved_details_after_session_closes(
    database: Database,
    idempotency_key: str,
    status: PaymentStatus,
) -> None:
    saved_payment = await _save_payment(database, idempotency_key, status)
    service = create_payment_service(database.session_scope)

    payment = await service.get_payment(saved_payment.id)

    assert payment is not None
    assert payment is not saved_payment
    assert payment.payment_id == saved_payment.id
    assert payment.amount == saved_payment.amount
    assert payment.currency == saved_payment.currency
    assert payment.description == saved_payment.description
    assert payment.metadata == saved_payment.payment_metadata
    assert payment.idempotency_key == saved_payment.idempotency_key
    assert payment.webhook_url == saved_payment.webhook_url
    assert payment.status == status
    assert payment.created_at == saved_payment.created_at
    assert payment.processed_at == saved_payment.processed_at


@pytest.mark.asyncio
async def test_get_payment_returns_none_for_unknown_id_without_creating_records(
    database: Database,
    idempotency_key: str,
) -> None:
    saved_payment = await _save_payment(
        database, idempotency_key, PaymentStatus.PENDING
    )
    service = create_payment_service(database.session_scope)
    payment_count = select(func.count()).select_from(Payment)
    outbox_count = select(func.count()).select_from(Outbox)

    async with database.session_factory() as session:
        counts_before = (
            await session.scalar(payment_count),
            await session.scalar(outbox_count),
        )

        payment = await service.get_payment(uuid4())

        assert payment is None
        assert (
            await session.scalar(payment_count),
            await session.scalar(outbox_count),
        ) == counts_before
        existing_payment = await session.get(Payment, saved_payment.id)
        assert existing_payment is not None
        assert existing_payment.idempotency_key == idempotency_key


async def _save_payment(
    database: Database,
    idempotency_key: str,
    status: PaymentStatus,
) -> Payment:
    payment = Payment(
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Payment lookup test",
        payment_metadata={"order_id": "lookup-order", "items": [{"quantity": 2}]},
        idempotency_key=idempotency_key,
        webhook_url="https://example.com/webhook",
        status=status,
        processed_at=(None if status == PaymentStatus.PENDING else datetime.now(UTC)),
    )
    async with database.session_scope() as session:
        session.add(payment)
        await session.flush()
    return payment
