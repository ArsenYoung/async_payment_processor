from decimal import Decimal

import pytest
from sqlalchemy import select

from payment_processor.db.models import Payment, PaymentCurrency
from payment_processor.db.session import Database

pytestmark = pytest.mark.integration


def payment_for_key(idempotency_key: str) -> Payment:
    return Payment(
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Test database transaction",
        idempotency_key=idempotency_key,
        webhook_url="https://example.com/webhook",
    )


@pytest.mark.asyncio
async def test_session_scope_commits_payment(
    database: Database, idempotency_key: str
) -> None:
    payment = payment_for_key(idempotency_key)
    async with database.session_scope() as session:
        session.add(payment)
        await session.flush()
        payment_id = payment.id

    # Committed values remain readable after the transaction and session close.
    assert payment.id == payment_id
    assert payment.amount == Decimal("10.50")
    async with database.session_factory() as session:
        assert (
            await session.scalar(
                select(Payment.id).where(Payment.idempotency_key == idempotency_key)
            )
            == payment_id
        )


@pytest.mark.asyncio
async def test_session_scope_rolls_back_flushed_payment_on_error(
    database: Database, idempotency_key: str
) -> None:
    error = RuntimeError("Abort transaction after flush")
    with pytest.raises(RuntimeError) as caught:
        async with database.session_scope() as session:
            session.add(payment_for_key(idempotency_key))
            await session.flush()
            assert (
                await session.scalar(
                    select(Payment.id).where(Payment.idempotency_key == idempotency_key)
                )
                is not None
            )
            raise error

    assert caught.value is error
    assert not session.in_transaction()
    async with database.session_scope() as session:
        assert (
            await session.scalar(
                select(Payment.id).where(Payment.idempotency_key == idempotency_key)
            )
            is None
        )
