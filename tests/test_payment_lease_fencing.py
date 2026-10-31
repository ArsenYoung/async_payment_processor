from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import update

from payment_processor.application.errors import LeaseLostError
from payment_processor.bootstrap import create_payment_service
from payment_processor.db.models import Payment, PaymentCurrency, PaymentStatus
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.db.session import Database

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["processing", "webhook"])
async def test_stale_external_success_cannot_overwrite_new_owner_or_release_its_lease(
    outbox_database: Database, operation: str
) -> None:
    payment = Payment(
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Lease fencing",
        payment_metadata={},
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
        status=PaymentStatus.PENDING
        if operation == "processing"
        else PaymentStatus.FAILED,
        processed_at=None if operation == "processing" else datetime.now(UTC),
    )
    async with outbox_database.session_scope() as session:
        session.add(payment)
        await session.flush()
    new_token = uuid4()
    delivered_at = None

    async def external(*args: object, **kwargs: object):
        nonlocal delivered_at
        async with outbox_database.session_scope() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment.id)
                .values(
                    {f"{operation}_until": datetime.now(UTC) - timedelta(seconds=1)}
                )
            )
        async with outbox_database.session_scope() as session:
            repository = PaymentRepository(session)
            assert await repository.claim_lease(payment.id, operation, new_token, 60)
            current = await repository.find_by_id_for_update(payment.id)
            if operation == "processing":
                current.process(PaymentStatus.FAILED)
                await repository.save(current)
            else:
                await repository.mark_webhook_delivered(current)
                delivered_at = current.webhook_delivered_at
        return PaymentStatus.SUCCEEDED

    dependency = AsyncMock()
    dependency.process.side_effect = external
    dependency.send.side_effect = external
    service = create_payment_service(outbox_database.session_scope, gateway=dependency)
    with pytest.raises(LeaseLostError):
        if operation == "processing":
            await service.process_payment(payment.id)
        else:
            await service.deliver_webhook(payment.id, dependency)
    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, payment.id)
        assert saved.status == PaymentStatus.FAILED
        assert saved.webhook_delivered_at == delivered_at
        assert getattr(saved, f"{operation}_token") == new_token
