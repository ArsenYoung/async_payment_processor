from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import update

from payment_processor.application.commands import (
    DeliverWebhookCommand,
    ProcessPaymentCommand,
)
from payment_processor.application.errors import LeaseLostError
from payment_processor.bootstrap import create_payment_service
from payment_processor.db.models import Payment, PaymentCurrency, PaymentStatus
from payment_processor.db.repositories.payments import SqlAlchemyPaymentRepository
from payment_processor.db.session import Database


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["processing", "webhook"])
async def test_stale_external_success_cannot_overwrite_new_owner_or_release_its_lease(
    outbox_database: Database, operation: str
) -> None:
    payment = Payment(
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Lease fencing",
        created_at=datetime.now(UTC) - timedelta(seconds=1),
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
            repository = SqlAlchemyPaymentRepository(session)
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
            await service.process_payment(ProcessPaymentCommand(payment.id))
        else:
            await service.deliver_webhook(DeliverWebhookCommand(payment.id), dependency)
    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, payment.id)
        assert saved.status == PaymentStatus.FAILED
        assert saved.webhook_delivered_at == delivered_at
        assert getattr(saved, f"{operation}_token") == new_token


@pytest.mark.asyncio
async def test_busy_lease_wait_is_bounded_and_releases_only_own_token(monkeypatch):
    import asyncio
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from payment_processor.application.errors import LeaseContentionError
    from payment_processor.messaging.errors import is_retryable_processing_error
    from payment_processor.services import payment_leases

    saved = SimpleNamespace(id=uuid4())
    store = AsyncMock()
    store.find_by_id_for_update.return_value = saved
    store.claim_lease.return_value = False

    @asynccontextmanager
    async def scope():
        yield store

    monkeypatch.setattr(payment_leases, "LEASE_WAIT_TIMEOUT", 0.03)
    with pytest.raises(LeaseContentionError) as caught:
        async with asyncio.timeout(0.5):
            async with payment_leases.payment_lease(
                scope, saved.id, "processing", lambda payment: True
            ):
                pytest.fail("A busy lease must never be yielded")
    assert is_retryable_processing_error(caught.value)
    store.release_lease.assert_awaited_once()
    assert (
        store.release_lease.await_args.args[2] == store.claim_lease.await_args.args[2]
    )


@pytest.mark.asyncio
async def test_lease_backoff_reduces_polling_and_reuses_completed_work(monkeypatch):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from payment_processor.services import payment_leases

    saved = SimpleNamespace(id=uuid4(), pending=True)
    store = AsyncMock()
    store.find_by_id_for_update.return_value = saved
    store.claim_lease.return_value = False
    active = False
    pauses = []

    @asynccontextmanager
    async def scope():
        nonlocal active
        active = True
        try:
            yield store
        finally:
            active = False

    async def pause(delay):
        assert not active
        pauses.append(delay)
        if len(pauses) == 6:
            saved.pending = False

    monkeypatch.setattr(payment_leases.asyncio, "sleep", pause)
    monkeypatch.setattr(payment_leases.random, "uniform", lambda low, high: high)
    async with payment_leases.payment_lease(
        scope, saved.id, "processing", lambda payment: payment.pending
    ) as (payment, token):
        assert payment is saved
        assert token is None
    assert pauses == [0.05, 0.1, 0.2, 0.4, 0.8, 1.0]
    assert sum(pauses) / 0.05 > store.find_by_id_for_update.await_count * 5
