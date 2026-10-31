import asyncio
from contextlib import asynccontextmanager
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from payment_processor.application import payment_leases
from payment_processor.application.errors import LeaseBusyError
from payment_processor.domain.payment import Payment, PaymentCurrency


@pytest.fixture
def payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Lease test",
        payment_metadata={},
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["processing", "webhook"])
async def test_busy_lease_has_bounded_wait_without_releasing_other_owner(
    monkeypatch: pytest.MonkeyPatch,
    payment: Payment,
    operation: str,
) -> None:
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = payment
    repository.claim_lease.return_value = False
    active = False

    @asynccontextmanager
    async def scope():
        nonlocal active
        active = True
        try:
            yield repository
        finally:
            active = False

    monkeypatch.setattr(payment_leases, "LEASE_WAIT_SECONDS", 0.02)
    monkeypatch.setattr(payment_leases, "LEASE_POLL_INTERVAL", 0.1)
    async with asyncio.timeout(1):
        with pytest.raises(LeaseBusyError):
            async with payment_leases.payment_lease(
                scope, payment.id, operation, lambda p: True
            ):
                pytest.fail("A busy lease must not be acquired")
    assert not active
    repository.claim_lease.assert_awaited_once()
    repository.release_lease.assert_not_awaited()


@pytest.mark.asyncio
async def test_waiting_lease_can_be_acquired_and_releases_only_its_token(
    monkeypatch: pytest.MonkeyPatch,
    payment: Payment,
) -> None:
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = payment
    repository.claim_lease.side_effect = [False, True]

    @asynccontextmanager
    async def scope():
        yield repository

    monkeypatch.setattr(payment_leases, "LEASE_POLL_INTERVAL", 0)
    async with payment_leases.payment_lease(
        scope, payment.id, "processing", lambda p: True
    ) as (loaded, token):
        assert loaded is payment
        assert token is not None
    assert repository.claim_lease.await_count == 2
    repository.release_lease.assert_awaited_once_with(payment.id, "processing", token)


@pytest.mark.asyncio
async def test_cancellation_during_lease_wait_does_not_release_other_owner(
    payment: Payment,
) -> None:
    entered = asyncio.Event()
    repository = AsyncMock()

    async def blocked_query(payment_id):
        entered.set()
        await asyncio.Event().wait()

    repository.find_by_id_for_update.side_effect = blocked_query

    @asynccontextmanager
    async def scope():
        yield repository

    async def acquire():
        async with payment_leases.payment_lease(
            scope, payment.id, "processing", lambda p: True
        ):
            pytest.fail("Cancelled lease must not be acquired")

    task = asyncio.create_task(acquire())
    try:
        async with asyncio.timeout(1):
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    repository.release_lease.assert_not_awaited()
