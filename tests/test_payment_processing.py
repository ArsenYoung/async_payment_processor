import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy import event, func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.models import Payment, PaymentCurrency, PaymentStatus
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.db.session import Database
from payment_processor.services.payment_gateway import PaymentGatewaySimulator
from payment_processor.services.processing import PaymentProcessingService


@pytest_asyncio.fixture
async def pending_payment(outbox_database: Database) -> Payment:
    payment = Payment(
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Payment processing test",
        payment_metadata={"order_id": "processing-order"},
        idempotency_key=f"test-processing-{uuid4()}",
        webhook_url="https://example.com/webhook",
    )
    async with outbox_database.session_scope() as session:
        session.add(payment)
        await session.flush()
    return payment


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_processing_commits_gateway_result_before_returning(
    outbox_database: Database, pending_payment: Payment, status: PaymentStatus
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = PaymentProcessingService(outbox_database.session_scope, gateway)
    started_at = datetime.now(UTC)

    payment = await service.process_payment(pending_payment.id)

    gateway.process.assert_awaited_once_with()
    assert payment.id == pending_payment.id
    assert payment.status == status
    assert payment.processed_at is not None
    assert started_at <= payment.processed_at <= datetime.now(UTC)
    assert payment.processed_at.utcoffset().total_seconds() == 0
    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, payment.id)
        assert saved is not None
        assert saved.status == status
        assert saved.processed_at == payment.processed_at
        assert saved.amount == pending_payment.amount
        assert saved.currency == pending_payment.currency
        assert saved.description == pending_payment.description
        assert saved.payment_metadata == pending_payment.payment_metadata
        assert saved.webhook_url == pending_payment.webhook_url
        assert saved.idempotency_key == pending_payment.idempotency_key
        assert saved.created_at == pending_payment.created_at


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_repeated_processing_preserves_completed_result(
    outbox_database: Database, pending_payment: Payment, status: PaymentStatus
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    first = await PaymentProcessingService(
        outbox_database.session_scope, gateway
    ).process_payment(pending_payment.id)
    repeated_gateway = AsyncMock(spec=PaymentGatewaySimulator)
    service = PaymentProcessingService(outbox_database.session_scope, repeated_gateway)

    repeated = await service.process_payment(pending_payment.id)

    gateway.process.assert_awaited_once_with()
    repeated_gateway.process.assert_not_awaited()
    assert repeated.status == first.status == status
    assert repeated.processed_at == first.processed_at


@pytest.mark.asyncio
async def test_unknown_payment_raises_without_calling_gateway(
    outbox_database: Database,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    service = PaymentProcessingService(outbox_database.session_scope, gateway)
    payment_id = uuid4()

    with pytest.raises(LookupError, match=str(payment_id)):
        await service.process_payment(payment_id)

    gateway.process.assert_not_awaited()
    async with outbox_database.session_scope() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_concurrent_processing_waits_and_reuses_committed_result(
    outbox_database: Database, pending_payment: Payment, status: PaymentStatus
) -> None:
    gateway_entered = asyncio.Event()
    release_gateway = asyncio.Event()
    waiter_started = asyncio.Event()
    waiter_pid: int | None = None

    async def process() -> PaymentStatus:
        gateway_entered.set()
        await release_gateway.wait()
        return status

    class WaitingRepository(PaymentRepository):
        async def find_by_id_for_update(self, payment_id: UUID) -> Payment | None:
            nonlocal waiter_pid
            waiter_pid = await self.session.scalar(select(func.pg_backend_pid()))
            waiter_started.set()
            return await super().find_by_id_for_update(payment_id)

    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.side_effect = process
    first_service = PaymentProcessingService(outbox_database.session_scope, gateway)
    second_gateway = AsyncMock(spec=PaymentGatewaySimulator)
    second_service = PaymentProcessingService(
        outbox_database.session_scope, second_gateway, WaitingRepository
    )
    tasks = [asyncio.create_task(first_service.process_payment(pending_payment.id))]
    try:
        async with asyncio.timeout(10):
            await gateway_entered.wait()
            tasks.append(
                asyncio.create_task(second_service.process_payment(pending_payment.id))
            )
            await waiter_started.wait()
            assert waiter_pid is not None
            async with outbox_database.session_scope() as session:
                while not await session.scalar(
                    select(func.pg_blocking_pids(waiter_pid))
                ):
                    await asyncio.sleep(0.01)
                saved = await session.get(Payment, pending_payment.id)
                assert saved is not None
                assert saved.status == PaymentStatus.PENDING
                assert saved.processed_at is None
            second_gateway.process.assert_not_awaited()
            release_gateway.set()
            first, second = await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    gateway.process.assert_awaited_once_with()
    second_gateway.process.assert_not_awaited()
    assert first.status == second.status == status
    assert first.processed_at == second.processed_at
    assert first.processed_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["gateway", "commit", "cancellation"])
async def test_processing_failure_rolls_back_and_allows_retry(
    outbox_database: Database, pending_payment: Payment, failure: str
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = PaymentStatus.SUCCEEDED
    error: Exception | asyncio.CancelledError
    if failure == "commit":
        error = OperationalError("COMMIT", {}, RuntimeError("Connection lost"))
    elif failure == "cancellation":
        error = asyncio.CancelledError()
        gateway.process.side_effect = error
    else:
        error = RuntimeError("Gateway unavailable")
        gateway.process.side_effect = error

    def fail_commit(session: object) -> None:
        raise error

    @asynccontextmanager
    async def session_scope() -> AsyncIterator[AsyncSession]:
        async with outbox_database.session_scope() as session:
            if failure == "commit":
                await session.connection()
                event.listen(session.sync_session, "before_commit", fail_commit)
            yield session
            if failure == "commit":
                await session.flush()

    service = PaymentProcessingService(session_scope, gateway)
    with pytest.raises(type(error)) as caught:
        await service.process_payment(pending_payment.id)
    assert caught.value is error

    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, pending_payment.id)
        assert saved is not None
        assert saved.status == PaymentStatus.PENDING
        assert saved.processed_at is None

    gateway.process.side_effect = None
    result = await PaymentProcessingService(
        outbox_database.session_scope, gateway
    ).process_payment(pending_payment.id)
    assert result.status == PaymentStatus.SUCCEEDED
    assert result.processed_at is not None
    assert gateway.process.await_count == 2
