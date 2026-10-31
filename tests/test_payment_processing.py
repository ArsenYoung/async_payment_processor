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

from payment_processor.adapters.payment_gateway import PaymentGatewaySimulator
from payment_processor.bootstrap import create_payment_service
from payment_processor.db.models import Payment, PaymentCurrency, PaymentStatus
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.db.session import Database
from payment_processor.domain.payment import Payment as DomainPayment


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
    service = create_payment_service(outbox_database.session_scope, gateway=gateway)
    started_at = datetime.now(UTC)

    payment = await service.process_payment(pending_payment.id)

    gateway.process.assert_awaited_once_with(idempotency_key=pending_payment.id)
    assert payment.payment_id == pending_payment.id
    assert payment.status == status
    assert payment.processed_at is not None
    assert started_at <= payment.processed_at <= datetime.now(UTC)
    assert payment.processed_at.utcoffset().total_seconds() == 0
    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, payment.payment_id)
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
    first = await create_payment_service(
        outbox_database.session_scope, gateway=gateway
    ).process_payment(pending_payment.id)
    repeated_gateway = AsyncMock(spec=PaymentGatewaySimulator)
    service = create_payment_service(
        outbox_database.session_scope, gateway=repeated_gateway
    )

    repeated = await service.process_payment(pending_payment.id)

    gateway.process.assert_awaited_once_with(idempotency_key=pending_payment.id)
    repeated_gateway.process.assert_not_awaited()
    assert repeated.status == first.status == status
    assert repeated.processed_at == first.processed_at


@pytest.mark.asyncio
async def test_unknown_payment_raises_without_calling_gateway(
    outbox_database: Database,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    service = create_payment_service(outbox_database.session_scope, gateway=gateway)
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

    async def process(*, idempotency_key: UUID) -> PaymentStatus:
        assert idempotency_key == pending_payment.id
        gateway_entered.set()
        await release_gateway.wait()
        return status

    class WaitingRepository(PaymentRepository):
        async def find_by_id_for_update(self, payment_id: UUID) -> DomainPayment | None:
            nonlocal waiter_pid
            waiter_pid = await self.session.scalar(select(func.pg_backend_pid()))
            waiter_started.set()
            return await super().find_by_id_for_update(payment_id)

    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.side_effect = process
    first_service = create_payment_service(
        outbox_database.session_scope, gateway=gateway
    )
    second_gateway = AsyncMock(spec=PaymentGatewaySimulator)
    second_service = create_payment_service(
        outbox_database.session_scope, WaitingRepository, gateway=second_gateway
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
                assert (
                    await session.scalar(select(func.pg_blocking_pids(waiter_pid)))
                    == []
                )
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

    gateway.process.assert_awaited_once_with(idempotency_key=pending_payment.id)
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

    scopes = 0

    @asynccontextmanager
    async def session_scope() -> AsyncIterator[AsyncSession]:
        nonlocal scopes
        scopes += 1
        async with outbox_database.session_scope() as session:
            if failure == "commit" and scopes == 2:
                await session.connection()
                event.listen(session.sync_session, "before_commit", fail_commit)
            yield session
            if failure == "commit" and scopes == 2:
                await session.flush()

    service = create_payment_service(session_scope, gateway=gateway)
    with pytest.raises(type(error)) as caught:
        await service.process_payment(pending_payment.id)
    assert caught.value is error

    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, pending_payment.id)
        assert saved is not None
        assert saved.status == PaymentStatus.PENDING
        assert saved.processed_at is None

    gateway.process.side_effect = None
    result = await create_payment_service(
        outbox_database.session_scope, gateway=gateway
    ).process_payment(pending_payment.id)
    assert result.status == PaymentStatus.SUCCEEDED
    assert result.processed_at is not None
    assert gateway.process.await_count == 2


@pytest.mark.asyncio
async def test_expired_processing_lease_can_be_reclaimed_and_fences_old_owner(
    outbox_database: Database, pending_payment: Payment
) -> None:
    from datetime import timedelta

    from payment_processor.application.errors import LeaseLostError

    old_token, new_token = uuid4(), uuid4()
    async with outbox_database.session_scope() as session:
        repository = PaymentRepository(session)
        assert await repository.claim_lease(
            pending_payment.id, "processing", old_token, 60
        )
    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, pending_payment.id)
        saved.processing_until = datetime.now(UTC) - timedelta(seconds=1)
    async with outbox_database.session_scope() as session:
        repository = PaymentRepository(session)
        assert await repository.claim_lease(
            pending_payment.id, "processing", new_token, 60
        )
        await repository.release_lease(pending_payment.id, "processing", old_token)
        await repository.verify_lease(pending_payment.id, "processing", new_token)
    with pytest.raises(LeaseLostError):
        async with outbox_database.session_scope() as session:
            await PaymentRepository(session).verify_lease(
                pending_payment.id, "processing", old_token
            )


@pytest.mark.asyncio
async def test_parallel_gateway_calls_release_a_single_connection_pool(
    outbox_database: Database, pending_payment: Payment
) -> None:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine(
        outbox_database.engine.url, pool_size=1, max_overflow=0, pool_timeout=1
    ).execution_options(**outbox_database.engine.get_execution_options())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    count = 8
    ids = [pending_payment.id]
    async with outbox_database.session_scope() as session:
        for _ in range(count - 1):
            model = Payment(
                amount=pending_payment.amount,
                currency=pending_payment.currency,
                description="Parallel processing",
                payment_metadata={},
                idempotency_key=str(uuid4()),
                webhook_url=pending_payment.webhook_url,
            )
            session.add(model)
            await session.flush()
            ids.append(model.id)
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    @asynccontextmanager
    async def scope():
        async with sessions.begin() as session:
            yield session

    async def process(*, idempotency_key: UUID) -> PaymentStatus:
        nonlocal calls
        assert idempotency_key in ids
        calls += 1
        if calls == count:
            entered.set()
        await release.wait()
        return PaymentStatus.SUCCEEDED

    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.side_effect = process
    service = create_payment_service(scope, gateway=gateway)
    tasks = [
        asyncio.create_task(service.process_payment(identifier)) for identifier in ids
    ]
    try:
        async with asyncio.timeout(10):
            await entered.wait()
            assert engine.pool.checkedout() == 0
            async with scope() as session:
                rows = await session.scalars(
                    select(Payment)
                    .where(Payment.id.in_(ids))
                    .with_for_update(nowait=True)
                )
                assert len(rows.all()) == count
            release.set()
            results = await asyncio.gather(*tasks)
            assert all(result.status == PaymentStatus.SUCCEEDED for result in results)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await engine.dispose()
