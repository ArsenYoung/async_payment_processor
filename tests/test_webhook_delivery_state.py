import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import event, func, select
from sqlalchemy.exc import OperationalError

from payment_processor.application.results import PaymentDetailsResult
from payment_processor.db.models import Payment, PaymentCurrency, PaymentStatus
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.db.session import Database
from payment_processor.domain.payment import Payment as DomainPayment
from payment_processor.services.errors import WebhookRequestTimeoutError
from payment_processor.services.payment_service import PaymentService
from payment_processor.services.webhooks import WebhookClient


@pytest_asyncio.fixture
async def completed_payment(outbox_database: Database) -> Payment:
    payment = Payment(
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Webhook delivery state test",
        payment_metadata={"order_id": "delivery-order"},
        idempotency_key=f"test-delivery-{uuid4()}",
        webhook_url="https://example.com/webhook",
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime(2026, 10, 28, 10, 0, 3, tzinfo=UTC),
    )
    async with outbox_database.session_scope() as session:
        session.add(payment)
        await session.flush()
    return payment


@pytest.mark.asyncio
@pytest.mark.parametrize("status", list(PaymentStatus))
async def test_payment_starts_without_delivery_mark(
    outbox_database: Database, completed_payment: Payment, status: PaymentStatus
) -> None:
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, completed_payment.id)
        assert payment is not None
        payment.status = status
        if status == PaymentStatus.PENDING:
            payment.processed_at = None

    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, completed_payment.id)
        assert saved is not None
        assert saved.webhook_delivered_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_delivery_mark_is_committed_without_changing_payment_result(
    outbox_database: Database, completed_payment: Payment, status: PaymentStatus
) -> None:
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, completed_payment.id)
        assert payment is not None
        payment.status = status

    started_at = datetime.now(UTC)
    async with outbox_database.session_scope() as session:
        repository = PaymentRepository(session)
        payment = await repository.find_by_id_for_update(completed_payment.id)
        assert payment is not None
        await repository.mark_webhook_delivered(payment)
        delivered_at = payment.webhook_delivered_at
        assert delivered_at is not None
        assert started_at <= delivered_at <= datetime.now(UTC)
        assert delivered_at.utcoffset().total_seconds() == 0
        async with outbox_database.session_scope() as observer:
            uncommitted = await observer.get(Payment, payment.id)
            assert uncommitted is not None
            assert uncommitted.webhook_delivered_at is None

    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, completed_payment.id)
        assert saved is not None
        assert saved.webhook_delivered_at == delivered_at
        assert saved.status == status
        assert saved.processed_at == completed_payment.processed_at
        assert saved.created_at == completed_payment.created_at
        assert saved.amount == completed_payment.amount
        assert saved.currency == completed_payment.currency
        assert saved.description == completed_payment.description
        assert saved.payment_metadata == completed_payment.payment_metadata
        assert saved.idempotency_key == completed_payment.idempotency_key
        assert saved.webhook_url == completed_payment.webhook_url


@pytest.mark.asyncio
async def test_repeated_delivery_mark_preserves_original_time(
    outbox_database: Database, completed_payment: Payment
) -> None:
    original_time = datetime(2026, 10, 28, 10, 0, 5, tzinfo=UTC)
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, completed_payment.id)
        assert payment is not None
        payment.webhook_delivered_at = original_time

    async with outbox_database.session_scope() as session:
        repository = PaymentRepository(session)
        payment = await repository.find_by_id_for_update(completed_payment.id)
        assert payment is not None
        await repository.mark_webhook_delivered(payment)

    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, completed_payment.id)
        assert saved is not None
        assert saved.webhook_delivered_at == original_time


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "processed_at"),
    [
        (PaymentStatus.PENDING, None),
        (PaymentStatus.PENDING, datetime(2026, 10, 28, tzinfo=UTC)),
        (PaymentStatus.SUCCEEDED, None),
        (PaymentStatus.FAILED, None),
    ],
)
async def test_incomplete_payment_cannot_be_marked_delivered(
    outbox_database: Database,
    completed_payment: Payment,
    status: PaymentStatus,
    processed_at: datetime | None,
) -> None:
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, completed_payment.id)
        assert payment is not None
        payment.status = status
        payment.processed_at = processed_at

    with pytest.raises(ValueError, match="requires a completed payment"):
        async with outbox_database.session_scope() as session:
            repository = PaymentRepository(session)
            payment = await repository.find_by_id_for_update(completed_payment.id)
            assert payment is not None
            await repository.mark_webhook_delivered(payment)

    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, completed_payment.id)
        assert saved is not None
        assert saved.webhook_delivered_at is None
        assert saved.status == status
        assert saved.processed_at == processed_at


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transaction", "commit", "cancellation"])
async def test_failed_transaction_rolls_back_delivery_mark_and_allows_retry(
    outbox_database: Database, completed_payment: Payment, failure: str
) -> None:
    error: BaseException = RuntimeError("Delivery transaction aborted")
    if failure == "commit":
        error = OperationalError("COMMIT", {}, RuntimeError("Connection lost"))
    elif failure == "cancellation":
        error = asyncio.CancelledError()

    def fail_commit(session: object) -> None:
        raise error

    with pytest.raises(type(error)) as caught:
        async with outbox_database.session_scope() as session:
            repository = PaymentRepository(session)
            payment = await repository.find_by_id_for_update(completed_payment.id)
            assert payment is not None
            await repository.mark_webhook_delivered(payment)
            if failure == "commit":
                event.listen(session.sync_session, "before_commit", fail_commit)
            else:
                raise error
    assert caught.value is error

    async with outbox_database.session_scope() as session:
        repository = PaymentRepository(session)
        saved = await repository.find_by_id_for_update(completed_payment.id)
        assert saved is not None
        assert saved.webhook_delivered_at is None
        assert saved.status == completed_payment.status
        assert saved.processed_at == completed_payment.processed_at
        await repository.mark_webhook_delivered(saved)

    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, completed_payment.id)
        assert saved is not None
        assert saved.webhook_delivered_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
@pytest.mark.parametrize("failure", ["http", "network", "timeout", "deadline"])
async def test_webhook_failure_preserves_result_without_marking_delivery(
    outbox_database: Database,
    completed_payment: Payment,
    status: PaymentStatus,
    failure: str,
) -> None:
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, completed_payment.id)
        assert payment is not None
        payment.status = status

    def handle(request: httpx.Request) -> httpx.Response:
        if failure == "network":
            raise httpx.ConnectError("Connection lost", request=request)
        if failure == "timeout":
            raise httpx.ReadTimeout("Webhook timed out", request=request)
        if failure == "deadline":
            raise WebhookRequestTimeoutError("Webhook request deadline exceeded")
        return httpx.Response(503)

    async with WebhookClient(
        timeout=5.0, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises((httpx.HTTPError, WebhookRequestTimeoutError)):
            async with outbox_database.session_scope() as session:
                repository = PaymentRepository(session)
                payment = await repository.find_by_id_for_update(completed_payment.id)
                assert payment is not None
                await client.send(PaymentDetailsResult.from_payment(payment))
                await repository.mark_webhook_delivered(payment)

    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, completed_payment.id)
        assert saved is not None
        assert saved.webhook_delivered_at is None
        assert saved.status == status
        assert saved.processed_at == completed_payment.processed_at


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_concurrent_delivery_waits_and_skips_committed_webhook(
    outbox_database: Database, completed_payment: Payment, status: PaymentStatus
) -> None:
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, completed_payment.id)
        assert payment is not None
        payment.status = status

    webhook_entered = asyncio.Event()
    release_webhook = asyncio.Event()
    waiter_started = asyncio.Event()
    waiter_pid: int | None = None

    async def handle(request: httpx.Request) -> httpx.Response:
        webhook_entered.set()
        await release_webhook.wait()
        return httpx.Response(204)

    class WaitingRepository(PaymentRepository):
        async def find_by_id_for_update(self, payment_id: UUID) -> DomainPayment | None:
            nonlocal waiter_pid
            waiter_pid = await self.session.scalar(select(func.pg_backend_pid()))
            waiter_started.set()
            return await super().find_by_id_for_update(payment_id)

    first_service = PaymentService(outbox_database.session_scope)
    second_service = PaymentService(outbox_database.session_scope, WaitingRepository)
    transport_handle = AsyncMock(side_effect=handle)
    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(transport_handle)
    ) as webhook:
        tasks = [
            asyncio.create_task(
                first_service.deliver_webhook(completed_payment.id, webhook)
            )
        ]
        try:
            async with asyncio.timeout(10):
                await webhook_entered.wait()
                tasks.append(
                    asyncio.create_task(
                        second_service.deliver_webhook(completed_payment.id, webhook)
                    )
                )
                await waiter_started.wait()
                assert waiter_pid is not None
                async with outbox_database.session_scope() as session:
                    while not await session.scalar(
                        select(func.pg_blocking_pids(waiter_pid))
                    ):
                        await asyncio.sleep(0.01)
                    saved = await session.get(Payment, completed_payment.id)
                    assert saved is not None
                    assert saved.status == status
                    assert saved.processed_at == completed_payment.processed_at
                    assert saved.webhook_delivered_at is None
                transport_handle.assert_awaited_once()
                release_webhook.set()
                await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    transport_handle.assert_awaited_once()
    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, completed_payment.id)
        assert saved is not None
        assert saved.status == status
        assert saved.processed_at == completed_payment.processed_at
        assert saved.created_at == completed_payment.created_at
        assert saved.webhook_delivered_at is not None
