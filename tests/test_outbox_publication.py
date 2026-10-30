import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from copy import copy
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from aio_pika.exceptions import DeliveryError
from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic
from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError

from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.db.models import Outbox
from payment_processor.db.repositories.outbox import OutboxRepository
from payment_processor.db.session import Database
from payment_processor.messaging import topology
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.publisher import RabbitEventPublisher
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.services.outbox_service import OutboxService, PublicationResult
from payment_processor.services.payment_service import PaymentService


@pytest.fixture
def broker() -> Mock:
    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(return_value=Basic.Ack())
    return broker


@pytest_asyncio.fixture
async def outbox_events(
    outbox_database: Database, payment_payload: dict
) -> list[Outbox]:
    service = PaymentService(outbox_database.session_scope)
    request = PaymentCreateRequest.model_validate(payment_payload)
    for _ in range(3):
        await service.create_payment(request, f"test-publication-{uuid4()}")
    return await _read_events(outbox_database)


@pytest_asyncio.fixture
async def publication_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[RabbitBroker]:
    exchange = copy(topology.PAYMENTS_EXCHANGE)
    queue = copy(topology.PAYMENTS_NEW_QUEUE)
    prefix = f"test-publication-{uuid4()}-"
    exchange.name = prefix + exchange.name
    queue.name = prefix + queue.name
    monkeypatch.setattr(topology, "PAYMENTS_EXCHANGE", exchange)
    monkeypatch.setattr(topology, "PAYMENTS_NEW_QUEUE", queue)
    broker = create_broker()

    try:
        await broker.connect()
        declared_exchange = await broker.declare_exchange(exchange)
        try:
            declared_queue = await broker.declare_queue(queue)
            try:
                await topology.declare_payment_topology(broker)
                yield broker
            finally:
                await declared_queue.delete(if_unused=False, if_empty=False)
        finally:
            await declared_exchange.delete(if_unused=False)
    finally:
        await broker.stop()


@pytest.mark.asyncio
async def test_empty_batch_does_not_publish(
    outbox_database: Database, broker: Mock
) -> None:
    service = OutboxService(
        outbox_database.session_scope, RabbitEventPublisher(broker)
    )

    assert await service.publish_batch(10) == PublicationResult(published=0, failed=0)
    broker.publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, -1])
async def test_invalid_batch_limit_does_not_publish(
    outbox_database: Database, broker: Mock, limit: int
) -> None:
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))

    with pytest.raises(ValueError, match="must be positive"):
        await service.publish_batch(limit)

    broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_selected_batch_is_published(
    outbox_database: Database, outbox_events: list[Outbox], broker: Mock
) -> None:
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))

    assert await service.publish_batch(2) == PublicationResult(published=2, failed=0)

    events = await _read_events(outbox_database)
    assert [event.id for event in events] == [event.id for event in outbox_events]
    assert [event.attempts for event in events] == [1, 1, 0]
    assert [event.published_at is not None for event in events] == [True, True, False]
    assert broker.publish.await_count == 2


@pytest.mark.asyncio
async def test_confirmed_batch_reaches_rabbitmq_and_is_not_published_again(
    outbox_database: Database,
    outbox_events: list[Outbox],
    publication_broker: RabbitBroker,
) -> None:
    service = OutboxService(
        outbox_database.session_scope, RabbitEventPublisher(publication_broker)
    )

    assert await service.publish_batch(10) == PublicationResult(published=3, failed=0)

    queue = await publication_broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    for expected in outbox_events:
        message = await queue.get(timeout=5)
        async with message.process():
            event = PaymentCreatedEvent.model_validate_json(message.body)
            assert event == PaymentCreatedEvent.from_outbox(expected)
            assert message.message_id == str(expected.id)

    assert await service.publish_batch(10) == PublicationResult(published=0, failed=0)
    assert await queue.get(fail=False, timeout=5) is None
    for event in await _read_events(outbox_database):
        assert event.published_at is not None
        assert event.attempts == 1
        assert event.last_error is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        TimeoutError(),
        ConnectionError("Broker disconnected"),
        DeliveryError(None, Basic.Nack()),
    ],
)
async def test_mixed_batch_saves_failure_and_retries_only_unpublished_event(
    outbox_database: Database,
    outbox_events: list[Outbox],
    broker: Mock,
    error: Exception,
) -> None:
    broker.publish.side_effect = [Basic.Ack(), error, Basic.Ack()]
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))

    assert await service.publish_batch(10) == PublicationResult(published=2, failed=1)

    events = await _read_events(outbox_database)
    assert [event.published_at is not None for event in events] == [True, False, True]
    assert [event.attempts for event in events] == [1, 1, 1]
    assert events[0].last_error is None
    assert events[1].last_error == f"{type(error).__name__}: {error}"
    assert events[2].last_error is None
    assert broker.publish.await_count == 3

    broker.publish.side_effect = None
    broker.publish.reset_mock()
    assert await service.publish_batch(10) == PublicationResult(0, 0)
    async with outbox_database.session_scope() as session:
        await session.execute(
            update(Outbox)
            .where(Outbox.published_at.is_(None))
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    assert await service.publish_batch(10) == PublicationResult(published=1, failed=0)
    broker.publish.assert_awaited_once()
    assert broker.publish.await_args.args[0]["event_id"] == str(outbox_events[1].id)

    retried = await _read_events(outbox_database)
    assert [event.attempts for event in retried] == [1, 2, 1]
    assert all(event.published_at is not None for event in retried)
    assert all(event.last_error is None for event in retried)
    assert retried[0].published_at == events[0].published_at
    assert retried[2].published_at == events[2].published_at


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmation", [None, Basic.Nack(), Basic.Reject()])
async def test_unconfirmed_batch_remains_unpublished(
    outbox_database: Database,
    outbox_events: list[Outbox],
    broker: Mock,
    confirmation: object,
) -> None:
    broker.publish.return_value = confirmation
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))

    assert await service.publish_batch(10) == PublicationResult(published=0, failed=3)

    for event in await _read_events(outbox_database):
        assert event.published_at is None
        assert event.attempts == 1
        assert event.last_error == (
            "RuntimeError: RabbitMQ did not acknowledge payment event publication"
        )


@pytest.mark.asyncio
async def test_invalid_payload_is_recorded_without_blocking_other_events(
    outbox_database: Database, outbox_events: list[Outbox], broker: Mock
) -> None:
    invalid = outbox_events[0]
    async with outbox_database.session_scope() as session:
        session.add(invalid)
        invalid.payload = {**invalid.payload, "amount": "invalid"}
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))

    assert await service.publish_batch(10) == PublicationResult(published=2, failed=1)

    events = await _read_events(outbox_database)
    assert events[0].published_at is None
    assert events[0].attempts == 1
    assert events[0].last_error is not None
    assert events[0].last_error.startswith("ValidationError:")
    assert all(event.published_at is not None for event in events[1:])
    assert broker.publish.await_count == 2


@pytest.mark.asyncio
async def test_stalled_publication_times_out_and_saves_diagnostic(
    outbox_database: Database, outbox_events: list[Outbox], broker: Mock
) -> None:
    waiting = asyncio.Event()

    async def stalled(*args: object, **kwargs: object) -> None:
        await waiting.wait()

    broker.publish.side_effect = stalled
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))

    async with asyncio.timeout(2):
        result = await service.publish_batch(10, timeout=0.01)

    assert result == PublicationResult(published=0, failed=3)
    for event in await _read_events(outbox_database):
        assert event.published_at is None
        assert event.attempts == 1
        assert event.last_error == "TimeoutError: "


@pytest.mark.asyncio
async def test_rows_stay_locked_while_another_publisher_waits_for_confirmation(
    outbox_database: Database, outbox_events: list[Outbox], broker: Mock
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed(*args: object, **kwargs: object) -> Basic.Ack:
        entered.set()
        await release.wait()
        return Basic.Ack()

    broker.publish.side_effect = delayed
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))
    second_broker = Mock(spec=RabbitBroker)
    second_broker.publish = AsyncMock(return_value=Basic.Ack())
    second_service = OutboxService(
        outbox_database.session_scope, RabbitEventPublisher(second_broker)
    )
    task = asyncio.create_task(service.publish_batch(10))
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            for event in await _read_events(outbox_database):
                assert event.published_at is None
                assert event.attempts == 0
            assert await second_service.publish_batch(10) == PublicationResult(0, 0)
            second_broker.publish.assert_not_awaited()
            release.set()
            assert await task == PublicationResult(published=3, failed=0)
    finally:
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


@pytest.mark.asyncio
async def test_cancellation_rolls_back_batch_and_releases_locks(
    outbox_database: Database, outbox_events: list[Outbox], broker: Mock
) -> None:
    entered = asyncio.Event()
    waiting = asyncio.Event()

    async def cancel_second(message: dict, **kwargs: object) -> Basic.Ack:
        if UUID(message["event_id"]) == outbox_events[1].id:
            entered.set()
            await waiting.wait()
        return Basic.Ack()

    broker.publish.side_effect = cancel_second
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))
    task = asyncio.create_task(service.publish_batch(10))
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async with asyncio.timeout(2):
        async with outbox_database.session_scope() as session:
            events = await OutboxRepository(session).fetch_unpublished(10)
            assert [event.id for event in events] == [
                event.id for event in outbox_events
            ]
            assert all(event.attempts == 0 for event in events)
            assert all(event.last_error is None for event in events)
    assert broker.publish.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["published", "failed"])
async def test_database_error_propagates_and_rolls_back_entire_batch(
    outbox_database: Database,
    outbox_events: list[Outbox],
    broker: Mock,
    outcome: str,
) -> None:
    class FailingRepository(OutboxRepository):
        async def mark_published(self, event: Outbox) -> None:
            await super().mark_published(event)
            await self._fail_second(event)

        async def record_failure(
            self, event: Outbox, error: str, *, next_attempt_at: datetime | None = None
        ) -> None:
            await super().record_failure(event, error, next_attempt_at=next_attempt_at)
            await self._fail_second(event)

        async def _fail_second(self, event: Outbox) -> None:
            if event.id == outbox_events[1].id:
                event.attempts = -1
                await self.session.flush()

    if outcome == "failed":
        broker.publish.side_effect = [
            Basic.Ack(),
            ConnectionError("Broker disconnected"),
        ]
    service = OutboxService(
        outbox_database.session_scope, broker, repository_factory=FailingRepository
    )

    with pytest.raises(IntegrityError):
        await service.publish_batch(10)

    async with asyncio.timeout(2):
        async with outbox_database.session_scope() as session:
            events = await OutboxRepository(session).fetch_unpublished(10)
            assert [event.id for event in events] == [
                event.id for event in outbox_events
            ]
            assert all(event.attempts == 0 for event in events)
            assert all(event.last_error is None for event in events)
    assert broker.publish.await_count == 2


async def _read_events(database: Database) -> list[Outbox]:
    async with database.session_scope() as session:
        result = await session.scalars(
            select(Outbox).order_by(Outbox.created_at, Outbox.id)
        )
        return list(result.all())


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_kind", ["amount", "payment_id"])
async def test_full_invalid_batch_does_not_starve_valid_event(
    outbox_database: Database,
    outbox_events: list[Outbox],
    broker: Mock,
    invalid_kind: str,
) -> None:
    async with outbox_database.session_scope() as session:
        session.add_all(outbox_events[:2])
        for record in outbox_events[:2]:
            value = "invalid" if invalid_kind == "amount" else str(uuid4())
            record.payload = {**record.payload, invalid_kind: value}
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))
    assert await service.publish_batch(2) == PublicationResult(0, 2)
    broker.publish.assert_not_awaited()
    assert await service.publish_batch(2) == PublicationResult(1, 0)
    broker.publish.assert_awaited_once()
    records = await _read_events(outbox_database)
    assert all(record.quarantined_at is not None for record in records[:2])
    assert all(record.published_at is None for record in records[:2])
    assert records[2].published_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "attempts,delay", [(0, 1), (1, 2), (5, 32), (6, 60), (1000, 60)]
)
async def test_broker_failure_defers_without_quarantine(
    outbox_database: Database,
    outbox_events: list[Outbox],
    broker: Mock,
    attempts: int,
    delay: int,
) -> None:
    async with outbox_database.session_scope() as session:
        session.add_all(outbox_events)
        for record in outbox_events:
            record.attempts = attempts
    broker.publish.side_effect = ConnectionError("Broker unavailable")
    service = OutboxService(outbox_database.session_scope, RabbitEventPublisher(broker))
    before = datetime.now(UTC)
    assert await service.publish_batch(10) == PublicationResult(0, 3)
    after = datetime.now(UTC)
    assert await service.publish_batch(10) == PublicationResult(0, 0)
    for record in await _read_events(outbox_database):
        assert record.quarantined_at is None
        assert record.published_at is None
        assert record.attempts == attempts + 1
        assert before + timedelta(seconds=delay) <= record.next_attempt_at
        assert record.next_attempt_at <= after + timedelta(seconds=delay)
