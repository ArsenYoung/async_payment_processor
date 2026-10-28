import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import copy
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from faststream.rabbit import RabbitBroker
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.db.models import Outbox
from payment_processor.db.session import Database
from payment_processor.messaging import topology
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.services.outbox_service import OutboxService, PublicationResult
from payment_processor.services.payment_service import PaymentService


@pytest_asyncio.fixture
async def recovery_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[RabbitBroker]:
    exchange = copy(topology.PAYMENTS_EXCHANGE)
    queue = copy(topology.PAYMENTS_NEW_QUEUE)
    prefix = f"test-recovery-{uuid4()}-"
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


@pytest_asyncio.fixture
async def recovery_events(
    outbox_database: Database, payment_payload: dict
) -> list[Outbox]:
    service = PaymentService(outbox_database.session_scope)
    request = PaymentCreateRequest.model_validate(payment_payload)
    for _ in range(3):
        await service.create_payment(request, f"test-recovery-{uuid4()}")
    return await _read_events(outbox_database)


@pytest.mark.asyncio
async def test_broker_failure_then_recovery_delivers_all_pending_events(
    outbox_database: Database,
    recovery_events: list[Outbox],
    recovery_broker: RabbitBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OutboxService(outbox_database.session_scope, recovery_broker)
    with monkeypatch.context() as patch:
        publish = AsyncMock(side_effect=ConnectionError("Broker unavailable"))
        patch.setattr(recovery_broker, "publish", publish)
        assert await service.publish_batch(10) == PublicationResult(0, 3)
        assert publish.await_count == 3

    for record in await _read_events(outbox_database):
        assert record.published_at is None
        assert record.attempts == 1
        assert record.last_error == "ConnectionError: Broker unavailable"
    await _assert_messages(recovery_broker, [])

    assert await service.publish_batch(10) == PublicationResult(3, 0)
    await _assert_messages(recovery_broker, recovery_events)
    for record in await _read_events(outbox_database):
        assert record.published_at is not None
        assert record.attempts == 2
        assert record.last_error is None
    assert await service.publish_batch(10) == PublicationResult(0, 0)
    await _assert_messages(recovery_broker, [])


@pytest.mark.asyncio
async def test_new_publisher_connection_resumes_only_unpublished_events(
    outbox_database: Database,
    recovery_events: list[Outbox],
    recovery_broker: RabbitBroker,
) -> None:
    first_broker = create_broker()
    try:
        await first_broker.connect()
        await topology.declare_payment_topology(first_broker)
        service = OutboxService(outbox_database.session_scope, first_broker)
        assert await service.publish_batch(1) == PublicationResult(1, 0)
    finally:
        await first_broker.stop()
    before_restart = await _read_events(outbox_database)

    await _publish_with_new_connection(outbox_database, expected_count=2)

    await _assert_messages(recovery_broker, recovery_events)
    records = await _read_events(outbox_database)
    assert records[0].published_at == before_restart[0].published_at
    assert all(record.published_at is not None for record in records)
    assert all(record.attempts == 1 for record in records)
    assert all(record.last_error is None for record in records)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["commit", "cancellation"])
async def test_restart_after_confirmed_publication_repeats_same_event_ids(
    outbox_database: Database,
    recovery_events: list[Outbox],
    recovery_broker: RabbitBroker,
    failure: str,
) -> None:
    error = OperationalError("COMMIT", {}, RuntimeError("Connection lost"))

    def fail_commit(session: object) -> None:
        raise error

    @asynccontextmanager
    async def failing_scope() -> AsyncIterator[AsyncSession]:
        async with outbox_database.session_scope() as session:
            if failure == "commit":
                event.listen(session.sync_session, "before_commit", fail_commit)
            yield session
            await session.flush()
            if failure == "cancellation":
                raise asyncio.CancelledError()

    service = OutboxService(failing_scope, recovery_broker)
    expected_error = OperationalError if failure == "commit" else asyncio.CancelledError
    with pytest.raises(expected_error):
        await service.publish_batch(10)

    await _assert_messages(recovery_broker, recovery_events)
    for record in await _read_events(outbox_database):
        assert record.published_at is None
        assert record.attempts == 0
        assert record.last_error is None

    async with asyncio.timeout(10):
        await _publish_with_new_connection(outbox_database, expected_count=3)

    await _assert_messages(recovery_broker, recovery_events)
    for record in await _read_events(outbox_database):
        assert record.published_at is not None
        assert record.attempts == 1
        assert record.last_error is None


async def _publish_with_new_connection(database: Database, expected_count: int) -> None:
    broker = create_broker()
    try:
        await broker.connect()
        await topology.declare_payment_topology(broker)
        service = OutboxService(database.session_scope, broker)
        assert await service.publish_batch(10) == PublicationResult(expected_count, 0)
        assert await service.publish_batch(10) == PublicationResult(0, 0)
    finally:
        await broker.stop()


async def _assert_messages(broker: RabbitBroker, expected: list[Outbox]) -> None:
    queue = await broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    for record in expected:
        message = await queue.get(timeout=5)
        async with message.process():
            assert message.message_id == str(record.id)
            assert PaymentCreatedEvent.model_validate_json(
                message.body
            ) == PaymentCreatedEvent.from_outbox(record)
    assert await queue.get(fail=False, timeout=5) is None


async def _read_events(database: Database) -> list[Outbox]:
    async with database.session_scope() as session:
        result = await session.scalars(
            select(Outbox).order_by(Outbox.created_at, Outbox.id)
        )
        return list(result.all())
