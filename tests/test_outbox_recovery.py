import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import copy
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from faststream.rabbit import RabbitBroker
from sqlalchemy import event, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.application.errors import PersistenceUnavailableError
from payment_processor.bootstrap import create_outbox_service, create_payment_service
from payment_processor.db.models import Outbox
from payment_processor.db.session import Database
from payment_processor.messaging import topology
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.publisher import RabbitEventPublisher
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.services.outbox_service import PublicationResult


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
    service = create_payment_service(outbox_database.session_scope)
    request = PaymentCreateRequest.model_validate(payment_payload)
    for _ in range(3):
        await service.create_payment(request, f"test-recovery-{uuid4()}")
    return await _read_events(outbox_database)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_broker_failure_then_recovery_delivers_all_pending_events(
    outbox_database: Database,
    recovery_events: list[Outbox],
    recovery_broker: RabbitBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = create_outbox_service(
        outbox_database.session_scope, RabbitEventPublisher(recovery_broker)
    )
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
    assert await service.publish_batch(10) == PublicationResult(0, 0)
    async with outbox_database.session_scope() as session:
        await session.execute(
            update(Outbox)
            .where(Outbox.published_at.is_(None))
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        )

    assert await service.publish_batch(10) == PublicationResult(3, 0)
    await _assert_messages(recovery_broker, recovery_events)
    for record in await _read_events(outbox_database):
        assert record.published_at is not None
        assert record.attempts == 2
        assert record.last_error is None
    assert await service.publish_batch(10) == PublicationResult(0, 0)
    await _assert_messages(recovery_broker, [])


@pytest.mark.integration
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
        service = create_outbox_service(
            outbox_database.session_scope, RabbitEventPublisher(first_broker)
        )
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


@pytest.mark.integration
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

    scopes = 0

    @asynccontextmanager
    async def failing_scope() -> AsyncIterator[AsyncSession]:
        nonlocal scopes
        scopes += 1
        async with outbox_database.session_scope() as session:
            if failure == "commit" and scopes == 2:
                event.listen(session.sync_session, "before_commit", fail_commit)
            yield session
            await session.flush()
            if failure == "cancellation" and scopes == 2:
                raise asyncio.CancelledError()

    service = create_outbox_service(
        failing_scope, RabbitEventPublisher(recovery_broker)
    )
    expected_error = (
        PersistenceUnavailableError if failure == "commit" else asyncio.CancelledError
    )
    with pytest.raises(expected_error) as caught:
        await service.publish_batch(10)
    if failure == "commit":
        assert caught.value.__cause__ is error

    await _assert_messages(recovery_broker, recovery_events[:1])
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
        service = create_outbox_service(
            database.session_scope, RabbitEventPublisher(broker)
        )
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_operator_recovers_only_selected_event_with_same_identity(
    outbox_database: Database,
    recovery_events: list[Outbox],
    recovery_broker: RabbitBroker,
) -> None:
    from payment_processor.operations.outbox_recovery import recover_event

    original = recovery_events[0].payload.copy()
    async with outbox_database.session_scope() as session:
        session.add_all(recovery_events)
        for record in recovery_events:
            record.payload = {**record.payload, "amount": "invalid"}
    service = create_outbox_service(
        outbox_database.session_scope, RabbitEventPublisher(recovery_broker)
    )
    assert await service.publish_batch(10) == PublicationResult(0, 3)
    await recover_event(outbox_database, recovery_events[0].id, payload=original)
    records = await _read_events(outbox_database)
    assert records[0].quarantined_at is None
    assert records[0].next_attempt_at is None
    assert records[0].published_at is None
    assert records[0].attempts == 1
    assert records[0].last_error is not None
    assert all(record.quarantined_at is not None for record in records[1:])
    assert await service.publish_batch(10) == PublicationResult(1, 0)
    await _assert_messages(recovery_broker, [records[0]])
    records = await _read_events(outbox_database)
    assert records[0].attempts == 2
    assert records[0].last_error is None


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("payload_kind", ["invalid", "wrong_payment", "stored_invalid"])
async def test_invalid_repair_keeps_quarantine_and_original_payload(
    outbox_database: Database,
    recovery_events: list[Outbox],
    payload_kind: str,
) -> None:
    from payment_processor.db.repositories.outbox import OutboxRepository
    from payment_processor.operations.outbox_recovery import recover_event

    saved = recovery_events[0]
    async with outbox_database.session_scope() as session:
        session.add(saved)
        saved.payload = {**saved.payload, "amount": "invalid"}
        await OutboxRepository(session).quarantine(saved, "Invalid amount")
    replacement = None
    if payload_kind == "invalid":
        replacement = {**saved.payload, "amount": "still-invalid"}
    elif payload_kind == "wrong_payment":
        replacement = {**saved.payload, "amount": "10.50", "payment_id": str(uuid4())}
    with pytest.raises(ValueError):
        await recover_event(outbox_database, saved.id, payload=replacement)
    [record, *_] = await _read_events(outbox_database)
    assert record.payload == saved.payload
    assert record.quarantined_at == saved.quarantined_at
    assert record.attempts == 1
    assert record.last_error == "Invalid amount"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recovery_rejects_missing_pending_and_published_events(
    outbox_database: Database,
    recovery_events: list[Outbox],
) -> None:
    from payment_processor.db.repositories.outbox import OutboxRepository
    from payment_processor.operations.outbox_recovery import recover_event

    with pytest.raises(LookupError):
        await recover_event(outbox_database, uuid4())
    with pytest.raises(ValueError):
        await recover_event(outbox_database, recovery_events[0].id)
    async with outbox_database.session_scope() as session:
        session.add(recovery_events[0])
        await OutboxRepository(session).mark_published(recovery_events[0])
    with pytest.raises(ValueError):
        await recover_event(outbox_database, recovery_events[0].id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_recovery_releases_event_only_once(
    outbox_database: Database,
    recovery_events: list[Outbox],
) -> None:
    from payment_processor.db.repositories.outbox import OutboxRepository
    from payment_processor.operations.outbox_recovery import recover_event

    async with outbox_database.session_scope() as session:
        session.add(recovery_events[0])
        await OutboxRepository(session).quarantine(recovery_events[0], "Old error")
    async with asyncio.timeout(5):
        results = await asyncio.gather(
            recover_event(outbox_database, recovery_events[0].id),
            recover_event(outbox_database, recovery_events[0].id),
            return_exceptions=True,
        )
    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, ValueError) for result in results) == 1


@pytest.mark.unit
def test_recovery_cli_validates_json_before_connecting(tmp_path, monkeypatch):
    from payment_processor.operations import outbox_recovery

    payload_file = tmp_path / "payload.json"
    payload_file.write_text("[]")
    run = AsyncMock()
    monkeypatch.setattr(outbox_recovery, "_run", run)
    monkeypatch.setattr(
        "sys.argv",
        [
            "outbox_recovery",
            "--event-id",
            str(uuid4()),
            "--payload-file",
            str(payload_file),
        ],
    )
    with pytest.raises(SystemExit) as error:
        outbox_recovery.main()
    assert error.value.code == 2
    run.assert_not_awaited()
