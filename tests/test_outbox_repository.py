import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from payment_processor.db.models import (
    Outbox,
    OutboxEventType,
    Payment,
    PaymentCurrency,
)
from payment_processor.db.repositories.outbox import OutboxRepository
from payment_processor.db.session import Database


@pytest.mark.asyncio
async def test_empty_outbox_returns_no_events(outbox_database: Database) -> None:
    async with outbox_database.session_scope() as session:
        assert await OutboxRepository(session).fetch_unpublished(10) == []


@pytest.mark.asyncio
async def test_fetch_excludes_published_events_and_orders_oldest_first(
    outbox_database: Database,
) -> None:
    events = await _save_events(outbox_database, 4)
    async with outbox_database.session_scope() as session:
        session.add(events[0])
        events[0].published_at = datetime.now(UTC)

    async with outbox_database.session_scope() as session:
        batch = await OutboxRepository(session).fetch_unpublished(10)

        assert [event.id for event in batch] == [event.id for event in events[1:]]
        assert all(event.published_at is None for event in batch)
        assert all(event.attempts == 0 for event in batch)


@pytest.mark.asyncio
async def test_batch_limit_and_id_order_for_equal_creation_times(
    outbox_database: Database,
) -> None:
    events = await _save_events(outbox_database, 4)
    created_at = events[0].created_at
    async with outbox_database.session_scope() as session:
        session.add_all(events)
        for event in events:
            event.created_at = created_at

    async with outbox_database.session_scope() as session:
        batch = await OutboxRepository(session).fetch_unpublished(2)

        assert [event.id for event in batch] == sorted(event.id for event in events)[:2]


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, -1])
async def test_non_positive_batch_limit_is_rejected(
    outbox_database: Database, limit: int
) -> None:
    async with outbox_database.session_scope() as session:
        with pytest.raises(ValueError, match="must be positive"):
            await OutboxRepository(session).fetch_unpublished(limit)


@pytest.mark.asyncio
async def test_fetch_skips_quarantine_and_future_retries(
    outbox_database: Database,
) -> None:
    events = await _save_events(outbox_database, 4)
    now = datetime.now(UTC)
    async with outbox_database.session_scope() as session:
        session.add_all(events)
        await OutboxRepository(session).quarantine(events[0], "Invalid payload")
        await OutboxRepository(session).record_failure(
            events[1], "Broker unavailable", next_attempt_at=now + timedelta(hours=1)
        )
        events[2].next_attempt_at = now - timedelta(seconds=1)

    async with outbox_database.session_scope() as session:
        batch = await OutboxRepository(session).fetch_unpublished(10)
        assert [event.id for event in batch] == [event.id for event in events[2:]]
        quarantined = await session.get(Outbox, events[0].id)
        assert quarantined is not None
        assert quarantined.quarantined_at is not None
        assert quarantined.last_error == "Invalid payload"
        assert quarantined.attempts == 1
        assert quarantined.published_at is None
        assert quarantined.payload == events[0].payload


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["deferred", "quarantined"])
async def test_rollback_restores_recovery_state(
    outbox_database: Database, outcome: str
) -> None:
    [saved] = await _save_events(outbox_database, 1)
    with pytest.raises(RuntimeError, match="Abort recovery"):
        async with outbox_database.session_scope() as session:
            repository = OutboxRepository(session)
            [record] = await repository.fetch_unpublished(1)
            if outcome == "deferred":
                await repository.record_failure(
                    record,
                    "Unavailable",
                    next_attempt_at=datetime.now(UTC) + timedelta(hours=1),
                )
            else:
                await repository.quarantine(record, "Invalid payload")
            raise RuntimeError("Abort recovery")
    async with outbox_database.session_scope() as session:
        [record] = await OutboxRepository(session).fetch_unpublished(1)
        assert record.id == saved.id
        assert record.attempts == 0
        assert record.last_error is None
        assert record.quarantined_at is None
        assert record.next_attempt_at is None


@pytest.mark.asyncio
async def test_parallel_publishers_lock_disjoint_batches_until_commit(
    outbox_database: Database,
) -> None:
    events = await _save_events(outbox_database, 4)
    async with (
        outbox_database.session_scope() as first_session,
        outbox_database.session_scope() as second_session,
    ):
        first_pid = await first_session.scalar(select(func.pg_backend_pid()))
        second_pid = await second_session.scalar(select(func.pg_backend_pid()))
        assert first_pid != second_pid

        async with asyncio.timeout(2):
            first, second = await asyncio.gather(
                OutboxRepository(first_session).fetch_unpublished(2),
                OutboxRepository(second_session).fetch_unpublished(2),
            )
            async with outbox_database.session_scope() as observer:
                assert await OutboxRepository(observer).fetch_unpublished(10) == []

        first_ids = {event.id for event in first}
        second_ids = {event.id for event in second}
        assert len(first_ids) == len(second_ids) == 2
        assert first_ids.isdisjoint(second_ids)
        assert first_ids | second_ids == {event.id for event in events}

    async with outbox_database.session_scope() as session:
        batch = await OutboxRepository(session).fetch_unpublished(10)
        assert [event.id for event in batch] == [event.id for event in events]


@pytest.mark.asyncio
async def test_mark_published_persists_success_and_clears_previous_error(
    outbox_database: Database,
) -> None:
    [saved_event] = await _save_events(outbox_database, 1)
    async with outbox_database.session_scope() as session:
        session.add(saved_event)
        saved_event.attempts = 2
        saved_event.last_error = "Previous publication failed"

    before = datetime.now(UTC)
    async with outbox_database.session_scope() as session:
        repository = OutboxRepository(session)
        [event] = await repository.fetch_unpublished(1)
        await repository.mark_published(event)
        assert session.in_transaction()

        async with asyncio.timeout(2):
            async with outbox_database.session_scope() as observer:
                visible_event = await observer.get(Outbox, saved_event.id)
                assert visible_event is not None
                assert visible_event.published_at is None
                assert visible_event.attempts == 2
                assert visible_event.last_error == "Previous publication failed"
                assert await OutboxRepository(observer).fetch_unpublished(1) == []

    after = datetime.now(UTC)
    async with outbox_database.session_scope() as session:
        event = await session.get(Outbox, saved_event.id)
        assert event is not None
        assert event.published_at is not None
        assert before <= event.published_at <= after
        assert event.attempts == 3
        assert event.last_error is None
        assert event.payload == saved_event.payload
        assert event.created_at == saved_event.created_at
        assert await OutboxRepository(session).fetch_unpublished(1) == []


@pytest.mark.asyncio
async def test_record_failure_persists_error_and_keeps_event_available_for_retry(
    outbox_database: Database,
) -> None:
    [saved_event] = await _save_events(outbox_database, 1)
    for attempt, error in enumerate(
        ["Broker unavailable", "Confirmation timed out"], 1
    ):
        async with outbox_database.session_scope() as session:
            repository = OutboxRepository(session)
            [event] = await repository.fetch_unpublished(1)
            await repository.record_failure(event, error)
            assert session.in_transaction()

            async with asyncio.timeout(2):
                async with outbox_database.session_scope() as observer:
                    assert await OutboxRepository(observer).fetch_unpublished(1) == []

        async with outbox_database.session_scope() as session:
            [event] = await OutboxRepository(session).fetch_unpublished(1)
            assert event.id == saved_event.id
            assert event.published_at is None
            assert event.attempts == attempt
            assert event.last_error == error
            assert event.payload == saved_event.payload
            assert event.created_at == saved_event.created_at


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["selected", "published", "failed"])
async def test_rollback_restores_event_and_releases_lock(
    outbox_database: Database, outcome: str
) -> None:
    [saved_event] = await _save_events(outbox_database, 1)
    async with outbox_database.session_scope() as session:
        session.add(saved_event)
        saved_event.attempts = 2
        saved_event.last_error = "Earlier failure"

    with pytest.raises(RuntimeError, match="Abort publisher transaction"):
        async with outbox_database.session_scope() as session:
            repository = OutboxRepository(session)
            [event] = await repository.fetch_unpublished(1)
            if outcome == "published":
                await repository.mark_published(event)
            elif outcome == "failed":
                await repository.record_failure(event, "New failure")

            async with asyncio.timeout(2):
                async with outbox_database.session_scope() as observer:
                    assert await OutboxRepository(observer).fetch_unpublished(1) == []

            raise RuntimeError("Abort publisher transaction")

    async with asyncio.timeout(2):
        async with outbox_database.session_scope() as session:
            [event] = await OutboxRepository(session).fetch_unpublished(1)
            assert event.id == saved_event.id
            assert event.published_at is None
            assert event.attempts == 2
            assert event.last_error == "Earlier failure"


async def _save_events(database: Database, count: int) -> list[Outbox]:
    created_at = datetime(2026, 1, 1, tzinfo=UTC)
    payment = Payment(
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Outbox repository test",
        payment_metadata={},
        idempotency_key=f"test-outbox-{uuid4()}",
        webhook_url="https://example.com/webhook",
    )
    async with database.session_scope() as session:
        session.add(payment)
        await session.flush()
        events = [
            Outbox(
                id=UUID(int=count - index),
                payment_id=payment.id,
                event_type=OutboxEventType.PAYMENT_CREATED,
                payload={
                    "payment_id": str(payment.id),
                    "amount": str(payment.amount),
                    "currency": payment.currency.value,
                    "description": payment.description,
                    "metadata": payment.payment_metadata,
                    "webhook_url": payment.webhook_url,
                },
                created_at=created_at + timedelta(seconds=index),
            )
            for index in range(count)
        ]
        session.add_all(events)
        await session.flush()
    return events
