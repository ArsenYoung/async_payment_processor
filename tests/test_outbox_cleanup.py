from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from payment_processor.db.models import Outbox
from payment_processor.operations.outbox_cleanup import cleanup_batch


@pytest.mark.asyncio
async def test_cleanup_boundary_batch_dry_run_and_excluded_states(
    outbox_database, retry_event
):
    cutoff = datetime.now(UTC) - timedelta(days=30)
    old = cutoff - timedelta(seconds=1)
    states = [
        {"published_at": old},
        {"published_at": old},
        {"published_at": old},
        {"published_at": cutoff},
        {"published_at": cutoff + timedelta(seconds=1)},
        {"published_at": old, "quarantined_at": old},
        {"published_at": old, "publication_token": uuid4()},
        {"published_at": old, "publication_until": old},
    ]
    rows = [
        Outbox(
            id=uuid4(),
            payment_id=retry_event.payload.payment_id,
            event_type=retry_event.event_type,
            payload={},
            **state,
        )
        for state in states
    ]
    async with outbox_database.session_scope() as session:
        session.add_all(rows)
    assert (
        await cleanup_batch(outbox_database, cutoff=cutoff, limit=2, dry_run=True) == 2
    )
    async with outbox_database.session_scope() as session:
        assert len((await session.scalars(select(Outbox))).all()) == 9
    assert await cleanup_batch(outbox_database, cutoff=cutoff, limit=2) == 2
    assert await cleanup_batch(outbox_database, cutoff=cutoff, limit=2) == 1
    assert await cleanup_batch(outbox_database, cutoff=cutoff, limit=2) == 0
    async with outbox_database.session_scope() as session:
        remaining = set((await session.scalars(select(Outbox.id))).all())
    assert remaining == {retry_event.event_id, *(row.id for row in rows[3:])}


@pytest.mark.asyncio
async def test_cleanup_skips_locked_rows_without_blocking(outbox_database, retry_event):
    now = datetime.now(UTC)
    async with outbox_database.session_scope() as session:
        row = await session.get(Outbox, retry_event.event_id)
        row.published_at = now - timedelta(days=31)
    async with outbox_database.session_scope() as lock:
        await lock.scalar(select(Outbox).with_for_update())
        assert await cleanup_batch(outbox_database, cutoff=now, limit=10) == 0
    assert await cleanup_batch(outbox_database, cutoff=now, limit=10) == 1
