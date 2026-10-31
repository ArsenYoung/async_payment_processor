from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.application.errors import LeaseLostError
from payment_processor.application.events import OutboxRecord
from payment_processor.application.ports import OutboxStore
from payment_processor.db.models import Outbox


class SqlAlchemyOutboxRepository(OutboxStore):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def fetch_unpublished(
        self, limit: int, *, exclude_ids: Sequence[UUID] = ()
    ) -> list[Outbox]:
        if limit <= 0:
            raise ValueError("Batch limit must be positive")
        result = await self.session.scalars(
            select(Outbox)
            .where(
                Outbox.id.not_in(exclude_ids),
                Outbox.published_at.is_(None),
                or_(
                    Outbox.publication_until.is_(None),
                    Outbox.publication_until <= func.clock_timestamp(),
                ),
                Outbox.quarantined_at.is_(None),
                or_(
                    Outbox.next_attempt_at.is_(None),
                    Outbox.next_attempt_at <= datetime.now(UTC),
                ),
            )
            .order_by(Outbox.created_at, Outbox.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.all())

    async def claim_next(
        self, token: UUID, seconds: float, *, exclude_ids: Sequence[UUID]
    ) -> Outbox | None:
        if seconds <= 0:
            raise ValueError("Lease duration must be positive")
        records = await self.fetch_unpublished(1, exclude_ids=exclude_ids)
        if not records:
            return None
        record = records[0]
        record.publication_token = token
        record.publication_until = await self.session.scalar(
            select(func.clock_timestamp() + timedelta(seconds=seconds))
        )
        await self.session.flush()
        return record

    async def lock_claimed(self, event_id: UUID, token: UUID) -> Outbox:
        record = await self.session.scalar(
            select(Outbox)
            .where(
                Outbox.id == event_id,
                Outbox.publication_token == token,
                Outbox.publication_until > func.clock_timestamp(),
                Outbox.published_at.is_(None),
                Outbox.quarantined_at.is_(None),
            )
            .with_for_update()
        )
        if record is None:
            raise LeaseLostError("Outbox publication lease was lost")
        return record

    async def release_claim(self, event_id: UUID, token: UUID) -> None:
        await self.session.execute(
            update(Outbox)
            .where(Outbox.id == event_id, Outbox.publication_token == token)
            .values(publication_token=None, publication_until=None)
            .execution_options(synchronize_session=False)
        )

    async def mark_published(self, event: OutboxRecord) -> None:
        event.published_at = datetime.now(UTC)
        event.attempts += 1
        event.last_error = None
        event.next_attempt_at = None
        await self.session.flush()

    async def record_failure(
        self,
        event: OutboxRecord,
        error: str,
        *,
        next_attempt_at: datetime | None = None,
    ) -> None:
        event.attempts += 1
        event.last_error = error
        event.next_attempt_at = next_attempt_at
        await self.session.flush()

    async def quarantine(self, event: OutboxRecord, error: str) -> None:
        event.attempts += 1
        event.last_error = error
        event.quarantined_at = datetime.now(UTC)
        event.next_attempt_at = None
        await self.session.flush()

    async def get_for_recovery(self, event_id: UUID) -> Outbox | None:
        return await self.session.scalar(
            select(Outbox).where(Outbox.id == event_id).with_for_update()
        )

    async def restore_quarantined(self, event: Outbox) -> None:
        if event.published_at is not None or event.quarantined_at is None:
            raise ValueError("Only unpublished quarantined events can be recovered")
        event.quarantined_at = None
        event.next_attempt_at = None
        await self.session.flush()


OutboxRepository = SqlAlchemyOutboxRepository
