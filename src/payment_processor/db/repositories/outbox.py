from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.models import Outbox


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def fetch_unpublished(self, limit: int) -> list[Outbox]:
        if limit <= 0:
            raise ValueError("Batch limit must be positive")
        result = await self.session.scalars(
            select(Outbox)
            .where(
                Outbox.published_at.is_(None),
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

    async def mark_published(self, event: Outbox) -> None:
        event.published_at = datetime.now(UTC)
        event.attempts += 1
        event.last_error = None
        event.next_attempt_at = None
        await self.session.flush()

    async def record_failure(
        self, event: Outbox, error: str, *, next_attempt_at: datetime | None = None
    ) -> None:
        event.attempts += 1
        event.last_error = error
        event.next_attempt_at = next_attempt_at
        await self.session.flush()

    async def quarantine(self, event: Outbox, error: str) -> None:
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
