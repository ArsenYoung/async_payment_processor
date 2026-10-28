from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.repositories.outbox import OutboxRepository
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.messaging.topology import publish_payment_event


@dataclass(frozen=True)
class PublicationResult:
    published: int
    failed: int


class OutboxService:
    def __init__(
        self,
        session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        broker: RabbitBroker,
        repository_factory: Callable[
            [AsyncSession], OutboxRepository
        ] = OutboxRepository,
    ) -> None:
        self._session_scope_factory = session_scope_factory
        self._broker = broker
        self._repository_factory = repository_factory

    async def publish_batch(
        self, limit: int, *, timeout: float = 5.0
    ) -> PublicationResult:
        published = 0
        failed = 0
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            events = await repository.fetch_unpublished(limit)
            for record in events:
                try:
                    event = PaymentCreatedEvent.from_outbox(record)
                    await publish_payment_event(self._broker, event, timeout=timeout)
                except Exception as error:
                    await repository.record_failure(
                        record, f"{type(error).__name__}: {error}"
                    )
                    failed += 1
                else:
                    await repository.mark_published(record)
                    published += 1
        return PublicationResult(published=published, failed=failed)
