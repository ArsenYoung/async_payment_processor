import asyncio
import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import perf_counter

from faststream.rabbit import RabbitBroker
from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    InterfaceError,
    OperationalError,
)
from sqlalchemy.exc import TimeoutError as DatabaseTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.core.logging import processing_fields
from payment_processor.db.repositories.outbox import OutboxRepository
from payment_processor.messaging import topology
from payment_processor.messaging.schemas import PaymentCreatedEvent

logger = logging.getLogger(__name__)


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

    async def run(
        self, *, batch_size: int, poll_interval: float, publish_timeout: float
    ) -> None:
        while True:
            try:
                result = await self.publish_batch(batch_size, timeout=publish_timeout)
            except (
                DBAPIError,
                DisconnectionError,
                DatabaseTimeoutError,
                OSError,
            ) as error:
                if isinstance(error, DBAPIError) and not (
                    error.connection_invalidated
                    or isinstance(error, (OperationalError, InterfaceError))
                ):
                    raise
                logger.exception(
                    "Outbox database unavailable; retrying after pause",
                    extra=processing_fields(
                        operation="outbox_poll",
                        outcome="failed",
                        reason="database_unavailable",
                        error_type=type(error).__name__,
                    ),
                )
            else:
                if result.failed:
                    logger.warning(
                        "Outbox batch: %d published, %d failed",
                        result.published,
                        result.failed,
                    )
                elif result.published:
                    logger.info("Outbox batch: %d published", result.published)
            await asyncio.sleep(poll_interval)

    async def publish_batch(
        self, limit: int, *, timeout: float = 5.0
    ) -> PublicationResult:
        published = 0
        failed = 0
        results: list[tuple[dict[str, object], Exception | None]] = []
        context: dict[str, object] = {"operation": "outbox_poll"}
        try:
            async with self._session_scope_factory() as session:
                repository = self._repository_factory(session)
                events = await repository.fetch_unpublished(limit)
                for record in events:
                    started = perf_counter()
                    context = processing_fields(
                        operation="outbox_publish",
                        payment_id=str(record.payment_id),
                        event_id=str(record.id),
                        queue=topology.PAYMENTS_NEW_QUEUE.name,
                        publication_attempt=record.attempts + 1,
                    )
                    failure: Exception | None = None
                    try:
                        event = PaymentCreatedEvent.from_outbox(record)
                    except ValueError as error:
                        await repository.quarantine(
                            record, f"{type(error).__name__}: {error}"
                        )
                        failure = error
                        failed += 1
                    else:
                        try:
                            await topology.publish_payment_event(
                                self._broker, event, timeout=timeout
                            )
                        except Exception as error:
                            delay = min(60, 2 ** min(record.attempts, 6))
                            await repository.record_failure(
                                record,
                                f"{type(error).__name__}: {error}",
                                next_attempt_at=datetime.now(UTC)
                                + timedelta(seconds=delay),
                            )
                            failure = error
                            failed += 1
                        else:
                            await repository.mark_published(record)
                            published += 1
                    results.append(
                        (
                            {
                                **context,
                                "outcome": "failed" if failure else "published",
                                "duration_ms": (perf_counter() - started) * 1000,
                            },
                            failure,
                        )
                    )
        except Exception as error:
            logger.exception(
                "Outbox transaction failed",
                extra={
                    **context,
                    "outcome": "transaction_failed",
                    "error_type": type(error).__name__,
                },
            )
            raise
        for fields, failure in results:
            if failure is not None:
                logger.warning(
                    "Outbox publication failed",
                    extra={**fields, "error_type": type(failure).__name__},
                    exc_info=(type(failure), failure, failure.__traceback__),
                )
            else:
                logger.info("Outbox event published", extra=fields)
        return PublicationResult(published=published, failed=failed)
