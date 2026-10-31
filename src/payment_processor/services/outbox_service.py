import asyncio
import logging
import math
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import perf_counter
from uuid import UUID, uuid4

from payment_processor.application.errors import PersistenceUnavailableError
from payment_processor.application.events import OutboxRecord, PaymentCreatedEvent
from payment_processor.application.ports import EventPublisher, OutboxStore
from payment_processor.core.logging import processing_fields

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublicationResult:
    published: int
    failed: int


class OutboxService:
    def __init__(
        self,
        store_scope_factory: Callable[[], AbstractAsyncContextManager[OutboxStore]],
        publisher: EventPublisher,
        *,
        queue_name: str | None = None,
    ) -> None:
        self._store_scope_factory = store_scope_factory
        self._publisher = publisher
        self._queue_name = queue_name

    async def run(
        self, *, batch_size: int, poll_interval: float, publish_timeout: float
    ) -> None:
        while True:
            try:
                result = await self.publish_batch(batch_size, timeout=publish_timeout)
            except (PersistenceUnavailableError, OSError) as error:
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
        if limit <= 0:
            raise ValueError("Batch limit must be positive")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Publication timeout must be positive and finite")
        published = 0
        failed = 0
        seen: list[UUID] = []
        context: dict[str, object] = {"operation": "outbox_poll"}
        try:
            for _ in range(limit):
                token = uuid4()
                finalized = False
                record: OutboxRecord | None = None
                try:
                    async with self._store_scope_factory() as repository:
                        record = await repository.claim_next(
                            token, max(60.0, timeout + 30.0), exclude_ids=seen
                        )
                    if record is None:
                        finalized = True
                        break
                    seen.append(record.id)
                    started = perf_counter()
                    context = processing_fields(
                        operation="outbox_publish",
                        payment_id=str(record.payment_id),
                        event_id=str(record.id),
                        queue=self._queue_name,
                        publication_attempt=record.attempts + 1,
                    )
                    failure: Exception | None = None
                    invalid = False
                    try:
                        event = PaymentCreatedEvent.from_outbox(record)
                    except ValueError as error:
                        invalid = True
                        failure = error
                    else:
                        try:
                            async with asyncio.timeout(timeout):
                                await self._publisher.publish_payment_created(
                                    event, timeout=timeout
                                )
                        except Exception as error:
                            failure = error
                    async with self._store_scope_factory() as repository:
                        current = await repository.lock_claimed(record.id, token)
                        if invalid:
                            await repository.quarantine(
                                current, f"{type(failure).__name__}: {failure}"
                            )
                        elif failure is not None:
                            delay = min(60, 2 ** min(current.attempts, 6))
                            await repository.record_failure(
                                current,
                                f"{type(failure).__name__}: {failure}",
                                next_attempt_at=datetime.now(UTC)
                                + timedelta(seconds=delay),
                            )
                        else:
                            await repository.mark_published(current)
                        await repository.release_claim(record.id, token)
                    finalized = True
                    fields = {
                        **context,
                        "outcome": "failed" if failure else "published",
                        "duration_ms": (perf_counter() - started) * 1000,
                    }
                    if failure is not None:
                        failed += 1
                        logger.warning(
                            "Outbox publication failed",
                            extra={**fields, "error_type": type(failure).__name__},
                            exc_info=(type(failure), failure, failure.__traceback__),
                        )
                    else:
                        published += 1
                        logger.info("Outbox event published", extra=fields)
                finally:
                    if not finalized and record is not None:
                        try:
                            async with self._store_scope_factory() as repository:
                                await repository.release_claim(record.id, token)
                        except Exception:
                            logger.exception(
                                "Outbox lease cleanup failed; lease will expire"
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
        return PublicationResult(published=published, failed=failed)
