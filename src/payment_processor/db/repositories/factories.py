from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.application.errors import (
    IdempotencyKeyViolationError,
    PersistenceUnavailableError,
)
from payment_processor.db.errors import is_database_unavailable
from payment_processor.db.repositories.outbox import OutboxRepository
from payment_processor.db.repositories.payments import (
    PaymentRepository,
    is_idempotency_key_violation,
)


def payment_repository_scope(
    session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    repository_factory: Callable[[AsyncSession], PaymentRepository] = PaymentRepository,
) -> Callable[[], AbstractAsyncContextManager[PaymentRepository]]:
    @asynccontextmanager
    async def scope() -> AsyncIterator[PaymentRepository]:
        try:
            async with session_scope_factory() as session:
                yield repository_factory(session)
        except IntegrityError as error:
            if not is_idempotency_key_violation(error):
                raise
            raise IdempotencyKeyViolationError from error

    return scope


def outbox_repository_scope(
    session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    repository_factory: Callable[[AsyncSession], OutboxRepository] = OutboxRepository,
) -> Callable[[], AbstractAsyncContextManager[OutboxRepository]]:
    @asynccontextmanager
    async def scope() -> AsyncIterator[OutboxRepository]:
        try:
            async with session_scope_factory() as session:
                yield repository_factory(session)
        except Exception as error:
            if not is_database_unavailable(error):
                raise
            raise PersistenceUnavailableError from error

    return scope
