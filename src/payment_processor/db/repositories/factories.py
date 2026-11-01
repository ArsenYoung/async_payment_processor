from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.application.errors import (
    IdempotencyKeyViolationError,
    PersistenceUnavailableError,
)
from payment_processor.db.errors import is_database_unavailable
from payment_processor.db.repositories.outbox import SqlAlchemyOutboxRepository
from payment_processor.db.repositories.payments import (
    SqlAlchemyPaymentRepository,
    is_idempotency_key_violation,
)


def payment_repository_scope(
    session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    repository_factory: Callable[
        [AsyncSession], SqlAlchemyPaymentRepository
    ] = SqlAlchemyPaymentRepository,
) -> Callable[[], AbstractAsyncContextManager[SqlAlchemyPaymentRepository]]:
    @asynccontextmanager
    async def scope() -> AsyncIterator[SqlAlchemyPaymentRepository]:
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
    repository_factory: Callable[
        [AsyncSession], SqlAlchemyOutboxRepository
    ] = SqlAlchemyOutboxRepository,
) -> Callable[[], AbstractAsyncContextManager[SqlAlchemyOutboxRepository]]:
    @asynccontextmanager
    async def scope() -> AsyncIterator[SqlAlchemyOutboxRepository]:
        try:
            async with session_scope_factory() as session:
                yield repository_factory(session)
        except Exception as error:
            if not is_database_unavailable(error):
                raise
            raise PersistenceUnavailableError from error

    return scope
