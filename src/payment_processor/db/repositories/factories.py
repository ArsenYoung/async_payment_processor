from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from sqlalchemy.exc import IntegrityError

from payment_processor.application.errors import IdempotencyKeyViolationError
from payment_processor.db.repositories.payments import (
    PaymentRepository,
    is_idempotency_key_violation,
)


def payment_repository_scope(
    session_scope_factory: Callable[[], Any],
    repository_factory: Callable[[Any], PaymentRepository] = PaymentRepository,
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
