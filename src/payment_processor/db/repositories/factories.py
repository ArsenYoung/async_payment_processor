from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from payment_processor.db.repositories.payments import PaymentRepository


def payment_repository_scope(
    session_scope_factory: Callable[[], Any],
    repository_factory: Callable[[Any], PaymentRepository] = PaymentRepository,
) -> Callable[[], AbstractAsyncContextManager[PaymentRepository]]:
    @asynccontextmanager
    async def scope() -> AsyncIterator[PaymentRepository]:
        async with session_scope_factory() as session:
            yield repository_factory(session)

    return scope
