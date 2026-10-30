from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.models import Payment
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.services.ports import PaymentReader


class GetPaymentUseCase:
    def __init__(
        self,
        session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        repository_factory: Callable[[AsyncSession], PaymentReader] = PaymentRepository,
    ) -> None:
        self._session_scope_factory = session_scope_factory
        self._repository_factory = repository_factory

    async def execute(self, payment_id: UUID) -> Payment | None:
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            return await repository.find_by_id(payment_id)
