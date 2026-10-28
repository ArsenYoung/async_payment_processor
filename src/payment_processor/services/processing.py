from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.models import Payment, PaymentStatus
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.services.payment_gateway import PaymentGatewaySimulator


class PaymentProcessingService:
    def __init__(
        self,
        session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        gateway: PaymentGatewaySimulator,
        repository_factory: Callable[[AsyncSession], PaymentRepository] = (
            PaymentRepository
        ),
    ) -> None:
        self._session_scope_factory = session_scope_factory
        self._gateway = gateway
        self._repository_factory = repository_factory

    async def process_payment(self, payment_id: UUID) -> Payment:
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            payment = await repository.find_by_id_for_update(payment_id)
            if payment is None:
                raise LookupError(f"Payment {payment_id} not found")
            if payment.status == PaymentStatus.PENDING:
                payment.status = await self._gateway.process()
                payment.processed_at = datetime.now(UTC)
        return payment
