from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.db.models import Payment
from payment_processor.db.repositories.payments import (
    PaymentRepository,
    is_idempotency_key_violation,
)
from payment_processor.services.idempotency import reuse_payment


class PaymentService:
    def __init__(
        self,
        session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        repository_factory: Callable[[AsyncSession], PaymentRepository] = (
            PaymentRepository
        ),
    ) -> None:
        self._session_scope_factory = session_scope_factory
        self._repository_factory = repository_factory

    async def create_payment(
        self, request: PaymentCreateRequest, idempotency_key: str
    ) -> Payment:
        try:
            return await self._create_payment(request, idempotency_key)
        except IntegrityError as error:
            return await self._recover_payment(request, idempotency_key, error)

    async def _create_payment(
        self, request: PaymentCreateRequest, idempotency_key: str
    ) -> Payment:
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            existing = await repository.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return reuse_payment(existing, request)

            payment = Payment(
                amount=request.amount,
                currency=request.currency,
                description=request.description,
                payment_metadata=request.metadata,
                idempotency_key=idempotency_key,
                webhook_url=str(request.webhook_url),
            )
            return await repository.save_with_outbox(payment)

    async def _recover_payment(
        self,
        request: PaymentCreateRequest,
        idempotency_key: str,
        error: IntegrityError,
    ) -> Payment:
        if not is_idempotency_key_violation(error):
            raise error
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            existing = await repository.find_by_idempotency_key(idempotency_key)
            if existing is None:
                raise error
            return reuse_payment(existing, request)
