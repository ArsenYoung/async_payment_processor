from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.db.models import Payment, PaymentStatus
from payment_processor.db.repositories.payments import (
    PaymentRepository,
    is_idempotency_key_violation,
)
from payment_processor.services.errors import PaymentNotFoundError
from payment_processor.services.payment_gateway_simulator import PaymentGatewaySimulator
from payment_processor.services.webhooks import WebhookClient


class IdempotencyConflictError(Exception):
    pass


class PaymentService:
    def __init__(
        self,
        session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        repository_factory: Callable[[AsyncSession], PaymentRepository] = (
            PaymentRepository
        ),
        *,
        gateway: PaymentGatewaySimulator | None = None,
    ) -> None:
        self._session_scope_factory = session_scope_factory
        self._repository_factory = repository_factory
        self._gateway = gateway if gateway is not None else PaymentGatewaySimulator()

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            return await repository.find_by_id(payment_id)

    async def create_payment(
        self, request: PaymentCreateRequest, idempotency_key: str
    ) -> Payment:
        try:
            return await self._create_payment(request, idempotency_key)
        except IntegrityError as error:
            return await self._recover_payment(request, idempotency_key, error)

    async def process_payment(self, payment_id: UUID) -> Payment:
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            payment = await repository.find_by_id_for_update(payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)
            if payment.status == PaymentStatus.PENDING:
                payment.status = await self._gateway.process()
                payment.processed_at = datetime.now(UTC)
        return payment

    async def deliver_webhook(
        self, payment_id: UUID, webhook_client: WebhookClient
    ) -> None:
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            payment = await repository.find_by_id_for_update(payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)
            if payment.webhook_delivered_at is not None:
                return
            await webhook_client.send(payment)
            await repository.mark_webhook_delivered(payment)

    async def _create_payment(
        self, request: PaymentCreateRequest, idempotency_key: str
    ) -> Payment:
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            existing = await repository.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return self._reuse_payment(existing, request)

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
            return self._reuse_payment(existing, request)

    def _reuse_payment(
        self, payment: Payment, request: PaymentCreateRequest
    ) -> Payment:
        if not self._matches_request(payment, request):
            raise IdempotencyConflictError
        return payment

    def _matches_request(self, payment: Payment, request: PaymentCreateRequest) -> bool:
        return (
            payment.amount == request.amount
            and payment.currency == request.currency
            and payment.description == request.description
            and payment.payment_metadata == request.metadata
            and payment.webhook_url == str(request.webhook_url)
        )
