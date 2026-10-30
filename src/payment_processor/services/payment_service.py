import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from decimal import Decimal
from time import perf_counter
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.core.logging import processing_fields
from payment_processor.db.models import Payment, PaymentStatus
from payment_processor.db.repositories.payments import (
    PaymentRepository,
    is_idempotency_key_violation,
)
from payment_processor.services.errors import PaymentNotFoundError
from payment_processor.services.payment_gateway_simulator import PaymentGatewaySimulator
from payment_processor.services.webhooks import WebhookClient

logger = logging.getLogger(__name__)


def _json_values_equal(left: object, right: object) -> bool:
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            _json_values_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _json_values_equal(left_value, right_value)
            for left_value, right_value in zip(left, right, strict=True)
        )
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, (int, float, Decimal)) and isinstance(
        right, (int, float, Decimal)
    ):
        return Decimal(str(left)) == Decimal(str(right))
    return type(left) is type(right) and left == right


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
        started = perf_counter()
        fields = processing_fields(
            payment_id=str(payment_id), operation="payment_process"
        )
        try:
            async with self._session_scope_factory() as session:
                repository = self._repository_factory(session)
                payment = await repository.find_by_id_for_update(payment_id)
                if payment is None:
                    raise PaymentNotFoundError(payment_id)
                outcome = "already_processed"
                if payment.status == PaymentStatus.PENDING:
                    payment.status = await self._gateway.process()
                    payment.processed_at = datetime.now(UTC)
                    outcome = "processed"
        except Exception as error:
            logger.warning(
                "Payment processing failed",
                extra={
                    **fields,
                    "outcome": "failed",
                    "duration_ms": (perf_counter() - started) * 1000,
                    "error_type": type(error).__name__,
                },
                exc_info=True,
            )
            raise
        logger.info(
            "Payment processing completed",
            extra={
                **fields,
                "outcome": outcome,
                "payment_status": payment.status.value,
                "duration_ms": (perf_counter() - started) * 1000,
            },
        )
        return payment

    async def deliver_webhook(
        self, payment_id: UUID, webhook_client: WebhookClient
    ) -> None:
        started = perf_counter()
        fields = processing_fields(
            payment_id=str(payment_id), operation="webhook_delivery"
        )
        try:
            async with self._session_scope_factory() as session:
                repository = self._repository_factory(session)
                payment = await repository.find_by_id_for_update(payment_id)
                if payment is None:
                    raise PaymentNotFoundError(payment_id)
                outcome = "already_delivered"
                if payment.webhook_delivered_at is None:
                    await webhook_client.send(payment)
                    await repository.mark_webhook_delivered(payment)
                    outcome = "delivered"
        except Exception as error:
            logger.warning(
                "Webhook delivery failed",
                extra={
                    **fields,
                    "outcome": "failed",
                    "duration_ms": (perf_counter() - started) * 1000,
                    "error_type": type(error).__name__,
                },
                exc_info=True,
            )
            raise
        logger.info(
            "Webhook delivery completed",
            extra={
                **fields,
                "outcome": outcome,
                "duration_ms": (perf_counter() - started) * 1000,
            },
        )

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
            and _json_values_equal(payment.payment_metadata, request.metadata)
            and payment.webhook_url == str(request.webhook_url)
        )
