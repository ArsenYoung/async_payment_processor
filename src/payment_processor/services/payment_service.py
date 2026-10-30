import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from decimal import Decimal
from time import perf_counter
from typing import Any
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.application.commands import (
    CreatePaymentCommand,
    DeliverWebhookCommand,
    GetPaymentQuery,
    ProcessPaymentCommand,
)
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

    async def get_payment(self, query: GetPaymentQuery | UUID) -> Payment | None:
        if isinstance(query, UUID):
            query = GetPaymentQuery(query)
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            return await repository.find_by_id(query.payment_id)

    async def create_payment(
        self, command: CreatePaymentCommand | Any, idempotency_key: str | None = None
    ) -> Payment:
        if not isinstance(command, CreatePaymentCommand):
            if idempotency_key is None:
                raise TypeError("idempotency_key is required for legacy requests")
            command = CreatePaymentCommand(
                amount=command.amount,
                currency=command.currency,
                description=command.description,
                metadata=command.metadata,
                webhook_url=str(command.webhook_url),
                idempotency_key=idempotency_key,
            )
        try:
            return await self._create_payment(command)
        except IntegrityError as error:
            return await self._recover_payment(command, error)

    async def process_payment(self, command: ProcessPaymentCommand | UUID) -> Payment:
        if isinstance(command, UUID):
            command = ProcessPaymentCommand(command)
        payment_id = command.payment_id
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
        self,
        command: DeliverWebhookCommand | UUID,
        webhook_client: WebhookClient,
    ) -> None:
        if isinstance(command, UUID):
            command = DeliverWebhookCommand(command)
        payment_id = command.payment_id
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
        self, command: CreatePaymentCommand
    ) -> Payment:
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            existing = await repository.find_by_idempotency_key(command.idempotency_key)
            if existing is not None:
                return self._reuse_payment(existing, command)

            payment = Payment(
                amount=command.amount,
                currency=command.currency,
                description=command.description,
                payment_metadata=command.metadata,
                idempotency_key=command.idempotency_key,
                webhook_url=command.webhook_url,
            )
            return await repository.save_with_outbox(payment)

    async def _recover_payment(
        self,
        command: CreatePaymentCommand,
        error: IntegrityError,
    ) -> Payment:
        if not is_idempotency_key_violation(error):
            raise error
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            existing = await repository.find_by_idempotency_key(command.idempotency_key)
            if existing is None:
                raise error
            return self._reuse_payment(existing, command)

    def _reuse_payment(
        self, payment: Payment, command: CreatePaymentCommand
    ) -> Payment:
        if not self._matches_request(payment, command):
            raise IdempotencyConflictError
        return payment

    def _matches_request(self, payment: Payment, command: CreatePaymentCommand) -> bool:
        return (
            payment.amount == command.amount
            and payment.currency == command.currency
            and payment.description == command.description
            and _json_values_equal(payment.payment_metadata, command.metadata)
            and payment.webhook_url == command.webhook_url
        )
