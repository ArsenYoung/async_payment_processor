"""Backward-compatible facade over the payment application use cases."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.application.commands import (
    CreatePaymentCommand,
    DeliverWebhookCommand,
    GetPaymentQuery,
    ProcessPaymentCommand,
)
from payment_processor.db.models import Payment
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.services.payment_creation import CreatePaymentUseCase
from payment_processor.services.payment_gateway_simulator import PaymentGatewaySimulator
from payment_processor.services.payment_lookup import GetPaymentUseCase
from payment_processor.services.payment_processing import ProcessPaymentUseCase
from payment_processor.services.ports import PaymentGateway, WebhookSender
from payment_processor.services.webhook_delivery import DeliverPaymentWebhookUseCase


class PaymentService:
    """Compatibility facade used by existing API, worker and test wiring."""

    def __init__(
        self,
        session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        repository_factory: Callable[
            [AsyncSession], PaymentRepository
        ] = PaymentRepository,
        *,
        gateway: PaymentGateway | None = None,
    ) -> None:
        selected_gateway = gateway if gateway is not None else PaymentGatewaySimulator()
        self._create = CreatePaymentUseCase(session_scope_factory, repository_factory)
        self._get = GetPaymentUseCase(session_scope_factory, repository_factory)
        self._process = ProcessPaymentUseCase(
            session_scope_factory, selected_gateway, repository_factory
        )
        self._deliver_webhook = DeliverPaymentWebhookUseCase(
            session_scope_factory, repository_factory
        )

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
        return await self._create.create(command)

    async def get_payment(self, query: GetPaymentQuery | UUID) -> Payment | None:
        payment_id = query.payment_id if isinstance(query, GetPaymentQuery) else query
        return await self._get.execute(payment_id)

    async def process_payment(
        self, command: ProcessPaymentCommand | UUID
    ) -> Payment:
        payment_id = (
            command.payment_id
            if isinstance(command, ProcessPaymentCommand)
            else command
        )
        return await self._process.process(payment_id)

    async def deliver_webhook(
        self, command: DeliverWebhookCommand | UUID, webhook_client: WebhookSender
    ) -> None:
        payment_id = (
            command.payment_id
            if isinstance(command, DeliverWebhookCommand)
            else command
        )
        await self._deliver_webhook.deliver(payment_id, webhook_client)
