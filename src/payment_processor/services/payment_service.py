"""Facade over the payment application use cases."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any
from uuid import UUID

from payment_processor.application.commands import (
    CreatePaymentCommand,
    DeliverWebhookCommand,
    GetPaymentQuery,
    ProcessPaymentCommand,
)
from payment_processor.application.ports import (
    PaymentGateway,
    PaymentRepository,
    WebhookSender,
)
from payment_processor.application.results import (
    PaymentCreatedResult,
    PaymentDetailsResult,
)
from payment_processor.services.payment_creation import CreatePaymentUseCase
from payment_processor.services.payment_lookup import GetPaymentUseCase
from payment_processor.services.payment_processing import ProcessPaymentUseCase
from payment_processor.services.webhook_delivery import DeliverPaymentWebhookUseCase


class PaymentService:
    """Facade used by API, worker and test wiring."""

    def __init__(
        self,
        store_scope_factory: Callable[
            [], AbstractAsyncContextManager[PaymentRepository]
        ],
        *,
        gateway: PaymentGateway,
    ) -> None:
        self._create = CreatePaymentUseCase(store_scope_factory)
        self._get = GetPaymentUseCase(store_scope_factory)
        self._process = ProcessPaymentUseCase(store_scope_factory, gateway)
        self._deliver_webhook = DeliverPaymentWebhookUseCase(store_scope_factory)

    async def create_payment(
        self, command: CreatePaymentCommand | Any, idempotency_key: str | None = None
    ) -> PaymentCreatedResult:
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

    async def get_payment(
        self, query: GetPaymentQuery | UUID
    ) -> PaymentDetailsResult | None:
        payment_id = query.payment_id if isinstance(query, GetPaymentQuery) else query
        return await self._get.execute(payment_id)

    async def process_payment(
        self, command: ProcessPaymentCommand | UUID
    ) -> PaymentDetailsResult:
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
