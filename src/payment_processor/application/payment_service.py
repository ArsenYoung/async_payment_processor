"""Facade over the payment application use cases."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from uuid import UUID

from payment_processor.application.commands import CreatePaymentCommand
from payment_processor.application.payment_creation import CreatePaymentUseCase
from payment_processor.application.payment_lookup import GetPaymentUseCase
from payment_processor.application.payment_processing import ProcessPaymentUseCase
from payment_processor.application.ports import (
    PaymentGateway,
    PaymentRepository,
    WebhookSender,
)
from payment_processor.application.results import (
    PaymentCreatedResult,
    PaymentDetailsResult,
)
from payment_processor.application.webhook_delivery import DeliverPaymentWebhookUseCase


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
        self, command: CreatePaymentCommand
    ) -> PaymentCreatedResult:
        return await self._create.create(command)

    async def get_payment(self, payment_id: UUID) -> PaymentDetailsResult | None:
        return await self._get.execute(payment_id)

    async def process_payment(self, payment_id: UUID) -> PaymentDetailsResult:
        return await self._process.process(payment_id)

    async def deliver_webhook(
        self, payment_id: UUID, webhook_client: WebhookSender
    ) -> None:
        await self._deliver_webhook.deliver(payment_id, webhook_client)
