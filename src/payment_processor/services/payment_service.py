"""Facade over the payment application use cases."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

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
        self, command: CreatePaymentCommand
    ) -> PaymentCreatedResult:
        return await self._create.create(command)

    async def get_payment(self, query: GetPaymentQuery) -> PaymentDetailsResult | None:
        return await self._get.execute(query.payment_id)

    async def process_payment(
        self, command: ProcessPaymentCommand
    ) -> PaymentDetailsResult:
        return await self._process.process(command.payment_id)

    async def deliver_webhook(
        self, command: DeliverWebhookCommand, webhook_client: WebhookSender
    ) -> None:
        await self._deliver_webhook.deliver(command.payment_id, webhook_client)
