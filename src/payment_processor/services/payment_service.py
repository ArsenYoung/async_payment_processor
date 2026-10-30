import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.models import Payment
from payment_processor.db.repositories.payments import (
    PaymentRepository,
)
from payment_processor.services.contracts import CreatePaymentCommand, CreatePaymentInput
from payment_processor.services.payment_creation import (
    PaymentCreationService,
)
from payment_processor.services.payment_gateway_simulator import PaymentGatewaySimulator
from payment_processor.services.payment_processing import PaymentProcessingService
from payment_processor.services.ports import PaymentGateway, WebhookSender
from payment_processor.services.webhook_delivery import WebhookDeliveryService

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(
        self,
        session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        repository_factory: Callable[[AsyncSession], PaymentRepository] = (
            PaymentRepository
        ),
        *,
        gateway: PaymentGateway | None = None,
    ) -> None:
        selected_gateway = gateway if gateway is not None else PaymentGatewaySimulator()
        self._creation = PaymentCreationService(
            session_scope_factory, repository_factory
        )
        self._processing = PaymentProcessingService(
            session_scope_factory, selected_gateway, repository_factory
        )
        self._webhook_delivery = WebhookDeliveryService(
            session_scope_factory, repository_factory
        )

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await self._creation.get(payment_id)

    async def create_payment(
        self, command: CreatePaymentCommand | CreatePaymentInput, idempotency_key: str
    ) -> Payment:
        return await self._creation.create(
            self._normalize_command(command), idempotency_key
        )

    async def process_payment(self, payment_id: UUID) -> Payment:
        return await self._processing.process(payment_id)

    async def deliver_webhook(
        self, payment_id: UUID, webhook_client: WebhookSender
    ) -> None:
        await self._webhook_delivery.deliver(payment_id, webhook_client)

    @staticmethod
    def _normalize_command(
        command: CreatePaymentCommand | CreatePaymentInput,
    ) -> CreatePaymentCommand:
        if isinstance(command, CreatePaymentCommand):
            return command
        return CreatePaymentCommand(
            amount=command.amount,
            currency=command.currency,
            description=command.description,
            metadata=command.metadata,
            webhook_url=str(command.webhook_url),
        )
