from collections.abc import Awaitable
from typing import Protocol

from payment_processor.db.models import Payment, PaymentStatus
from payment_processor.messaging.schemas import PaymentCreatedEvent


class PaymentGateway(Protocol):
    def process(self) -> Awaitable[PaymentStatus]: ...


class WebhookSender(Protocol):
    def send(self, payment: Payment) -> Awaitable[None]: ...


class EventPublisher(Protocol):
    async def publish_payment_created(
        self, event: PaymentCreatedEvent, *, timeout: float
    ) -> None: ...
