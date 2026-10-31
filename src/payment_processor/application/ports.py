from collections.abc import Awaitable
from typing import Protocol
from uuid import UUID

from payment_processor.application.events import PaymentCreatedEvent
from payment_processor.application.records import PaymentRecord
from payment_processor.application.results import PaymentDetailsResult
from payment_processor.domain.payment import PaymentStatus


class PaymentGateway(Protocol):
    def process(self) -> Awaitable[PaymentStatus]: ...


class WebhookSender(Protocol):
    def send(self, payment: PaymentDetailsResult) -> Awaitable[None]: ...


class EventPublisher(Protocol):
    async def publish_payment_created(
        self, event: PaymentCreatedEvent, *, timeout: float
    ) -> None: ...


class PaymentReader(Protocol):
    async def find_by_id(self, payment_id: UUID) -> PaymentRecord | None: ...


class PaymentCreationStore(PaymentReader, Protocol):
    async def find_by_idempotency_key(
        self, idempotency_key: str
    ) -> PaymentRecord | None: ...

    def set_event_payload(self, payload: dict[str, object]) -> None: ...

    async def save_with_outbox(self, payment: PaymentRecord) -> PaymentRecord: ...


class PaymentProcessingStore(Protocol):
    async def find_by_id_for_update(self, payment_id: UUID) -> PaymentRecord | None: ...


class WebhookDeliveryStore(PaymentProcessingStore, Protocol):
    async def mark_webhook_delivered(self, payment: PaymentRecord) -> None: ...


class PaymentRepository(PaymentCreationStore, WebhookDeliveryStore, Protocol):
    pass
