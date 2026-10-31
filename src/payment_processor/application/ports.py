from collections.abc import Awaitable, Sequence
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from payment_processor.application.events import OutboxRecord, PaymentCreatedEvent
from payment_processor.application.records import PaymentInput
from payment_processor.application.results import PaymentDetailsResult
from payment_processor.domain.payment import Payment, PaymentStatus


class PaymentGateway(Protocol):
    def process(self, *, idempotency_key: UUID) -> Awaitable[PaymentStatus]: ...


class WebhookSender(Protocol):
    def send(self, payment: PaymentDetailsResult) -> Awaitable[None]: ...


class EventPublisher(Protocol):
    async def publish_payment_created(
        self, event: PaymentCreatedEvent, *, timeout: float
    ) -> None: ...


class PaymentReader(Protocol):
    async def find_by_id(self, payment_id: UUID) -> Payment | None: ...


class PaymentCreationStore(PaymentReader, Protocol):
    async def find_by_idempotency_key(self, idempotency_key: str) -> Payment | None: ...

    def set_event_payload(self, payload: dict[str, object]) -> None: ...

    async def save_with_outbox(self, payment: PaymentInput) -> Payment: ...


LeaseOperation = Literal["processing", "webhook"]


class PaymentProcessingStore(Protocol):
    async def claim_lease(
        self, payment_id: UUID, operation: LeaseOperation, token: UUID, seconds: float
    ) -> bool: ...

    async def verify_lease(
        self, payment_id: UUID, operation: LeaseOperation, token: UUID
    ) -> None: ...

    async def release_lease(
        self, payment_id: UUID, operation: LeaseOperation, token: UUID
    ) -> None: ...

    async def save(self, payment: Payment) -> None: ...

    async def find_by_id_for_update(self, payment_id: UUID) -> Payment | None: ...


class WebhookDeliveryStore(PaymentProcessingStore, Protocol):
    async def mark_webhook_delivered(self, payment: Payment) -> None: ...


class PaymentRepository(PaymentCreationStore, WebhookDeliveryStore, Protocol):
    pass


class OutboxStore(Protocol):
    async def fetch_unpublished(self, limit: int) -> Sequence[OutboxRecord]: ...

    async def mark_published(self, event: OutboxRecord) -> None: ...

    async def record_failure(
        self,
        event: OutboxRecord,
        error: str,
        *,
        next_attempt_at: datetime | None = None,
    ) -> None: ...

    async def quarantine(self, event: OutboxRecord, error: str) -> None: ...
