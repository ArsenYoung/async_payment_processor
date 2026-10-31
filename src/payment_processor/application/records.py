from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

from payment_processor.domain.payment import PaymentCurrency, PaymentStatus


class PaymentInput(Protocol):
    @property
    def id(self) -> UUID: ...

    @property
    def amount(self) -> Decimal: ...

    @property
    def currency(self) -> PaymentCurrency: ...

    @property
    def description(self) -> str: ...

    @property
    def payment_metadata(self) -> dict[str, Any]: ...

    @property
    def idempotency_key(self) -> str: ...

    @property
    def webhook_url(self) -> str: ...


class PaymentRecord(PaymentInput, Protocol):
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None


@dataclass(frozen=True)
class NewPayment:
    id: UUID
    amount: Decimal
    currency: PaymentCurrency
    description: str
    payment_metadata: dict[str, Any]
    idempotency_key: str
    webhook_url: str
