from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

from payment_processor.db.models import PaymentCurrency, PaymentStatus


@dataclass(frozen=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: PaymentCurrency
    description: str
    metadata: dict[str, Any]
    webhook_url: str


class CreatePaymentInput(Protocol):
    amount: Decimal
    currency: PaymentCurrency
    description: str
    metadata: dict[str, Any]
    webhook_url: Any


@dataclass(frozen=True)
class PaymentCreatedResult:
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


@dataclass(frozen=True)
class PaymentDetailsResult:
    payment_id: UUID
    amount: Decimal
    currency: PaymentCurrency
    description: str
    metadata: dict[str, Any]
    idempotency_key: str
    webhook_url: str
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
