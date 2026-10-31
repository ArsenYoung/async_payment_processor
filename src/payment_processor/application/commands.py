"""Commands and queries used by payment application services."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any
from uuid import UUID

from payment_processor.domain.payment import PaymentCurrency


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: PaymentCurrency
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class ProcessPaymentCommand:
    payment_id: UUID


@dataclass(frozen=True, slots=True)
class DeliverWebhookCommand:
    payment_id: UUID


@dataclass(frozen=True, slots=True)
class GetPaymentQuery:
    payment_id: UUID
