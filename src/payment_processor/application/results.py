"""Stable results exposed by application use cases."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from payment_processor.db.models import Payment, PaymentCurrency, PaymentStatus


@dataclass(frozen=True)
class PaymentCreatedResult:
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_payment(cls, payment: Payment) -> "PaymentCreatedResult":
        return cls(payment.id, payment.status, payment.created_at)


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
    webhook_delivered_at: datetime | None = None

    @classmethod
    def from_payment(cls, payment: Payment) -> "PaymentDetailsResult":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=deepcopy(payment.payment_metadata),
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            status=payment.status,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
        )
