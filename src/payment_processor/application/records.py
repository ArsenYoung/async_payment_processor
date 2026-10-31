from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

from payment_processor.domain.payment import PaymentCurrency, PaymentStatus


class PaymentRecord(Protocol):
    id: UUID
    amount: Decimal
    currency: PaymentCurrency
    description: str
    payment_metadata: dict[str, Any]
    idempotency_key: str
    webhook_url: str
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
