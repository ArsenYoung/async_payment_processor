from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

from payment_processor.domain.payment import PaymentCurrency, PaymentStatus


class PaymentRecord(Protocol):
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

    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None

