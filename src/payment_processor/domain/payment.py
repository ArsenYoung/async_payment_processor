from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID


class PaymentCurrency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


def is_completed(status: PaymentStatus, processed_at: datetime | None) -> bool:
    return status in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED} and (
        processed_at is not None
    )


def can_process(status: PaymentStatus) -> bool:
    return status == PaymentStatus.PENDING


class InvalidPaymentTransition(ValueError):
    """Raised when a payment state transition violates a domain invariant."""


@dataclass(kw_only=True)
class Payment:
    id: UUID
    amount: Decimal
    currency: PaymentCurrency
    description: str
    payment_metadata: dict[str, Any]
    idempotency_key: str
    webhook_url: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    status: PaymentStatus = PaymentStatus.PENDING
    processed_at: datetime | None = None
    webhook_delivered_at: datetime | None = None

    def __post_init__(self) -> None:
        self.status = PaymentStatus(self.status)
        self.currency = PaymentCurrency(self.currency)
        self.payment_metadata = deepcopy(self.payment_metadata)

    @property
    def is_completed(self) -> bool:
        return is_completed(self.status, self.processed_at)

    @property
    def can_process(self) -> bool:
        return can_process(self.status)

    def process(self, status: PaymentStatus) -> None:
        if not self.can_process:
            raise InvalidPaymentTransition(
                f"Payment in status {self.status} cannot be processed"
            )
        if status not in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}:
            raise InvalidPaymentTransition(
                f"Payment cannot transition to status {status}"
            )
        self.status = PaymentStatus(status)
        self.processed_at = datetime.now(UTC)

    def mark_webhook_delivered(self) -> None:
        if not self.is_completed:
            raise InvalidPaymentTransition(
                "Webhook delivery requires a completed payment"
            )
        if self.webhook_delivered_at is None:
            self.webhook_delivered_at = datetime.now(UTC)
