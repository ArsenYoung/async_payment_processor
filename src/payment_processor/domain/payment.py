from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

from payment_processor.domain.value_objects import (
    validate_amount,
    validate_currency,
    validate_idempotency_key,
    validate_webhook_url,
)


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
        self.validate()
        self.payment_metadata = deepcopy(self.payment_metadata)

    def validate(self) -> None:
        validate_amount(self.amount)
        validate_currency(self.currency.value)
        validate_idempotency_key(self.idempotency_key)
        validate_webhook_url(self.webhook_url)
        if (self.status == PaymentStatus.PENDING) != (self.processed_at is None):
            raise InvalidPaymentTransition(
                "Payment status and processing time disagree"
            )
        if self.webhook_delivered_at is not None and not self.is_completed:
            raise InvalidPaymentTransition(
                "Webhook delivery requires a completed payment"
            )
        dates = [self.created_at, self.processed_at, self.webhook_delivered_at]
        if any(value is not None and value.utcoffset() is None for value in dates):
            raise ValueError("Payment timestamps must include a timezone")
        if self.processed_at is not None and self.processed_at < self.created_at:
            raise InvalidPaymentTransition("Processing cannot precede payment creation")
        if (
            self.webhook_delivered_at is not None
            and self.processed_at is not None
            and self.webhook_delivered_at < self.processed_at
        ):
            raise InvalidPaymentTransition("Delivery cannot precede processing")

    @property
    def is_completed(self) -> bool:
        return is_completed(self.status, self.processed_at)

    @property
    def can_process(self) -> bool:
        return can_process(self.status)

    def process(self, status: PaymentStatus) -> None:
        self.validate()
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
        self.validate()
        if not self.is_completed:
            raise InvalidPaymentTransition(
                "Webhook delivery requires a completed payment"
            )
        if self.webhook_delivered_at is None:
            self.webhook_delivered_at = datetime.now(UTC)
