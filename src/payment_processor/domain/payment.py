from datetime import UTC, datetime
from enum import StrEnum


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class InvalidPaymentTransition(ValueError):
    """Raised when a payment state transition violates a domain invariant."""


class Payment:
    def __init__(
        self,
        *,
        status: PaymentStatus = PaymentStatus.PENDING,
        processed_at: datetime | None = None,
        webhook_delivered_at: datetime | None = None,
    ) -> None:
        self.status = status
        self.processed_at = processed_at
        self.webhook_delivered_at = webhook_delivered_at

    @property
    def is_completed(self) -> bool:
        return self.status in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED} and (
            self.processed_at is not None
        )

    @property
    def can_process(self) -> bool:
        return self.status == PaymentStatus.PENDING

    def process(self, status: PaymentStatus) -> None:
        if not self.can_process:
            raise InvalidPaymentTransition(
                f"Payment in status {self.status} cannot be processed"
            )
        if status not in {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}:
            raise InvalidPaymentTransition(
                f"Payment cannot transition to status {status}"
            )
        self.status = status
        self.processed_at = datetime.now(UTC)

    def mark_webhook_delivered(self) -> None:
        if not self.is_completed:
            raise InvalidPaymentTransition(
                "Webhook delivery requires a completed payment"
            )
        if self.webhook_delivered_at is None:
            self.webhook_delivered_at = datetime.now(UTC)
