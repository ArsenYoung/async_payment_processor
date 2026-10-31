from dataclasses import dataclass
from decimal import Decimal
from urllib.parse import urlparse


@dataclass(frozen=True)
class Money:
    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        if self.amount <= 0:
            raise ValueError("Money amount must be positive")
        if not self.currency:
            raise ValueError("Money currency is required")


@dataclass(frozen=True)
class IdempotencyKey:
    value: str

    def __post_init__(self) -> None:
        if not self.value or len(self.value) > 255:
            raise ValueError("Idempotency key must contain 1 to 255 characters")


@dataclass(frozen=True)
class WebhookUrl:
    value: str

    def __post_init__(self) -> None:
        parsed = urlparse(self.value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Webhook URL must be an HTTP or HTTPS URL")
