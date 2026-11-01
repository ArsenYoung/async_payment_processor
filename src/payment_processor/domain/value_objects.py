from dataclasses import dataclass
from decimal import Decimal
from urllib.parse import urlparse


def validate_amount(amount: Decimal) -> None:
    if not amount.is_finite() or amount <= 0:
        raise ValueError("Money amount must be positive")


def validate_currency(currency: str) -> None:
    if currency not in {"RUB", "USD", "EUR"}:
        raise ValueError("Unsupported money currency")


def validate_idempotency_key(value: str) -> None:
    if not value or len(value) > 255:
        raise ValueError("Idempotency key must contain 1 to 255 characters")


def validate_webhook_url(value: str) -> None:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Webhook URL must be an HTTP or HTTPS URL")


@dataclass(frozen=True)
class Money:
    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        validate_amount(self.amount)
        validate_currency(self.currency)


@dataclass(frozen=True)
class IdempotencyKey:
    value: str

    def __post_init__(self) -> None:
        validate_idempotency_key(self.value)


@dataclass(frozen=True)
class WebhookUrl:
    value: str

    def __post_init__(self) -> None:
        validate_webhook_url(self.value)
