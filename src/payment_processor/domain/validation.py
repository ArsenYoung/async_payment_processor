from decimal import Decimal
from urllib.parse import urlparse

AMOUNT_MAX_DIGITS = 18
AMOUNT_DECIMAL_PLACES = 2
IDEMPOTENCY_KEY_MAX_LENGTH = 255
WEBHOOK_URL_MAX_LENGTH = 2048


def validate_amount(amount: Decimal) -> Decimal:
    if not isinstance(amount, Decimal) or not amount.is_finite() or amount <= 0:
        raise ValueError("Payment amount must be a positive finite decimal")
    _, digits, exponent = amount.as_tuple()
    if not isinstance(exponent, int):
        raise ValueError("Payment amount must be finite")
    while digits and digits[-1] == 0:
        digits = digits[:-1]
        exponent += 1
    if max(-exponent, 0) > AMOUNT_DECIMAL_PLACES:
        raise ValueError("Payment amount must have at most two decimal places")
    if len(digits) + exponent > AMOUNT_MAX_DIGITS - AMOUNT_DECIMAL_PLACES:
        raise ValueError("Payment amount exceeds NUMERIC(18, 2)")
    return amount


def validate_idempotency_key(value: str) -> str:
    if not value or len(value) > IDEMPOTENCY_KEY_MAX_LENGTH:
        raise ValueError("Idempotency key must contain 1 to 255 characters")
    if "\x00" in value:
        raise ValueError("Null characters are not allowed")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("Invalid Unicode string") from error
    return value


def validate_webhook_url(value: str) -> str:
    parsed = urlparse(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or len(value) > WEBHOOK_URL_MAX_LENGTH
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        raise ValueError(
            "Webhook URL must be an HTTP or HTTPS URL of at most 2048 characters"
        )
    try:
        _ = parsed.port
    except ValueError as error:
        raise ValueError("Invalid webhook URL port") from error
    return value
