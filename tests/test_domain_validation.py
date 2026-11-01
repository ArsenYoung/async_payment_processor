from decimal import Decimal

import pytest

from payment_processor.domain.validation import (
    validate_amount,
    validate_idempotency_key,
    validate_webhook_url,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("amount", ["0.01", "10.5000", "9999999999999999.99"])
def test_amount_accepts_database_precision(amount: str) -> None:
    value = Decimal(amount)
    assert validate_amount(value) == value


@pytest.mark.parametrize(
    "amount", ["0", "-1", "NaN", "sNaN", "Infinity", "10.123", "1E16", "1E-100000"]
)
def test_amount_rejects_invalid_values(amount: str) -> None:
    with pytest.raises(ValueError):
        validate_amount(Decimal(amount))


@pytest.mark.parametrize("key", ["", "a" * 256, "invalid\x00key", "\ud800"])
def test_idempotency_key_rejects_invalid_values(key: str) -> None:
    with pytest.raises(ValueError):
        validate_idempotency_key(key)


def test_idempotency_key_accepts_limit() -> None:
    assert validate_idempotency_key("a" * 255) == "a" * 255


@pytest.mark.parametrize(
    "url",
    [
        "",
        "ftp://example.com",
        "example.com",
        "https://example.com:invalid",
        "https://example.com/" + "a" * 2048,
    ],
)
def test_webhook_url_rejects_invalid_values(url: str) -> None:
    with pytest.raises(ValueError):
        validate_webhook_url(url)


def test_webhook_url_accepts_http() -> None:
    assert (
        validate_webhook_url("http://example.com/payment")
        == "http://example.com/payment"
    )
