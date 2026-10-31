from decimal import Decimal

import pytest

from payment_processor.domain.value_objects import IdempotencyKey, Money, WebhookUrl


def test_money_accepts_positive_amount() -> None:
    assert Money(Decimal("10.50"), "RUB").amount == Decimal("10.50")


@pytest.mark.parametrize("amount", [Decimal("0"), Decimal("-1")])
def test_money_rejects_non_positive_amount(amount: Decimal) -> None:
    with pytest.raises(ValueError):
        Money(amount, "RUB")


def test_idempotency_key_rejects_empty_value() -> None:
    with pytest.raises(ValueError):
        IdempotencyKey("")


def test_webhook_url_accepts_http_url() -> None:
    assert WebhookUrl("https://example.com/payment").value.endswith("payment")


@pytest.mark.parametrize("url", ["", "ftp://example.com", "example.com"])
def test_webhook_url_rejects_non_http_url(url: str) -> None:
    with pytest.raises(ValueError):
        WebhookUrl(url)
