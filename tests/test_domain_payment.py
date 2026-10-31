from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from payment_processor.domain.payment import (
    InvalidPaymentTransition,
    Payment,
    PaymentCurrency,
    PaymentStatus,
)


def make_payment(**state: Any) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Domain payment",
        payment_metadata={},
        idempotency_key="domain-key",
        webhook_url="https://example.com/webhook",
        **state,
    )


def test_pending_payment_can_transition_to_success() -> None:
    payment = make_payment()

    payment.process(PaymentStatus.SUCCEEDED)

    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.is_completed


def test_completed_payment_cannot_be_processed_again() -> None:
    payment = make_payment(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )

    with pytest.raises(InvalidPaymentTransition):
        payment.process(PaymentStatus.FAILED)


def test_webhook_requires_completed_payment() -> None:
    with pytest.raises(InvalidPaymentTransition):
        make_payment().mark_webhook_delivered()


def test_webhook_delivery_is_idempotent() -> None:
    payment = make_payment(
        status=PaymentStatus.FAILED,
        processed_at=datetime.now(UTC),
    )

    payment.mark_webhook_delivered()
    first_delivery = payment.webhook_delivered_at
    payment.mark_webhook_delivered()

    assert first_delivery is not None
    assert payment.webhook_delivered_at == first_delivery


@pytest.mark.parametrize("target", list(PaymentStatus))
@pytest.mark.parametrize("source", list(PaymentStatus))
def test_only_pending_to_terminal_transitions_are_allowed(
    source: PaymentStatus, target: PaymentStatus
) -> None:
    payment = make_payment(status=source)
    if source == PaymentStatus.PENDING and target != PaymentStatus.PENDING:
        payment.process(target)
        assert payment.status == target
        assert payment.is_completed
    else:
        with pytest.raises(InvalidPaymentTransition):
            payment.process(target)
        assert payment.status == source
        assert payment.processed_at is None


def test_unknown_status_is_rejected() -> None:
    with pytest.raises(ValueError):
        make_payment(status="unknown")


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
def test_terminal_status_without_processing_time_cannot_deliver(status: PaymentStatus):
    payment = make_payment(status=status)
    assert not payment.is_completed
    with pytest.raises(InvalidPaymentTransition):
        payment.mark_webhook_delivered()
