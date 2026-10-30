from datetime import UTC, datetime

import pytest

from payment_processor.domain.payment import (
    InvalidPaymentTransition,
    Payment,
    PaymentStatus,
)


def test_pending_payment_can_transition_to_success() -> None:
    payment = Payment()

    payment.process(PaymentStatus.SUCCEEDED)

    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert payment.is_completed


def test_completed_payment_cannot_be_processed_again() -> None:
    payment = Payment(
        status=PaymentStatus.SUCCEEDED,
        processed_at=datetime.now(UTC),
    )

    with pytest.raises(InvalidPaymentTransition):
        payment.process(PaymentStatus.FAILED)


def test_webhook_requires_completed_payment() -> None:
    with pytest.raises(InvalidPaymentTransition):
        Payment().mark_webhook_delivered()


def test_webhook_delivery_is_idempotent() -> None:
    payment = Payment(
        status=PaymentStatus.FAILED,
        processed_at=datetime.now(UTC),
    )

    payment.mark_webhook_delivered()
    first_delivery = payment.webhook_delivered_at
    payment.mark_webhook_delivered()

    assert first_delivery is not None
    assert payment.webhook_delivered_at == first_delivery
