from datetime import datetime
from decimal import Decimal

from payment_processor.application.records import PaymentRecord
from payment_processor.domain.payment import Payment as DomainPayment
from payment_processor.domain.payment import PaymentStatus
from payment_processor.services.contracts import CreatePaymentCommand


def json_values_equal(left: object, right: object) -> bool:
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            json_values_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            json_values_equal(left_value, right_value)
            for left_value, right_value in zip(left, right, strict=True)
        )
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, (int, float, Decimal)) and isinstance(
        right, (int, float, Decimal)
    ):
        return Decimal(str(left)) == Decimal(str(right))
    return type(left) is type(right) and left == right


def matches_create_command(
    payment: PaymentRecord, command: CreatePaymentCommand
) -> bool:
    return (
        payment.amount == command.amount
        and payment.currency == command.currency
        and payment.description == command.description
        and json_values_equal(payment.payment_metadata, command.metadata)
        and payment.webhook_url == command.webhook_url
    )


def is_completed(status: PaymentStatus, processed_at: datetime | None) -> bool:
    return DomainPayment(status=status, processed_at=processed_at).is_completed


def can_process(status: PaymentStatus) -> bool:
    return DomainPayment(status=status).can_process


def can_deliver_webhook(status: PaymentStatus, processed_at: datetime | None) -> bool:
    return is_completed(status, processed_at)
