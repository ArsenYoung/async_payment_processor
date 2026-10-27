from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.db.models import Payment


class IdempotencyConflictError(Exception):
    pass


def _matches_request(payment: Payment, request: PaymentCreateRequest) -> bool:
    return (
        payment.amount == request.amount
        and payment.currency == request.currency
        and payment.description == request.description
        and payment.payment_metadata == request.metadata
        and payment.webhook_url == str(request.webhook_url)
    )


def reuse_payment(payment: Payment, request: PaymentCreateRequest) -> Payment:
    if not _matches_request(payment, request):
        raise IdempotencyConflictError
    return payment
