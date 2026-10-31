from copy import deepcopy

from payment_processor.db.models.payment import Payment as PaymentModel
from payment_processor.domain.payment import Payment


def to_domain(model: PaymentModel) -> Payment:
    return Payment(
        id=model.id,
        amount=model.amount,
        currency=model.currency,
        description=model.description,
        payment_metadata=model.payment_metadata,
        idempotency_key=model.idempotency_key,
        webhook_url=model.webhook_url,
        created_at=model.created_at,
        status=model.status,
        processed_at=model.processed_at,
        webhook_delivered_at=model.webhook_delivered_at,
    )


def update_model(model: PaymentModel, payment: Payment) -> None:
    if model.id != payment.id:
        raise ValueError("Payment identity does not match the persistence model")
    model.amount = payment.amount
    model.currency = payment.currency
    model.description = payment.description
    model.payment_metadata = deepcopy(payment.payment_metadata)
    model.idempotency_key = payment.idempotency_key
    model.webhook_url = payment.webhook_url
    model.created_at = payment.created_at
    model.status = payment.status
    model.processed_at = payment.processed_at
    model.webhook_delivered_at = payment.webhook_delivered_at


def to_model(payment: Payment) -> PaymentModel:
    model = PaymentModel(id=payment.id)
    update_model(model, payment)
    return model
