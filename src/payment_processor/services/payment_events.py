from typing import Any

from payment_processor.db.models import Payment
from payment_processor.messaging.schemas import PaymentCreatedPayload


def build_payment_created_payload(payment: Payment) -> dict[str, Any]:
    return PaymentCreatedPayload.model_validate(
        {
            "payment_id": payment.id,
            "amount": payment.amount,
            "currency": payment.currency,
            "description": payment.description,
            "metadata": payment.payment_metadata,
            "webhook_url": payment.webhook_url,
        }
    ).model_dump(mode="json")
