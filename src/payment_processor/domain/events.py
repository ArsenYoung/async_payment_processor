from enum import StrEnum


class OutboxEventType(StrEnum):
    PAYMENT_CREATED = "payment.created"
