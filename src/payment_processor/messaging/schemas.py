import json
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    JsonValue,
    field_validator,
)

from payment_processor.db.models import (
    Outbox,
    OutboxEventType,
    Payment,
    PaymentCurrency,
    PaymentStatus,
)


class PaymentCreatedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    payment_id: UUID
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: PaymentCurrency
    description: str = Field(min_length=1, max_length=500)
    metadata: dict[str, JsonValue]
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        json.dumps(value, allow_nan=False)
        return value


class PaymentCreatedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: Literal[OutboxEventType.PAYMENT_CREATED]
    payload: PaymentCreatedPayload

    @classmethod
    def from_outbox(cls, outbox: Outbox) -> "PaymentCreatedEvent":
        event = cls.model_validate(
            {
                "event_id": outbox.id,
                "event_type": outbox.event_type,
                "payload": outbox.payload,
            }
        )
        if event.payload.payment_id != outbox.payment_id:
            raise ValueError("Event payment_id does not match the outbox payment_id")
        return event


class PaymentResultPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    payment_id: UUID
    status: Literal[PaymentStatus.SUCCEEDED, PaymentStatus.FAILED]
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: PaymentCurrency
    description: str = Field(min_length=1, max_length=500)
    metadata: dict[str, JsonValue]
    created_at: AwareDatetime
    processed_at: AwareDatetime

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        json.dumps(value, allow_nan=False)
        return value

    @classmethod
    def from_payment(cls, payment: Payment) -> "PaymentResultPayload":
        return cls.model_validate(
            {
                "payment_id": payment.id,
                "status": payment.status,
                "amount": payment.amount,
                "currency": payment.currency,
                "description": payment.description,
                "metadata": payment.payment_metadata,
                "created_at": payment.created_at,
                "processed_at": payment.processed_at,
            }
        )
