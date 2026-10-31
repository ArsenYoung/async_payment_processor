import json
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
)

from payment_processor.application.events import (
    PaymentCreatedEvent as PaymentCreatedEvent,
)
from payment_processor.application.events import (
    PaymentCreatedPayload as PaymentCreatedPayload,
)
from payment_processor.application.results import PaymentDetailsResult
from payment_processor.domain.payment import PaymentCurrency, PaymentStatus


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
    def from_result(cls, result: PaymentDetailsResult) -> "PaymentResultPayload":
        return cls.model_validate(
            {
                "payment_id": result.payment_id,
                "status": result.status,
                "amount": result.amount,
                "currency": result.currency,
                "description": result.description,
                "metadata": result.metadata,
                "created_at": result.created_at,
                "processed_at": result.processed_at,
            }
        )
