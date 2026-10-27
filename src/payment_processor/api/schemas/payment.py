"""Schemas for creating payments."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue, field_validator

from payment_processor.api.schemas.validation import (
    validate_database_text,
    validate_json_value,
)
from payment_processor.db.models.payment import PaymentCurrency, PaymentStatus


class PaymentCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: PaymentCurrency
    description: str = Field(min_length=1, max_length=500)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("description")
    @classmethod
    def validate_description(cls, value: str) -> str:
        return validate_database_text(value)

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        validate_json_value(value)
        return value

    @field_validator("webhook_url")
    @classmethod
    def validate_webhook_url(cls, value: HttpUrl) -> HttpUrl:
        if len(str(value)) > 2048:
            raise ValueError("Webhook URL must not exceed 2048 characters")
        return value


class PaymentCreateResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime
