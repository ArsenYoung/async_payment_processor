from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import AfterValidator

from payment_processor.api.dependencies import get_payment_service, require_api_key
from payment_processor.api.schemas.payment import (
    PaymentCreateRequest,
    PaymentCreateResponse,
)
from payment_processor.api.schemas.validation import validate_database_text
from payment_processor.services.idempotency import IdempotencyConflictError
from payment_processor.services.payments import PaymentService

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_api_key)],
)
async def create_payment(
    request: PaymentCreateRequest,
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255),
        AfterValidator(validate_database_text),
    ],
    payment_service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentCreateResponse:
    try:
        payment = await payment_service.create_payment(request, idempotency_key)
    except IdempotencyConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key was used with a different request",
        ) from error

    return PaymentCreateResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )
