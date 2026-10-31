from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from pydantic import AfterValidator

from payment_processor.api.dependencies import get_payment_service
from payment_processor.api.schemas.payment import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentDetailsResponse,
)
from payment_processor.api.schemas.validation import validate_database_text
from payment_processor.application.commands import CreatePaymentCommand, GetPaymentQuery
from payment_processor.services.payment_creation import IdempotencyConflictError
from payment_processor.services.payment_service import PaymentService

router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_payment(
    request: PaymentCreateRequest,
    http_request: Request,
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=255),
        AfterValidator(validate_database_text),
    ],
    payment_service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentCreateResponse:
    try:
        result = await payment_service.create_payment(
            CreatePaymentCommand(
                amount=request.amount,
                currency=request.currency,
                description=request.description,
                metadata=request.metadata,
                webhook_url=str(request.webhook_url),
                idempotency_key=idempotency_key,
            )
        )
    except IdempotencyConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency key was used with a different request",
        ) from error

    http_request.state.payment_id = result.payment_id
    return PaymentCreateResponse(
        payment_id=result.payment_id,
        status=result.status,
        created_at=result.created_at,
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentDetailsResponse,
    status_code=status.HTTP_200_OK,
)
async def get_payment(
    payment_id: UUID,
    request: Request,
    payment_service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentDetailsResponse:
    request.state.payment_id = payment_id
    result = await payment_service.get_payment(GetPaymentQuery(payment_id))
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Payment not found",
        )

    return PaymentDetailsResponse(
        payment_id=result.payment_id,
        amount=result.amount,
        currency=result.currency,
        description=result.description,
        metadata=result.metadata,
        idempotency_key=result.idempotency_key,
        webhook_url=result.webhook_url,
        status=result.status,
        created_at=result.created_at,
        processed_at=result.processed_at,
    )
