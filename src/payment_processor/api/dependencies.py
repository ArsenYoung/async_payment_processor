"""Reusable API dependencies."""

import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

from payment_processor.core.config import settings
from payment_processor.db.session import Database
from payment_processor.services.payment_service import PaymentService

api_key_header = APIKeyHeader(
    name="X-API-Key",
    scheme_name="APIKey",
    auto_error=False,
)


def get_payment_service(request: Request) -> PaymentService:
    database: Database = request.app.state.database
    return PaymentService(session_scope_factory=database.session_scope)


async def require_api_key(
    api_key: Annotated[str | None, Depends(api_key_header)],
) -> None:
    if api_key is None or not secrets.compare_digest(
        api_key.encode("utf-8"), settings.api_key.encode("utf-8")
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
