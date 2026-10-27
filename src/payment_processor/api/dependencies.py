"""Reusable API dependencies."""

import secrets
from typing import Annotated

from fastapi import Header, HTTPException, Request, status

from payment_processor.core.config import settings
from payment_processor.db.session import Database
from payment_processor.services.payments import PaymentService


def get_payment_service(request: Request) -> PaymentService:
    database: Database = request.app.state.database
    return PaymentService(session_scope_factory=database.session_scope)


async def require_api_key(
    api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> None:
    if api_key is None or not secrets.compare_digest(
        api_key.encode("utf-8"), settings.api_key.encode("utf-8")
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
