from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.exceptions import RequestValidationError

from payment_processor.api.dependencies import require_api_key
from payment_processor.api.exception_handlers import request_validation_error_handler
from payment_processor.api.routes.payments import router as payments_router
from payment_processor.core.config import settings
from payment_processor.db.session import Database
from payment_processor.messaging.broker import create_broker


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Open infrastructure connections when the API starts."""
    database: Database = application.state.database
    try:
        await database.check_connection()
        broker = create_broker()
        try:
            await broker.connect()
            application.state.broker = broker
            yield
        finally:
            await broker.stop()
    finally:
        await database.dispose()


def create_app(database: Database | None = None) -> FastAPI:
    application = FastAPI(
        title="Payment Processing Service",
        lifespan=lifespan,
        dependencies=[Depends(require_api_key)],
    )
    application.state.database = (
        database if database is not None else Database(settings.database_url)
    )
    application.add_exception_handler(
        RequestValidationError, request_validation_error_handler
    )
    application.include_router(payments_router, prefix="/api/v1")
    application.add_api_route("/health", healthcheck, methods=["GET"], tags=["health"])
    return application


async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


app = create_app()
