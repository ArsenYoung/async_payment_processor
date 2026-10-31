import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.exceptions import RequestValidationError

from payment_processor.adapters.payment_gateway import PaymentGatewaySimulator
from payment_processor.api.dependencies import require_api_key
from payment_processor.api.exception_handlers import request_validation_error_handler
from payment_processor.api.middleware import (
    RequestLoggingMiddleware,
    unhandled_error_response,
)
from payment_processor.api.routes.payments import router as payments_router
from payment_processor.core.config import settings
from payment_processor.core.logging import configure_logging
from payment_processor.db.session import Database

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Check the database and release its resources when the API stops."""
    database: Database = application.state.database
    try:
        await database.check_connection()
        yield
    except Exception:
        logger.exception("API lifespan failed")
        raise
    finally:
        await database.dispose()


def create_app(database: Database | None = None) -> FastAPI:
    configure_logging("api", settings)
    application = FastAPI(
        title="Payment Processing Service",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        dependencies=[Depends(require_api_key)],
    )
    application.state.database = (
        database if database is not None else Database(settings.database_url)
    )
    application.state.payment_gateway = PaymentGatewaySimulator()
    application.add_exception_handler(
        RequestValidationError, request_validation_error_handler
    )
    application.add_exception_handler(Exception, unhandled_error_response)
    application.add_middleware(RequestLoggingMiddleware)
    application.include_router(payments_router, prefix="/api/v1")
    application.add_api_route("/health", healthcheck, methods=["GET"], tags=["health"])
    application.add_api_route("/openapi.json", openapi_schema, include_in_schema=False)
    application.add_api_route("/docs", swagger_documentation, include_in_schema=False)
    application.add_api_route("/redoc", redoc_documentation, include_in_schema=False)
    application.add_api_route(
        "/docs/oauth2-redirect", get_swagger_ui_oauth2_redirect_html,
        include_in_schema=False,
    )
    return application


async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


async def openapi_schema(request: Request) -> JSONResponse:
    return JSONResponse(request.app.openapi())


def embedded_schema(request: Request) -> str:
    return json.dumps(request.app.openapi()).replace("<", "\\u003c")


async def swagger_documentation(request: Request) -> HTMLResponse:
    response = get_swagger_ui_html(openapi_url="/openapi.json", title="Payment API")
    html = bytes(response.body).decode().replace(
        "url: '/openapi.json',", f"spec: {embedded_schema(request)},"
    )
    return HTMLResponse(html)


async def redoc_documentation(request: Request) -> HTMLResponse:
    response = get_redoc_html(openapi_url="/openapi.json", title="Payment API")
    html = bytes(response.body).decode().replace(
        '<redoc spec-url="/openapi.json"></redoc>', '<div id="redoc"></div>'
    ).replace(
        "</body>",
        f'<script>Redoc.init({embedded_schema(request)}, {{}}, document.getElementById("redoc"));</script></body>',
    )
    return HTMLResponse(html)


app = create_app()
