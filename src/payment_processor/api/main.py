import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import HTMLResponse, JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from payment_processor.adapters.metrics import collect_outbox
from payment_processor.adapters.payment_gateway import PaymentGatewaySimulator
from payment_processor.api.dependencies import require_api_key
from payment_processor.api.exception_handlers import request_validation_error_handler
from payment_processor.api.middleware import (
    RequestLoggingMiddleware,
    unhandled_error_response,
)
from payment_processor.api.routes.payments import router as payments_router
from payment_processor.core.config import settings
from payment_processor.core.health import HEALTH_TIMEOUT
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
    application.add_exception_handler(HTTPException, documentation_auth_error)
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
        "/docs/oauth2-redirect",
        get_swagger_ui_oauth2_redirect_html,
        include_in_schema=False,
    )
    application.add_api_route("/ready", readiness, methods=["GET"], tags=["health"])
    application.add_api_route(
        "/metrics", metrics, methods=["GET"], include_in_schema=False
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
    html = (
        bytes(response.body)
        .decode()
        .replace(
            "url: '/openapi.json',",
            f"spec: {embedded_schema(request)},"
            + """
requestInterceptor: request => {
  const key = sessionStorage.getItem('documentation-api-key');
  if (key) request.headers['X-API-Key'] = key;
  return request;
},""",
        )
    )
    return HTMLResponse(html)


async def redoc_documentation(request: Request) -> HTMLResponse:
    response = get_redoc_html(openapi_url="/openapi.json", title="Payment API")
    html = (
        bytes(response.body)
        .decode()
        .replace('<redoc spec-url="/openapi.json"></redoc>', '<div id="redoc"></div>')
        .replace(
            "</body>",
            f"<script>Redoc.init({embedded_schema(request)}, {{}}, "
            'document.getElementById("redoc"));</script></body>',
        )
    )
    return HTMLResponse(html)


async def metrics(request: Request) -> Response:
    await collect_outbox(request.app.state.database)
    return Response(generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})


async def readiness(request: Request) -> Response:
    try:
        async with asyncio.timeout(HEALTH_TIMEOUT):
            await request.app.state.database.check_connection()
    except Exception:
        return JSONResponse({"status": "not_ready"}, status_code=503)
    return JSONResponse({"status": "ready"})


async def documentation_auth_error(
    request: Request, error: Exception
) -> HTMLResponse | Response:
    assert isinstance(error, HTTPException)
    if error.status_code == 401 and request.url.path in {"/docs", "/redoc"}:
        return HTMLResponse(
            DOCUMENTATION_LOGIN, status_code=401, headers={"Cache-Control": "no-store"}
        )
    return await http_exception_handler(request, error)


DOCUMENTATION_LOGIN = """<!doctype html><html><title>API documentation access</title>
<form id="login"><label>API key <input id="key" type="password" required></label>
<button>Open documentation</button><p id="error"></p></form>
<script>
document.getElementById('login').onsubmit = async event => {
  event.preventDefault();
  const key = document.getElementById('key').value;
  const response = await fetch(location.pathname, {headers: {'X-API-Key': key}});
  if (!response.ok) {
    document.getElementById('error').textContent = 'Invalid API key'; return;
  }
  sessionStorage.setItem('documentation-api-key', key);
  document.open(); document.write(await response.text()); document.close();
};
</script></html>"""


app = create_app()
