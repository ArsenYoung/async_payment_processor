import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import HTMLResponse, Response

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
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
        dependencies=[Depends(require_api_key)],
    )
    application.add_exception_handler(HTTPException, documentation_auth_error)
    application.add_api_route(
        "/openapi.json", application.openapi, include_in_schema=False
    )
    application.add_api_route("/docs", swagger, include_in_schema=False)
    application.add_api_route("/redoc", redoc, include_in_schema=False)
    application.add_api_route(
        "/docs/oauth2-redirect",
        get_swagger_ui_oauth2_redirect_html,
        include_in_schema=False,
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
    return application


async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


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


async def swagger() -> HTMLResponse:
    response = get_swagger_ui_html(openapi_url="/openapi.json", title="Payment API")
    html = (
        bytes(response.body)
        .decode()
        .replace(
            "url: '/openapi.json',",
            """url: '/openapi.json',
        requestInterceptor: request => {
          request.headers['X-API-Key'] =
            sessionStorage.getItem('documentation-api-key') || '';
          return request;
        },""",
        )
    )
    return HTMLResponse(html, headers={"Cache-Control": "no-store"})


async def redoc() -> HTMLResponse:
    return HTMLResponse(
        """<!doctype html><html><title>Payment API</title><div id="redoc"></div>
<script src="https://cdn.jsdelivr.net/npm/redoc@2/bundles/redoc.standalone.js"></script>
<script>
const key = sessionStorage.getItem('documentation-api-key') || '';
fetch('/openapi.json', {headers: {'X-API-Key': key}})
.then(response => {
  if (!response.ok) throw Error('Authorization failed');
  return response.json();
})
.then(spec => Redoc.init(spec, {}, document.getElementById('redoc')));
</script></html>""",
        headers={"Cache-Control": "no-store"},
    )


app = create_app()
