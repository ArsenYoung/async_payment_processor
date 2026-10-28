import logging
from time import perf_counter
from uuid import uuid4

from starlette.datastructures import MutableHeaders
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        request.state.request_id = str(uuid4())
        started_at = perf_counter()
        status_code = 500

        async def send_response(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                MutableHeaders(scope=message)["X-Request-ID"] = request.state.request_id
            await send(message)

        def context() -> dict[str, object]:
            fields: dict[str, object] = {
                "request_id": request.state.request_id,
                "method": request.method,
                "path": scope["path"],
                "status_code": status_code,
                "duration_ms": round((perf_counter() - started_at) * 1000, 3),
            }
            payment_id = getattr(request.state, "payment_id", None)
            if payment_id is not None:
                fields["payment_id"] = str(payment_id)
            return fields

        try:
            await self.app(scope, receive, send_response)
        except Exception:
            logger.exception("HTTP request failed", extra=context())
            raise
        else:
            logger.info("HTTP request completed", extra=context())


async def unhandled_error_response(
    request: Request, exc: Exception
) -> PlainTextResponse:
    return PlainTextResponse(
        "Internal Server Error",
        status_code=500,
        headers={"X-Request-ID": request.state.request_id},
    )
