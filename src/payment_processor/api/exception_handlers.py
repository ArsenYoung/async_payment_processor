import json

from fastapi import Request, Response
from fastapi.exceptions import RequestValidationError


async def request_validation_error_handler(
    request: Request, exc: Exception
) -> Response:
    if not isinstance(exc, RequestValidationError):
        raise exc
    errors = [
        {"type": error["type"], "loc": error["loc"], "msg": error["msg"]}
        for error in exc.errors()
    ]
    return Response(
        content=json.dumps({"detail": errors}, ensure_ascii=True, allow_nan=False),
        status_code=422,
        media_type="application/json",
    )
