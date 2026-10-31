from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import httpx
from sqlalchemy.exc import DBAPIError

from payment_processor.application.retry import (
    MAX_ATTEMPTS as MAX_ATTEMPTS,
)
from payment_processor.application.retry import (
    RetryAttempt as RetryAttempt,
)
from payment_processor.application.retry import (
    RetryPolicy as RetryPolicy,
)
from payment_processor.application.retry import (
    validate_attempt,
)

ATTEMPT_HEADER = "x-payment-attempt"


def read_attempt(
    headers: Mapping[str, Any], *, max_attempts: int = MAX_ATTEMPTS
) -> int:
    attempt = headers.get(ATTEMPT_HEADER, 1)
    return validate_attempt(attempt, max_attempts=max_attempts)


def failure_headers(
    headers: Mapping[str, Any],
    *,
    attempt: int,
    error: Exception,
    reason: str,
    max_attempts: int = MAX_ATTEMPTS,
) -> dict[str, Any]:
    result = dict(headers)
    result.update(
        {
            ATTEMPT_HEADER: attempt,
            "x-payment-failed-attempt": attempt,
            "x-payment-error-type": type(error).__name__,
            "x-payment-error-reason": reason,
            "x-payment-error-at": datetime.now(UTC).isoformat(),
        }
    )
    if ATTEMPT_HEADER in headers:
        try:
            read_attempt(headers, max_attempts=max_attempts)
        except ValueError:
            result["x-payment-invalid-attempt"] = headers[ATTEMPT_HEADER]
    result.pop("x-payment-error-http-status", None)
    result.pop("x-payment-error-sqlstate", None)
    if isinstance(error, httpx.HTTPStatusError):
        result["x-payment-error-http-status"] = error.response.status_code
    if isinstance(error, DBAPIError):
        sqlstate = getattr(error.orig, "sqlstate", None)
        if isinstance(sqlstate, str) and len(sqlstate) == 5 and sqlstate.isalnum():
            result["x-payment-error-sqlstate"] = sqlstate
    return result
