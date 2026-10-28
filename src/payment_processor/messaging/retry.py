from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isfinite
from typing import Any

import httpx
from sqlalchemy.exc import DBAPIError

MAX_ATTEMPTS = 3
ATTEMPT_HEADER = "x-payment-attempt"


def read_attempt(headers: Mapping[str, Any]) -> int:
    attempt = headers.get(ATTEMPT_HEADER, 1)
    if type(attempt) is not int or not 1 <= attempt <= MAX_ATTEMPTS:
        raise ValueError(f"Attempt must be an integer between 1 and {MAX_ATTEMPTS}")
    return attempt


def failure_headers(
    headers: Mapping[str, Any], *, attempt: int, error: Exception, reason: str
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
            read_attempt(headers)
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


@dataclass(frozen=True)
class RetryAttempt:
    number: int
    delay: float


class RetryPolicy:
    def __init__(self, *, base_delay: float) -> None:
        if (
            isinstance(base_delay, bool)
            or not isinstance(base_delay, (int, float))
            or not isfinite(base_delay)
            or base_delay <= 0
            or not isfinite(base_delay * 2)
        ):
            raise ValueError("Retry base delay must produce positive finite delays")
        self._base_delay = base_delay

    def next_retry(self, attempt: int) -> RetryAttempt | None:
        if type(attempt) is not int or not 1 <= attempt <= MAX_ATTEMPTS:
            raise ValueError(f"Attempt must be an integer between 1 and {MAX_ATTEMPTS}")
        if attempt == MAX_ATTEMPTS:
            return None
        return RetryAttempt(
            number=attempt + 1,
            delay=self._base_delay * 2 ** (attempt - 1),
        )
