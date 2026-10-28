from dataclasses import dataclass
from math import isfinite

MAX_ATTEMPTS = 3


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
