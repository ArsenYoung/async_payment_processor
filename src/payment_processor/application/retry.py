from dataclasses import dataclass
from math import isfinite

MAX_ATTEMPTS = 3


def validate_attempt(attempt: int, *, max_attempts: int = MAX_ATTEMPTS) -> int:
    if type(attempt) is not int or not 1 <= attempt <= max_attempts:
        raise ValueError(f"Attempt must be an integer between 1 and {max_attempts}")
    return attempt


@dataclass(frozen=True)
class RetryAttempt:
    number: int
    delay: float


@dataclass(frozen=True, kw_only=True)
class RetryPolicy:
    base_delay: float = 1.0
    max_attempts: int = MAX_ATTEMPTS
    backoff_multiplier: float = 2.0

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("Retry max attempts must be a positive integer")
        for value in (self.base_delay, self.backoff_multiplier):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not isfinite(value)
                or value <= 0
            ):
                raise ValueError(
                    "Retry configuration must produce positive finite delays"
                )
        if self.backoff_multiplier < 1:
            raise ValueError("Retry backoff multiplier must be at least one")
        try:
            delay = self.base_delay * self.backoff_multiplier ** max(
                0, self.max_attempts - 2
            )
        except OverflowError as error:
            raise ValueError(
                "Retry configuration must produce positive finite delays"
            ) from error
        if not isfinite(delay):
            raise ValueError("Retry configuration must produce positive finite delays")

    @property
    def retry_attempts(self) -> range:
        return range(2, self.max_attempts + 1)

    def next_retry(self, attempt: int) -> RetryAttempt | None:
        validate_attempt(attempt, max_attempts=self.max_attempts)
        if attempt == self.max_attempts:
            return None
        return RetryAttempt(
            number=attempt + 1,
            delay=self.base_delay * self.backoff_multiplier ** (attempt - 1),
        )
