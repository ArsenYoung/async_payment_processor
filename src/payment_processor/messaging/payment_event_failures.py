from dataclasses import dataclass

from payment_processor.messaging.errors import is_retryable_processing_error
from payment_processor.messaging.retry import RetryAttempt, RetryPolicy


@dataclass(frozen=True)
class FailureDecision:
    reason: str
    retry: RetryAttempt | None


def classify_payment_event_failure(
    error: Exception,
    *,
    message_valid: bool,
    attempt: int,
    retry_policy: RetryPolicy,
) -> FailureDecision:
    if not message_valid:
        return FailureDecision("invalid_message", None)
    if not is_retryable_processing_error(error):
        return FailureDecision("non_retryable_processing_error", None)
    retry = retry_policy.next_retry(attempt)
    return FailureDecision(
        "retryable_processing_error" if retry else "attempts_exhausted", retry
    )
