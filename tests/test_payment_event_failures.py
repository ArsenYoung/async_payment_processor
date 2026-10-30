import httpx

from payment_processor.messaging.retry import RetryPolicy
from payment_processor.services.payment_event_failures import (
    classify_payment_event_failure,
)


def test_invalid_message_is_never_retryable() -> None:
    decision = classify_payment_event_failure(
        ValueError("invalid"),
        message_valid=False,
        attempt=1,
        retry_policy=RetryPolicy(base_delay=1),
    )

    assert decision.reason == "invalid_message"
    assert decision.retry is None


def test_retryable_error_is_scheduled_before_last_attempt() -> None:
    decision = classify_payment_event_failure(
        httpx.ConnectError("unavailable"),
        message_valid=True,
        attempt=1,
        retry_policy=RetryPolicy(base_delay=2),
    )

    assert decision.reason == "retryable_processing_error"
    assert decision.retry is not None
    assert decision.retry.number == 2
    assert decision.retry.delay == 2


def test_retryable_error_is_exhausted_on_last_attempt() -> None:
    decision = classify_payment_event_failure(
        httpx.ConnectError("unavailable"),
        message_valid=True,
        attempt=3,
        retry_policy=RetryPolicy(base_delay=2),
    )

    assert decision.reason == "attempts_exhausted"
    assert decision.retry is None
