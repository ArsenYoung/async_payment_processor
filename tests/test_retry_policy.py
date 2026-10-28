import sys

import pytest
from pydantic import ValidationError

from payment_processor.core.config import Settings
from payment_processor.messaging.retry import MAX_ATTEMPTS, RetryPolicy


@pytest.fixture
def settings_values(monkeypatch: pytest.MonkeyPatch) -> dict:
    monkeypatch.delenv("RETRY_BASE_DELAY", raising=False)
    return {
        "_env_file": None,
        "database_url": "postgresql+asyncpg://test:test@localhost/test",
        "rabbitmq_url": "amqp://test:test@localhost/",
        "api_key": "test-key",
    }


@pytest.mark.parametrize("base_delay", [0.001, 0.25, 1.0, 5.0, 60.0])
def test_failures_allow_exactly_two_retries_with_exponential_delays(
    base_delay: float,
) -> None:
    policy = RetryPolicy(base_delay=base_delay)
    attempt = 1
    attempts = [attempt]
    delays = []

    while (retry := policy.next_retry(attempt)) is not None:
        attempts.append(retry.number)
        delays.append(retry.delay)
        attempt = retry.number

    assert attempts == [1, 2, 3]
    assert MAX_ATTEMPTS == 3
    assert delays == [base_delay, 2 * base_delay]
    assert delays[1] > delays[0]


def test_third_failure_exhausts_retries() -> None:
    assert RetryPolicy(base_delay=1.0).next_retry(3) is None


@pytest.mark.parametrize("attempt", [0, -1, 4, 100, 1.0, 1.5, True, False, "1", None])
def test_invalid_attempts_are_rejected(attempt: object) -> None:
    with pytest.raises(ValueError, match="Attempt must be an integer between 1 and 3"):
        RetryPolicy(base_delay=1.0).next_retry(attempt)


@pytest.mark.parametrize(
    "base_delay",
    [
        0,
        -1,
        -0.1,
        float("nan"),
        float("inf"),
        float("-inf"),
        sys.float_info.max,
        True,
        False,
        "1.0",
        None,
    ],
)
def test_invalid_policy_delays_are_rejected(base_delay: object) -> None:
    with pytest.raises(ValueError, match="positive finite delays"):
        RetryPolicy(base_delay=base_delay)


def test_retry_settings_default_to_one_second(settings_values: dict) -> None:
    settings = Settings(**settings_values)

    retry = RetryPolicy(base_delay=settings.retry_base_delay).next_retry(1)

    assert settings.retry_base_delay == 1.0
    assert retry is not None
    assert retry.number == 2
    assert retry.delay == 1.0


def test_retry_settings_read_environment(
    settings_values: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("RETRY_BASE_DELAY", "2.5")

    settings = Settings(**settings_values)
    retry = RetryPolicy(base_delay=settings.retry_base_delay).next_retry(2)

    assert settings.retry_base_delay == 2.5
    assert retry is not None
    assert retry.number == 3
    assert retry.delay == 5.0


@pytest.mark.parametrize("value", ["0", "-0.1", "nan", "inf", "-inf", "invalid"])
def test_invalid_retry_environment_is_rejected(
    settings_values: dict, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("RETRY_BASE_DELAY", value)

    with pytest.raises(ValidationError) as caught:
        Settings(**settings_values)

    assert caught.value.errors()[0]["loc"] == ("retry_base_delay",)
