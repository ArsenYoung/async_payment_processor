from unittest.mock import AsyncMock, Mock

import pytest

from payment_processor.adapters.payment_gateway import PaymentGatewaySimulator
from payment_processor.db.models import PaymentStatus


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delay_random_value", "expected_delay"),
    [(0.0, 2.0), (1.0, 5.0)],
)
async def test_gateway_uses_delay_range(
    delay_random_value: float, expected_delay: float
) -> None:
    sleep = AsyncMock()
    random_value = Mock(side_effect=[delay_random_value, 0.0])
    gateway = PaymentGatewaySimulator(random_value=random_value, sleep=sleep)

    result = await gateway.process()

    assert result == PaymentStatus.SUCCEEDED
    sleep.assert_awaited_once_with(expected_delay)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome_random_value", "expected_status"),
    [
        (0.0, PaymentStatus.SUCCEEDED),
        (0.899999, PaymentStatus.SUCCEEDED),
        (0.9, PaymentStatus.FAILED),
        (1.0, PaymentStatus.FAILED),
    ],
)
async def test_gateway_uses_ninety_percent_success_rate(
    outcome_random_value: float, expected_status: PaymentStatus
) -> None:
    sleep = AsyncMock()
    random_value = Mock(side_effect=[0.5, outcome_random_value])
    gateway = PaymentGatewaySimulator(random_value=random_value, sleep=sleep)

    result = await gateway.process()

    assert result == expected_status
    sleep.assert_awaited_once()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"min_delay": -1.0},
            "Payment gateway delay range is invalid",
        ),
        (
            {"min_delay": 5.0, "max_delay": 2.0},
            "Payment gateway delay range is invalid",
        ),
        (
            {"success_rate": -0.1},
            "Payment gateway success rate must be between 0 and 1",
        ),
        (
            {"success_rate": 1.1},
            "Payment gateway success rate must be between 0 and 1",
        ),
    ],
)
def test_gateway_rejects_invalid_configuration(
    kwargs: dict[str, float], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        PaymentGatewaySimulator(**kwargs)
