import asyncio
import random
from collections.abc import Awaitable, Callable

from payment_processor.domain.payment import PaymentStatus

RandomValue = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]


class PaymentGatewaySimulator:
    """Simulate an external payment gateway for the payment processor."""

    def __init__(
        self,
        *,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        success_rate: float = 0.9,
        random_value: RandomValue = random.random,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if min_delay < 0 or max_delay < min_delay:
            raise ValueError("Payment gateway delay range is invalid")
        if not 0 <= success_rate <= 1:
            raise ValueError("Payment gateway success rate must be between 0 and 1")

        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._random_value = random_value
        self._sleep = sleep

    async def process(self) -> PaymentStatus:
        delay_random_value = self._random_value()
        if not 0 <= delay_random_value <= 1:
            raise ValueError("Random value must be between 0 and 1")

        delay = (
            self._min_delay + (self._max_delay - self._min_delay) * delay_random_value
        )
        await self._sleep(delay)

        outcome_random_value = self._random_value()
        if not 0 <= outcome_random_value <= 1:
            raise ValueError("Random value must be between 0 and 1")

        if outcome_random_value < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
