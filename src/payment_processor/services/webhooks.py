from math import isfinite
from types import TracebackType
from typing import Self

import httpx

from payment_processor.db.models import Payment
from payment_processor.messaging.schemas import PaymentResultPayload


class WebhookClient:
    def __init__(
        self,
        *,
        timeout: float,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not isfinite(timeout) or timeout <= 0:
            raise ValueError("Webhook timeout must be positive and finite")
        self._client = httpx.AsyncClient(
            timeout=timeout, transport=transport, follow_redirects=False
        )

    async def __aenter__(self) -> Self:
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._client.__aexit__(exc_type, exc_value, traceback)

    async def send(self, payment: Payment) -> None:
        payload = PaymentResultPayload.from_payment(payment)
        response = await self._client.post(
            payment.webhook_url, json=payload.model_dump(mode="json")
        )
        response.raise_for_status()
