import asyncio
import logging
from math import isfinite
from time import perf_counter
from types import TracebackType
from typing import Self

import httpx

from payment_processor.core.logging import processing_fields
from payment_processor.db.models import Payment
from payment_processor.messaging.schemas import PaymentResultPayload
from payment_processor.services.errors import WebhookRequestTimeoutError

logger = logging.getLogger(__name__)


class WebhookClient:
    def __init__(
        self,
        *,
        timeout: float,
        request_deadline: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not isfinite(timeout) or timeout <= 0:
            raise ValueError("Webhook timeout must be positive and finite")
        if request_deadline is None:
            request_deadline = timeout
        if not isfinite(request_deadline) or request_deadline <= 0:
            raise ValueError("Webhook request deadline must be positive and finite")
        self._request_deadline = request_deadline
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
        started = perf_counter()
        fields = processing_fields(payment_id=str(payment.id), operation="webhook_http")
        try:
            payload = PaymentResultPayload.from_payment(payment)
            try:
                async with asyncio.timeout(self._request_deadline):
                    async with self._client.stream(
                        "POST",
                        payment.webhook_url,
                        json=payload.model_dump(mode="json"),
                    ) as response:
                        fields["status_code"] = response.status_code
                        response.raise_for_status()
            except TimeoutError as error:
                raise WebhookRequestTimeoutError(
                    f"Webhook request exceeded {self._request_deadline} seconds"
                ) from error
        except Exception as error:
            logger.warning(
                "Webhook HTTP request failed",
                extra={
                    **fields,
                    "outcome": "failed",
                    "duration_ms": (perf_counter() - started) * 1000,
                    "error_type": type(error).__name__,
                },
                exc_info=True,
            )
            raise
        logger.info(
            "Webhook HTTP request completed",
            extra={
                **fields,
                "outcome": "accepted",
                "duration_ms": (perf_counter() - started) * 1000,
            },
        )
