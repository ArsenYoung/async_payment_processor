import asyncio
import json
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from payment_processor.adapters.webhooks import (
    WebhookClient,
    WebhookRequestTimeoutError,
)
from payment_processor.application.ports import WebhookSender
from payment_processor.application.results import PaymentDetailsResult
from payment_processor.domain.payment import Payment, PaymentCurrency, PaymentStatus

pytestmark = pytest.mark.unit


@pytest.fixture
def webhook_snapshot() -> PaymentDetailsResult:
    payment = Payment(
        id=uuid4(),
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Webhook port contract",
        payment_metadata={"order": [1, True, None]},
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
    )
    payment.process(PaymentStatus.FAILED)
    return PaymentDetailsResult.from_payment(payment)


@pytest.mark.asyncio
async def test_webhook_sender_accepts_plain_application_snapshot(
    webhook_snapshot: PaymentDetailsResult,
) -> None:
    requests = []

    def accept(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    async with WebhookClient(
        timeout=0.5, transport=httpx.MockTransport(accept)
    ) as client:
        sender: WebhookSender = client
        await sender.send(webhook_snapshot)
    assert len(requests) == 1
    request = requests[0]
    assert request.headers["X-Webhook-Delivery-Id"] == str(webhook_snapshot.payment_id)
    assert request.extensions["timeout"] == dict.fromkeys(
        ["connect", "read", "write", "pool"], 0.5
    )
    assert json.loads(request.content) == {
        "payment_id": str(webhook_snapshot.payment_id),
        "status": "failed",
        "amount": "10.50",
        "currency": "RUB",
        "description": webhook_snapshot.description,
        "metadata": webhook_snapshot.metadata,
        "created_at": webhook_snapshot.created_at.isoformat().replace("+00:00", "Z"),
        "processed_at": webhook_snapshot.processed_at.isoformat().replace(
            "+00:00", "Z"
        ),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["deadline", "cancellation"])
async def test_webhook_sender_cancels_transport_without_retry(
    webhook_snapshot: PaymentDetailsResult, outcome: str
) -> None:
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    calls = 0

    async def stalled(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    transport = httpx.MockTransport(stalled)
    async with WebhookClient(
        timeout=5,
        request_deadline=0.02 if outcome == "deadline" else 5,
        transport=transport,
    ) as client:
        sender: WebhookSender = client
        task = asyncio.create_task(sender.send(webhook_snapshot))
        try:
            async with asyncio.timeout(1):
                await entered.wait()
                if outcome == "cancellation":
                    task.cancel()
                with pytest.raises(
                    asyncio.CancelledError
                    if outcome == "cancellation"
                    else WebhookRequestTimeoutError
                ):
                    await task
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert calls == 1
    assert cancelled.is_set()
    assert client._client.is_closed


@pytest.mark.parametrize("deadline", [0, -1, float("inf"), float("nan")])
def test_webhook_sender_rejects_invalid_overall_deadline(deadline: float) -> None:
    with pytest.raises(ValueError, match="positive and finite"):
        WebhookClient(timeout=1, request_deadline=deadline)
