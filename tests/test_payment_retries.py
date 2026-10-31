import asyncio
import json
from collections.abc import Callable
from time import monotonic
from unittest.mock import AsyncMock

import httpx
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from payment_processor.db.models import Outbox, PaymentStatus
from payment_processor.db.session import Database
from payment_processor.messaging import topology
from payment_processor.messaging.retry import ATTEMPT_HEADER
from payment_processor.messaging.schemas import (
    PaymentCreatedEvent,
    PaymentResultPayload,
)
from payment_processor.services.errors import WebhookRequestTimeoutError
from payment_processor.services.payment_gateway_simulator import PaymentGatewaySimulator
from payment_processor.services.payment_service import PaymentService
from payment_processor.services.webhooks import WebhookClient


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
@pytest.mark.parametrize("failure", ["http", "connection", "timeout", "deadline"])
@pytest.mark.parametrize("successful_attempt", [2, 3])
async def test_automatic_webhook_retry_preserves_result_and_recovers(
    outbox_database: Database,
    routing_broker: RabbitBroker,
    retry_event: PaymentCreatedEvent,
    retry_consumer: Callable,
    status: PaymentStatus,
    failure: str,
    successful_attempt: int,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    requests: list[httpx.Request] = []
    call_times: list[float] = []
    initial = await service.get_payment(retry_event.payload.payment_id)
    assert initial is not None

    async def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        call_times.append(monotonic())
        payment = await service.get_payment(initial.payment_id)
        assert payment is not None
        assert payment.status == status
        assert payment.processed_at is not None
        assert payment.webhook_delivered_at is None
        assert str(request.url) == payment.webhook_url
        assert json.loads(request.content) == PaymentResultPayload.from_result(
            payment
        ).model_dump(mode="json")
        if len(requests) < successful_attempt:
            if failure == "http":
                return httpx.Response(503)
            if failure == "deadline":
                raise WebhookRequestTimeoutError("Webhook request deadline exceeded")
            error = httpx.ConnectError if failure == "connection" else httpx.ReadTimeout
            raise error("Temporary webhook failure", request=request)
        return httpx.Response(204)

    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(handle)
    ) as webhook:
        async with retry_consumer(service, webhook) as (_, deliveries):
            await topology.publish_payment_event(routing_broker, retry_event)
            async with asyncio.timeout(10):
                while True:
                    delivered = await service.get_payment(initial.payment_id)
                    assert delivered is not None
                    if delivered.webhook_delivered_at is not None:
                        break
                    await asyncio.sleep(0.02)

    gateway.process.assert_awaited_once_with()
    assert len(requests) == successful_attempt
    assert [message.headers.get(ATTEMPT_HEADER, 1) for message in deliveries] == list(
        range(1, successful_attempt + 1)
    )
    assert all(
        message.message_id == str(retry_event.event_id) for message in deliveries
    )
    assert all(request.content == requests[0].content for request in requests)
    for index in range(1, successful_attempt):
        assert call_times[index] - call_times[index - 1] >= 0.09 * 2 ** (index - 1)
    result = PaymentResultPayload.model_validate_json(requests[0].content)
    assert delivered.status == status
    assert delivered.processed_at == result.processed_at
    assert delivered.created_at == initial.created_at
    assert delivered.webhook_delivered_at >= delivered.processed_at

    for definition in (
        topology.PAYMENTS_NEW_QUEUE,
        topology.retry_queue(2),
        topology.retry_queue(3),
        topology.dead_letter_queue(),
    ):
        queue = await routing_broker.declare_queue(definition)
        assert await queue.get(fail=False, timeout=1) is None
    async with outbox_database.session_scope() as session:
        outbox = await session.scalar(
            select(Outbox).where(Outbox.id == retry_event.event_id)
        )
        assert outbox is not None
        assert outbox.attempts == 0
        assert outbox.last_error is None
