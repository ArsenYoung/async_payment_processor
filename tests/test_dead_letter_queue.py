import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from time import monotonic
from unittest.mock import AsyncMock

import httpx
import pytest
from aio_pika import DeliveryMode
from aio_pika.abc import AbstractIncomingMessage
from aio_pika.exceptions import PublishError
from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from payment_processor.adapters.payment_gateway import PaymentGatewaySimulator
from payment_processor.adapters.webhooks import WebhookClient
from payment_processor.bootstrap import create_payment_service
from payment_processor.db.models import Outbox, PaymentStatus
from payment_processor.db.session import Database
from payment_processor.messaging import topology
from payment_processor.messaging.retry import ATTEMPT_HEADER
from payment_processor.messaging.schemas import (
    PaymentCreatedEvent,
    PaymentResultPayload,
)


async def receive_dead_letter(broker: RabbitBroker) -> AbstractIncomingMessage:
    queue = await broker.declare_queue(topology.dead_letter_queue())
    async with asyncio.timeout(10):
        while (message := await queue.get(fail=False, timeout=1)) is None:
            await asyncio.sleep(0.02)
    return message


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
@pytest.mark.parametrize("failure", ["http", "connection", "timeout"])
async def test_webhook_exhaustion_preserves_result_and_dead_letter_diagnostics(
    outbox_database: Database,
    routing_broker: RabbitBroker,
    retry_event: PaymentCreatedEvent,
    retry_consumer: Callable,
    status: PaymentStatus,
    failure: str,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = create_payment_service(outbox_database.session_scope, gateway=gateway)
    requests: list[httpx.Request] = []
    call_times: list[float] = []
    initial = await service.get_payment(retry_event.payload.payment_id)
    assert initial is not None

    def fail(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        call_times.append(monotonic())
        if failure == "http":
            return httpx.Response(503)
        error = httpx.ConnectError if failure == "connection" else httpx.ReadTimeout
        raise error("Private webhook failure details", request=request)

    started = datetime.now(UTC)
    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(fail)
    ) as webhook:
        async with retry_consumer(service, webhook) as (_, deliveries):
            await routing_broker.publish(
                retry_event.model_dump(mode="json"),
                exchange=topology.PAYMENTS_EXCHANGE,
                routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
                message_id=str(retry_event.event_id),
                message_type=retry_event.event_type.value,
                correlation_id="retry-diagnostics",
                headers={"custom": "preserved"},
                content_type="application/json",
                persist=True,
                mandatory=True,
            )
            message = await receive_dead_letter(routing_broker)
            async with message.process():
                assert message.body == deliveries[0].body
                assert (
                    PaymentCreatedEvent.model_validate_json(message.body) == retry_event
                )
                assert message.message_id == str(retry_event.event_id)
                assert message.type == retry_event.event_type.value
                assert message.correlation_id == "retry-diagnostics"
                assert message.content_type == "application/json"
                assert message.delivery_mode == DeliveryMode.PERSISTENT
                assert message.expiration is None
                assert message.headers["custom"] == "preserved"
                assert message.headers[ATTEMPT_HEADER] == 3
                assert message.headers["x-payment-failed-attempt"] == 3
                assert message.headers["x-payment-error-reason"] == "attempts_exhausted"
                assert (
                    message.headers["x-payment-error-type"]
                    == {
                        "http": "HTTPStatusError",
                        "connection": "ConnectError",
                        "timeout": "ReadTimeout",
                    }[failure]
                )
                diagnosed = datetime.fromisoformat(
                    message.headers["x-payment-error-at"]
                )
                assert diagnosed.utcoffset().total_seconds() == 0
                assert started <= diagnosed <= datetime.now(UTC)
                if failure == "http":
                    assert message.headers["x-payment-error-http-status"] == 503
                else:
                    assert "x-payment-error-http-status" not in message.headers
                assert "x-payment-error-sqlstate" not in message.headers
                assert "Private webhook failure details" not in str(message.headers)
                assert {
                    death["queue"]: death["reason"]
                    for death in message.headers["x-death"]
                } == {
                    topology.retry_queue(2).name: "expired",
                    topology.retry_queue(3).name: "expired",
                }

    gateway.process.assert_awaited_once_with()
    assert len(requests) == 3
    assert [message.headers.get(ATTEMPT_HEADER, 1) for message in deliveries] == [
        1,
        2,
        3,
    ]
    assert call_times[1] - call_times[0] >= 0.09
    assert call_times[2] - call_times[1] >= 0.18
    assert all(request.content == requests[0].content for request in requests)
    payment = await service.get_payment(initial.payment_id)
    assert payment is not None
    assert payment.status == status
    assert payment.created_at == initial.created_at
    assert (
        payment.processed_at
        == PaymentResultPayload.model_validate_json(requests[0].content).processed_at
    )
    assert payment.webhook_delivered_at is None
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


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
@pytest.mark.parametrize("attempt", [1, 3], ids=["retry", "dlq"])
@pytest.mark.parametrize("failure", ["connection", "unroutable"])
async def test_failed_transfer_recovers_original_without_repeating_gateway(
    outbox_database: Database,
    routing_broker: RabbitBroker,
    retry_event: PaymentCreatedEvent,
    retry_consumer: Callable,
    monkeypatch: pytest.MonkeyPatch,
    status: PaymentStatus,
    attempt: int,
    failure: str,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = create_payment_service(outbox_database.session_scope, gateway=gateway)
    requests: list[httpx.Request] = []
    transfer_errors: list[Exception] = []
    definition = (
        topology.retry_queue(2) if attempt == 1 else topology.dead_letter_queue()
    )
    exchange = await routing_broker.declare_exchange(
        topology.retry_exchange() if attempt == 1 else topology.dead_letter_exchange()
    )
    queue = await routing_broker.declare_queue(definition)

    def fail(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(503)

    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(fail)
    ) as webhook:
        async with retry_consumer(service, webhook) as (consumer, deliveries):
            publish = consumer.publish

            async def interrupt_once(*args: object, **kwargs: object) -> object:
                if transfer_errors:
                    return await publish(*args, **kwargs)
                try:
                    if failure == "connection":
                        raise ConnectionError("Broker connection lost")
                    await queue.unbind(exchange, routing_key=definition.routing())
                    return await publish(*args, **kwargs)
                except (ConnectionError, PublishError) as error:
                    transfer_errors.append(error)
                    raise
                finally:
                    if failure == "unroutable":
                        await queue.bind(exchange, routing_key=definition.routing())

            monkeypatch.setattr(consumer, "publish", interrupt_once)
            await routing_broker.publish(
                retry_event.model_dump(mode="json"),
                exchange=topology.PAYMENTS_EXCHANGE,
                routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
                message_id=str(retry_event.event_id),
                headers={ATTEMPT_HEADER: attempt, "custom": "preserved"},
                persist=True,
                mandatory=True,
            )
            message = await receive_dead_letter(routing_broker)
            async with message.process():
                assert message.body == deliveries[0].body
                assert message.message_id == str(retry_event.event_id)
                assert message.headers[ATTEMPT_HEADER] == 3
                assert message.headers["x-payment-failed-attempt"] == 3
                assert message.headers["x-payment-error-reason"] == "attempts_exhausted"
                assert message.headers["x-payment-error-http-status"] == 503

    assert len(transfer_errors) == 1
    assert isinstance(
        transfer_errors[0], ConnectionError if failure == "connection" else PublishError
    )
    assert [message.headers[ATTEMPT_HEADER] for message in deliveries] == (
        [1, 1, 2, 3] if attempt == 1 else [3, 3]
    )
    assert deliveries[0].redelivered is False
    assert deliveries[1].redelivered is True
    assert deliveries[1].body == deliveries[0].body
    assert deliveries[1].headers == deliveries[0].headers
    assert all(
        message.message_id == str(retry_event.event_id) for message in deliveries
    )
    gateway.process.assert_awaited_once_with()
    assert len(requests) == (4 if attempt == 1 else 2)
    assert all(request.content == requests[0].content for request in requests)
    payment = await service.get_payment(retry_event.payload.payment_id)
    assert payment is not None
    assert payment.status == status
    assert (
        payment.processed_at
        == PaymentResultPayload.model_validate_json(requests[0].content).processed_at
    )
    assert payment.webhook_delivered_at is None
    for definition in (
        topology.PAYMENTS_NEW_QUEUE,
        topology.retry_queue(2),
        topology.retry_queue(3),
        topology.dead_letter_queue(),
    ):
        queue = await routing_broker.declare_queue(definition)
        assert await queue.get(fail=False, timeout=1) is None
