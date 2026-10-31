import asyncio
from datetime import datetime
from time import monotonic
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from aio_pika import DeliveryMode
from aio_pika.abc import AbstractIncomingMessage
from aio_pika.exceptions import DeliveryError, PublishError
from faststream.rabbit import QueueType, RabbitBroker, RabbitQueue
from faststream.rabbit import TestRabbitBroker as RabbitTestBroker
from faststream.rabbit.message import RabbitMessage
from faststream.rabbit.parser import AioPikaParser
from faststream.rabbit.testing import PatchedMessage, build_message
from pamqp.commands import Basic
from sqlalchemy.exc import OperationalError

from payment_processor.adapters.webhooks import WebhookClient
from payment_processor.messaging import topology
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.retry import (
    ATTEMPT_HEADER,
    RetryAttempt,
    RetryPolicy,
    failure_headers,
    read_attempt,
)
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.rabbitmq_to_payment_worker.handlers import (
    register_payment_subscriber,
)
from payment_processor.services.payment_service import PaymentService


@pytest.fixture(autouse=True)
def unlocked_test_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PatchedMessage, "locked", property(lambda self: False))


@pytest.fixture
def payment_event(payment_payload: dict) -> PaymentCreatedEvent:
    return PaymentCreatedEvent.model_validate(
        {
            "event_id": str(uuid4()),
            "event_type": "payment.created",
            "payload": {"payment_id": str(uuid4()), **payment_payload},
        }
    )


@pytest_asyncio.fixture
async def payment_message(payment_event: PaymentCreatedEvent) -> RabbitMessage:
    incoming = await build_message(
        payment_event.model_dump(mode="json"),
        message_id=str(payment_event.event_id),
        message_type=payment_event.event_type.value,
        correlation_id="test-correlation",
        headers={"custom": "preserved"},
    )
    return await AioPikaParser().parse_message(incoming)


async def receive(
    broker: RabbitBroker, definition: RabbitQueue, *, timeout: float = 10
) -> AbstractIncomingMessage:
    queue = await broker.declare_queue(definition)
    async with asyncio.timeout(timeout):
        while (message := await queue.get(fail=False, timeout=1)) is None:
            await asyncio.sleep(0.05)
    return message


@pytest.mark.parametrize(
    "headers, expected",
    [
        ({}, 1),
        ({ATTEMPT_HEADER: 1}, 1),
        ({ATTEMPT_HEADER: 2}, 2),
        ({ATTEMPT_HEADER: 3}, 3),
    ],
)
def test_attempt_header_accepts_only_valid_attempts(
    headers: dict, expected: int
) -> None:
    assert read_attempt(headers) == expected


@pytest.mark.parametrize("attempt", [0, -1, 4, True, False, "2", 2.0, None, [], {}])
def test_invalid_attempt_header_is_rejected(attempt: object) -> None:
    with pytest.raises(ValueError, match="Attempt must be an integer"):
        read_attempt({ATTEMPT_HEADER: attempt})


def test_retry_topology_uses_durable_routes_and_confirmed_dead_lettering() -> None:
    for attempt in (2, 3):
        queue = topology.retry_queue(attempt)
        assert queue.name == f"payments.retry.{attempt}"
        assert queue.durable is True
        assert queue.auto_delete is False
        assert queue.arguments["x-queue-type"] == QueueType.QUORUM.value
        assert queue.arguments["x-dead-letter-strategy"] == "at-least-once"
        assert queue.arguments["x-overflow"] == "reject-publish"
        assert (
            queue.arguments["x-dead-letter-exchange"] == topology.PAYMENTS_EXCHANGE.name
        )
        assert (
            queue.arguments["x-dead-letter-routing-key"]
            == topology.PAYMENTS_NEW_QUEUE.routing()
        )
    assert topology.retry_exchange().durable is True
    assert topology.dead_letter_exchange().durable is True
    assert topology.dead_letter_queue().name == "payments.dlq"
    assert topology.dead_letter_queue().durable is True


def test_diagnostics_preserve_headers_without_copying_sensitive_exception_text() -> (
    None
):
    request = httpx.Request("POST", "https://secret.example/path?token=secret")
    error = httpx.HTTPStatusError(
        "sensitive response body",
        request=request,
        response=httpx.Response(503, request=request),
    )
    headers = failure_headers(
        {"custom": "preserved", "x-payment-error-sqlstate": "40001"},
        attempt=2,
        error=error,
        reason="retryable_processing_error",
    )
    assert headers["custom"] == "preserved"
    assert headers[ATTEMPT_HEADER] == 2
    assert headers["x-payment-error-type"] == "HTTPStatusError"
    assert headers["x-payment-error-http-status"] == 503
    assert "x-payment-error-sqlstate" not in headers
    assert (
        datetime.fromisoformat(headers["x-payment-error-at"])
        .utcoffset()
        .total_seconds()
        == 0
    )
    assert "secret" not in str(headers)
    assert "sensitive" not in str(headers)


def test_database_diagnostics_include_sqlstate_and_remove_stale_http_status() -> None:
    original = RuntimeError("password=secret")
    original.sqlstate = "40001"
    error = OperationalError("secret SQL", {}, original)
    headers = failure_headers(
        {"x-payment-error-http-status": 503},
        attempt=3,
        error=error,
        reason="attempts_exhausted",
    )
    assert headers["x-payment-error-sqlstate"] == "40001"
    assert "x-payment-error-http-status" not in headers
    assert "secret" not in str(headers)


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [RetryAttempt(2, 0.1), RetryAttempt(3, 0.2), None])
async def test_failed_publication_preserves_identity_and_persistent_delivery(
    payment_message: RabbitMessage, retry: RetryAttempt | None
) -> None:
    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(return_value=Basic.Ack())
    attempt = retry.number - 1 if retry else 3
    await topology.publish_failed_payment(
        broker,
        payment_message,
        attempt=attempt,
        error=TimeoutError("secret"),
        reason="attempts_exhausted" if retry is None else "retryable_processing_error",
        retry=retry,
    )
    args = broker.publish.await_args
    assert args.args == (payment_message.body,)
    assert args.kwargs["message_id"] == payment_message.message_id
    assert args.kwargs["message_type"] == "payment.created"
    assert args.kwargs["correlation_id"] == "test-correlation"
    assert args.kwargs["content_type"] == "application/json"
    assert args.kwargs["headers"]["custom"] == "preserved"
    assert args.kwargs["headers"][ATTEMPT_HEADER] == (retry.number if retry else 3)
    assert args.kwargs["headers"]["x-payment-failed-attempt"] == attempt
    assert args.kwargs["persist"] is True
    assert args.kwargs["mandatory"] is True
    assert args.kwargs["expiration"] == (retry.delay if retry else None)


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [RetryAttempt(2, 0.1), None])
@pytest.mark.parametrize("confirmation", [None, Basic.Nack(), Basic.Reject()])
async def test_failed_publication_requires_positive_confirmation(
    payment_message: RabbitMessage, retry: RetryAttempt | None, confirmation: object
) -> None:
    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(return_value=confirmation)
    with pytest.raises(RuntimeError, match="did not acknowledge"):
        await topology.publish_failed_payment(
            broker,
            payment_message,
            attempt=3,
            error=TimeoutError(),
            reason="test",
            retry=retry,
        )


@pytest.mark.asyncio
async def test_failed_publication_has_a_bounded_confirmation_wait(
    payment_message: RabbitMessage,
) -> None:
    cancelled = asyncio.Event()

    async def stalled(*args: object, **kwargs: object) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(side_effect=stalled)
    async with asyncio.timeout(1):
        with pytest.raises(TimeoutError):
            await topology.publish_failed_payment(
                broker,
                payment_message,
                attempt=1,
                error=TimeoutError(),
                reason="test",
                retry=RetryAttempt(2, 0.1),
                timeout=0.01,
            )
    assert cancelled.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["processing", "delivery"])
@pytest.mark.parametrize(
    "max_attempts,attempt", [(3, 1), (3, 2), (3, 3), (1, 1), (5, 4), (5, 5)]
)
async def test_subscriber_acks_only_after_confirmed_transfer(
    payment_event: PaymentCreatedEvent,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    max_attempts: int,
    attempt: int,
) -> None:
    service = Mock(spec=PaymentService)
    operation = (
        service.process_payment if stage == "processing" else service.deliver_webhook
    )
    operation.side_effect = TimeoutError("Unavailable")
    broker = create_broker()
    register_payment_subscriber(
        broker,
        service,
        Mock(spec=WebhookClient),
        retry_policy=RetryPolicy(base_delay=0.1, max_attempts=max_attempts),
    )
    operations = []

    async def confirm(*args: object, **kwargs: object) -> Basic.Ack:
        operations.append("confirmed")
        return Basic.Ack()

    async def acknowledge(*args: object, **kwargs: object) -> None:
        operations.append("ack")

    ack = AsyncMock(side_effect=acknowledge)
    nack = AsyncMock()
    monkeypatch.setattr(PatchedMessage, "ack", ack)
    monkeypatch.setattr(PatchedMessage, "nack", nack)
    async with RabbitTestBroker(broker):
        publish = broker.publish
        transfer = AsyncMock(side_effect=confirm)
        monkeypatch.setattr(broker, "publish", transfer)
        await publish(
            payment_event.model_dump(mode="json"),
            exchange=topology.PAYMENTS_EXCHANGE,
            routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
            headers={ATTEMPT_HEADER: attempt},
        )
    assert operations == ["confirmed", "ack"]
    nack.assert_not_awaited()
    assert transfer.await_args.kwargs["routing_key"] == (
        f"payment.retry.{attempt + 1}" if attempt < max_attempts else "payment.dead"
    )
    assert transfer.await_args.kwargs["headers"][ATTEMPT_HEADER] == min(
        attempt + 1, max_attempts
    )
    assert transfer.await_args.kwargs["headers"]["x-payment-failed-attempt"] == attempt


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [1, 3])
@pytest.mark.parametrize(
    "failure",
    [
        None,
        Basic.Nack(),
        TimeoutError("Confirm timeout"),
        ConnectionError("Disconnected"),
        DeliveryError(None, Basic.Nack()),
    ],
)
async def test_transfer_failure_nacks_original_without_ack(
    payment_event: PaymentCreatedEvent,
    monkeypatch: pytest.MonkeyPatch,
    attempt: int,
    failure: object,
) -> None:
    service = Mock(spec=PaymentService)
    service.process_payment.side_effect = TimeoutError("Processing failed")
    broker = create_broker()
    register_payment_subscriber(
        broker,
        service,
        Mock(spec=WebhookClient),
        retry_policy=RetryPolicy(base_delay=0.1),
    )
    ack, nack = AsyncMock(), AsyncMock()
    monkeypatch.setattr(PatchedMessage, "ack", ack)
    monkeypatch.setattr(PatchedMessage, "nack", nack)
    async with RabbitTestBroker(broker):
        publish = broker.publish
        transfer = (
            AsyncMock(side_effect=failure)
            if isinstance(failure, Exception)
            else AsyncMock(return_value=failure)
        )
        monkeypatch.setattr(broker, "publish", transfer)
        error_type = type(failure) if isinstance(failure, Exception) else RuntimeError
        with pytest.raises(error_type):
            await publish(
                payment_event.model_dump(mode="json"),
                exchange=topology.PAYMENTS_EXCHANGE,
                routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
                headers={ATTEMPT_HEADER: attempt},
            )
    ack.assert_not_awaited()
    nack.assert_awaited_once_with(multiple=False, requeue=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body,headers",
    [
        (b"{invalid json", {}),
        (b"{}", {}),
        (None, {ATTEMPT_HEADER: "2"}),
        (None, {ATTEMPT_HEADER: 4}),
    ],
)
async def test_invalid_messages_reach_dlq_without_processing(
    payment_event: PaymentCreatedEvent,
    monkeypatch: pytest.MonkeyPatch,
    body: bytes | None,
    headers: dict,
) -> None:
    service = Mock(spec=PaymentService)
    broker = create_broker()
    register_payment_subscriber(
        broker,
        service,
        Mock(spec=WebhookClient),
        retry_policy=RetryPolicy(base_delay=0.1),
    )
    async with RabbitTestBroker(broker):
        publish = broker.publish
        transfer = AsyncMock(return_value=Basic.Ack())
        monkeypatch.setattr(broker, "publish", transfer)
        await publish(
            body if body is not None else payment_event.model_dump(mode="json"),
            exchange=topology.PAYMENTS_EXCHANGE,
            routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
            content_type="application/json",
            headers=headers,
        )
    service.process_payment.assert_not_awaited()
    service.deliver_webhook.assert_not_awaited()
    assert transfer.await_args.kwargs["routing_key"] == "payment.dead"
    assert (
        transfer.await_args.kwargs["headers"]["x-payment-error-reason"]
        == "invalid_message"
    )
    if body is not None:
        assert transfer.await_args.args[0] == body
    if ATTEMPT_HEADER in headers:
        assert (
            transfer.await_args.kwargs["headers"]["x-payment-invalid-attempt"]
            == headers[ATTEMPT_HEADER]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["processing", "transfer"])
async def test_cancellation_does_not_ack_or_route_as_processing_failure(
    payment_event: PaymentCreatedEvent, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    service = Mock(spec=PaymentService)
    service.process_payment.side_effect = (
        asyncio.CancelledError() if stage == "processing" else TimeoutError()
    )
    broker = create_broker()
    register_payment_subscriber(
        broker,
        service,
        Mock(spec=WebhookClient),
        retry_policy=RetryPolicy(base_delay=0.1),
    )
    ack, nack = AsyncMock(), AsyncMock()
    monkeypatch.setattr(PatchedMessage, "ack", ack)
    monkeypatch.setattr(PatchedMessage, "nack", nack)
    async with RabbitTestBroker(broker):
        publish = broker.publish
        transfer = AsyncMock(side_effect=asyncio.CancelledError())
        monkeypatch.setattr(broker, "publish", transfer)
        with pytest.raises(asyncio.CancelledError):
            await publish(
                payment_event.model_dump(mode="json"),
                exchange=topology.PAYMENTS_EXCHANGE,
                routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
            )
    ack.assert_not_awaited()
    nack.assert_not_awaited()
    assert transfer.await_count == (0 if stage == "processing" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt,delay", [(2, 0.1), (3, 0.2)])
async def test_real_retry_waits_then_returns_persistent_event(
    routing_broker: RabbitBroker,
    payment_message: RabbitMessage,
    attempt: int,
    delay: float,
) -> None:
    await topology.declare_retry_topology(routing_broker)
    started = monotonic()
    await topology.publish_failed_payment(
        routing_broker,
        payment_message,
        attempt=attempt - 1,
        error=TimeoutError(),
        reason="retryable_processing_error",
        retry=RetryAttempt(attempt, delay),
    )
    message = await receive(routing_broker, topology.PAYMENTS_NEW_QUEUE)
    async with message.process():
        assert monotonic() - started >= delay * 0.9
        assert message.body == payment_message.body
        assert message.message_id == payment_message.message_id
        assert message.type == "payment.created"
        assert message.correlation_id == "test-correlation"
        assert message.delivery_mode == DeliveryMode.PERSISTENT
        assert message.headers[ATTEMPT_HEADER] == attempt
        assert message.headers["custom"] == "preserved"
        assert message.headers["x-death"][0]["reason"] == "expired"
        assert message.expiration is None


@pytest.mark.asyncio
async def test_real_subscriber_exhausts_three_attempts_then_routes_to_dlq(
    routing_broker: RabbitBroker, payment_event: PaymentCreatedEvent
) -> None:
    service = Mock(spec=PaymentService)
    calls = []

    async def fail(*args: object) -> None:
        calls.append(monotonic())
        raise TimeoutError("Unavailable")

    service.process_payment.side_effect = fail
    register_payment_subscriber(
        routing_broker,
        service,
        Mock(spec=WebhookClient),
        retry_policy=RetryPolicy(base_delay=0.1),
    )
    await routing_broker.start()
    await topology.publish_payment_event(routing_broker, payment_event)
    message = await receive(routing_broker, topology.dead_letter_queue())
    async with message.process():
        assert PaymentCreatedEvent.model_validate_json(message.body) == payment_event
        assert message.message_id == str(payment_event.event_id)
        assert message.headers[ATTEMPT_HEADER] == 3
        assert message.headers["x-payment-error-type"] == "TimeoutError"
        assert message.headers["x-payment-error-reason"] == "attempts_exhausted"
        assert message.delivery_mode == DeliveryMode.PERSISTENT
    assert len(calls) == 3
    assert calls[1] - calls[0] >= 0.09
    assert calls[2] - calls[1] >= 0.18
    service.deliver_webhook.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [RetryAttempt(2, 0.1), None])
async def test_real_unroutable_transfer_is_rejected(
    routing_broker: RabbitBroker,
    payment_message: RabbitMessage,
    retry: RetryAttempt | None,
) -> None:
    definition = topology.retry_queue(2) if retry else topology.dead_letter_queue()
    exchange = await routing_broker.declare_exchange(
        topology.retry_exchange() if retry else topology.dead_letter_exchange()
    )
    queue = await routing_broker.declare_queue(definition)
    await queue.unbind(exchange, routing_key=definition.routing())
    with pytest.raises(PublishError):
        await topology.publish_failed_payment(
            routing_broker,
            payment_message,
            attempt=3,
            error=TimeoutError(),
            reason="test",
            retry=retry,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", [1, 3])
async def test_real_transfer_failure_preserves_original_for_redelivery(
    routing_broker: RabbitBroker,
    payment_event: PaymentCreatedEvent,
    monkeypatch: pytest.MonkeyPatch,
    attempt: int,
) -> None:
    redelivered = asyncio.Event()
    release = asyncio.Event()
    service = Mock(spec=PaymentService)

    async def fail_or_wait(*args: object) -> None:
        if service.process_payment.await_count == 1:
            raise TimeoutError("Processing unavailable")
        redelivered.set()
        await release.wait()
        raise asyncio.CancelledError()

    service.process_payment.side_effect = fail_or_wait
    transfer = AsyncMock(side_effect=ConnectionError("Transfer unavailable"))
    monkeypatch.setattr(topology, "publish_failed_payment", transfer)
    consumer = create_broker()
    register_payment_subscriber(
        consumer,
        service,
        Mock(spec=WebhookClient),
        retry_policy=RetryPolicy(base_delay=0.1),
    )
    try:
        await consumer.connect()
        await consumer.start()
        await routing_broker.publish(
            payment_event.model_dump(mode="json"),
            exchange=topology.PAYMENTS_EXCHANGE,
            routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
            headers={ATTEMPT_HEADER: attempt},
            message_id=str(payment_event.event_id),
            persist=True,
        )
        async with asyncio.timeout(5):
            await redelivered.wait()
    finally:
        release.set()
        await consumer.stop()

    transfer.assert_awaited_once()
    assert service.process_payment.await_count == 2
    message = await receive(routing_broker, topology.PAYMENTS_NEW_QUEUE)
    async with message.process():
        assert message.redelivered is True
        assert PaymentCreatedEvent.model_validate_json(message.body) == payment_event
        assert message.message_id == str(payment_event.event_id)
        assert message.headers[ATTEMPT_HEADER] == attempt


@pytest.mark.asyncio
async def test_expired_retry_waits_for_main_route_to_be_restored(
    routing_broker: RabbitBroker, payment_message: RabbitMessage
) -> None:
    queue = await routing_broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    exchange = await routing_broker.declare_exchange(topology.PAYMENTS_EXCHANGE)
    routing_key = topology.PAYMENTS_NEW_QUEUE.routing()
    await queue.unbind(exchange, routing_key=routing_key)
    try:
        await topology.publish_failed_payment(
            routing_broker,
            payment_message,
            attempt=1,
            error=TimeoutError(),
            reason="retryable_processing_error",
            retry=RetryAttempt(2, 0.05),
        )
        await asyncio.sleep(0.15)
        assert await queue.get(fail=False, timeout=1) is None
    finally:
        await queue.bind(exchange, routing_key=routing_key)

    message = await receive(routing_broker, topology.PAYMENTS_NEW_QUEUE, timeout=190)
    async with message.process():
        assert message.body == payment_message.body
        assert message.message_id == payment_message.message_id
        assert message.headers[ATTEMPT_HEADER] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("max_attempts", [1, 5])
async def test_topology_declares_queues_for_configured_policy(
    max_attempts: int,
) -> None:
    broker = Mock()
    broker.declare_exchange = AsyncMock()
    broker.declare_queue = AsyncMock()
    await topology.declare_retry_topology(
        broker, RetryPolicy(base_delay=1, max_attempts=max_attempts)
    )
    assert [call.args[0].name for call in broker.declare_queue.call_args_list] == [
        *[f"payments.retry.{attempt}" for attempt in range(2, max_attempts + 1)],
        "payments.dlq",
    ]


def test_custom_attempt_limit_is_used_for_headers_and_failure_diagnostics() -> None:
    headers = {ATTEMPT_HEADER: 4}
    assert read_attempt(headers, max_attempts=5) == 4
    result = failure_headers(
        headers,
        attempt=4,
        error=TimeoutError(),
        reason="retryable_processing_error",
        max_attempts=5,
    )
    assert "x-payment-invalid-attempt" not in result
