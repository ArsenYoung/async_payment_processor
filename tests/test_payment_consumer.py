import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import copy
from unittest.mock import AsyncMock, Mock, call
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from faststream.rabbit import RabbitBroker
from faststream.rabbit import TestRabbitBroker as RabbitTestBroker
from pydantic import ValidationError
from sqlalchemy import event, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.db.models import Outbox, Payment, PaymentStatus
from payment_processor.db.session import Database
from payment_processor.messaging import topology
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.rabbitmq_to_payment_worker.handlers import (
    register_payment_subscriber,
)
from payment_processor.services.payment_gateway_simulator import PaymentGatewaySimulator
from payment_processor.services.payment_service import PaymentService
from payment_processor.services.webhooks import WebhookClient


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
async def saved_event(
    outbox_database: Database, payment_payload: dict
) -> PaymentCreatedEvent:
    payment = await PaymentService(outbox_database.session_scope).create_payment(
        PaymentCreateRequest.model_validate(payment_payload), f"consumer-{uuid4()}"
    )
    async with outbox_database.session_scope() as session:
        outbox = await session.scalar(
            select(Outbox).where(Outbox.payment_id == payment.id)
        )
        assert outbox is not None
        return PaymentCreatedEvent.from_outbox(outbox)


async def _publish(broker: RabbitBroker, event: PaymentCreatedEvent | dict) -> None:
    body = (
        event.model_dump(mode="json")
        if isinstance(event, PaymentCreatedEvent)
        else event
    )
    await broker.publish(
        body,
        exchange=topology.PAYMENTS_EXCHANGE,
        routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
    )


@pytest.mark.asyncio
async def test_subscriber_processes_before_delivering_webhook(
    payment_event: PaymentCreatedEvent,
) -> None:
    service = Mock(spec=PaymentService)
    webhook = Mock(spec=WebhookClient)
    broker = create_broker()
    register_payment_subscriber(broker, service, webhook)

    async with RabbitTestBroker(broker):
        await _publish(broker, payment_event)

    assert service.mock_calls == [
        call.process_payment(payment_event.payload.payment_id),
        call.deliver_webhook(payment_event.payload.payment_id, webhook),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["processing", "delivery"])
async def test_subscriber_propagates_dependency_errors(
    payment_event: PaymentCreatedEvent, stage: str
) -> None:
    service = Mock(spec=PaymentService)
    webhook = Mock(spec=WebhookClient)
    error = RuntimeError(f"Failed {stage}")
    operation = (
        service.process_payment if stage == "processing" else service.deliver_webhook
    )
    operation.side_effect = error
    broker = create_broker()
    register_payment_subscriber(broker, service, webhook)

    async with RabbitTestBroker(broker):
        with pytest.raises(RuntimeError) as caught:
            await _publish(broker, payment_event)

    assert caught.value is error
    if stage == "processing":
        service.deliver_webhook.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["event_id", "event_type", "payment_id", "amount"])
async def test_invalid_event_is_rejected_before_processing(
    payment_event: PaymentCreatedEvent, field: str
) -> None:
    body = payment_event.model_dump(mode="json")
    if field in ("event_id", "event_type"):
        body[field] = "invalid"
    else:
        body["payload"][field] = "invalid" if field == "payment_id" else "0"
    service = Mock(spec=PaymentService)
    broker = create_broker()
    register_payment_subscriber(broker, service, Mock(spec=WebhookClient))

    async with RabbitTestBroker(broker):
        with pytest.raises(ValidationError):
            await _publish(broker, body)

    service.process_payment.assert_not_awaited()
    service.deliver_webhook.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_event_commits_result_then_delivers_saved_data(
    outbox_database: Database, saved_event: PaymentCreatedEvent, status: PaymentStatus
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    requests: list[httpx.Request] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        async with outbox_database.session_scope() as session:
            payment = await session.get(Payment, saved_event.payload.payment_id)
            assert payment is not None
            assert payment.status == status
            assert payment.processed_at is not None
            assert payment.webhook_delivered_at is None
            assert str(request.url) == payment.webhook_url
            assert json.loads(request.content) == {
                "payment_id": str(payment.id),
                "status": status.value,
                "amount": "10.50",
                "currency": "RUB",
                "description": "Test payment",
                "metadata": {"order_id": "test-order"},
                "created_at": payment.created_at.isoformat().replace("+00:00", "Z"),
                "processed_at": payment.processed_at.isoformat().replace("+00:00", "Z"),
            }
        requests.append(request)
        return httpx.Response(204)

    body = saved_event.model_dump(mode="json")
    body["payload"].update(
        amount="99.99",
        currency="USD",
        description="Stale event",
        metadata={"stale": True},
        webhook_url="https://other.example/webhook",
    )
    broker = create_broker()
    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(handle)
    ) as webhook:
        register_payment_subscriber(broker, service, webhook)
        async with RabbitTestBroker(broker):
            await _publish(broker, body)

    gateway.process.assert_awaited_once_with()
    assert len(requests) == 1
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, saved_event.payload.payment_id)
        assert payment is not None
        assert payment.status == status
        assert payment.processed_at is not None
        assert payment.webhook_delivered_at is not None
        assert payment.webhook_delivered_at >= payment.processed_at
        assert payment.webhook_delivered_at.utcoffset().total_seconds() == 0


@pytest.mark.asyncio
async def test_unknown_payment_does_not_call_gateway_or_webhook(
    outbox_database: Database, payment_event: PaymentCreatedEvent
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    webhook = Mock(spec=WebhookClient)
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    broker = create_broker()
    register_payment_subscriber(broker, service, webhook)

    async with RabbitTestBroker(broker):
        with pytest.raises(LookupError, match=str(payment_event.payload.payment_id)):
            await _publish(broker, payment_event)

    gateway.process.assert_not_awaited()
    webhook.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_gateway_error_leaves_payment_pending_without_webhook(
    outbox_database: Database, saved_event: PaymentCreatedEvent
) -> None:
    error = RuntimeError("Gateway unavailable")
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.side_effect = error
    webhook = Mock(spec=WebhookClient)
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    broker = create_broker()
    register_payment_subscriber(broker, service, webhook)

    async with RabbitTestBroker(broker):
        with pytest.raises(RuntimeError) as caught:
            await _publish(broker, saved_event)

    assert caught.value is error
    webhook.send.assert_not_awaited()
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, saved_event.payload.payment_id)
        assert payment is not None
        assert payment.status == PaymentStatus.PENDING
        assert payment.processed_at is None
        assert payment.webhook_delivered_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
@pytest.mark.parametrize("failure", ["http", "connection", "timeout"])
async def test_webhook_failure_preserves_committed_payment_result(
    outbox_database: Database,
    saved_event: PaymentCreatedEvent,
    status: PaymentStatus,
    failure: str,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    error_type = {
        "http": httpx.HTTPStatusError,
        "connection": httpx.ConnectError,
        "timeout": httpx.ReadTimeout,
    }[failure]

    async def handle(request: httpx.Request) -> httpx.Response:
        if failure == "http":
            return httpx.Response(503)
        raise error_type("Webhook unavailable", request=request)

    transport = httpx.MockTransport(AsyncMock(side_effect=handle))
    broker = create_broker()
    async with WebhookClient(timeout=1.0, transport=transport) as webhook:
        register_payment_subscriber(broker, service, webhook)
        async with RabbitTestBroker(broker):
            with pytest.raises(error_type):
                await _publish(broker, saved_event)

    gateway.process.assert_awaited_once_with()
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, saved_event.payload.payment_id)
        assert payment is not None
        assert payment.status == status
        assert payment.processed_at is not None
        assert payment.webhook_delivered_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_duplicate_event_preserves_result_and_skips_delivered_webhook(
    outbox_database: Database, saved_event: PaymentCreatedEvent, status: PaymentStatus
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    handle = AsyncMock(return_value=httpx.Response(204))
    broker = create_broker()

    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(handle)
    ) as webhook:
        register_payment_subscriber(broker, service, webhook)
        async with RabbitTestBroker(broker):
            await _publish(broker, saved_event)
            async with outbox_database.session_scope() as session:
                first = await session.get(Payment, saved_event.payload.payment_id)
                assert first is not None
                assert first.status == status
                assert first.processed_at is not None
                assert first.webhook_delivered_at is not None

            await _publish(broker, saved_event)

    gateway.process.assert_awaited_once_with()
    handle.assert_awaited_once()
    async with outbox_database.session_scope() as session:
        repeated = await session.get(Payment, saved_event.payload.payment_id)
        assert repeated is not None
        assert repeated.status == first.status
        assert repeated.processed_at == first.processed_at
        assert repeated.webhook_delivered_at == first.webhook_delivered_at
        assert repeated.created_at == first.created_at


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
@pytest.mark.parametrize("failure", ["http", "connection", "timeout"])
async def test_repeated_event_retries_failed_webhook_without_repeating_gateway(
    outbox_database: Database,
    saved_event: PaymentCreatedEvent,
    status: PaymentStatus,
    failure: str,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    requests: list[httpx.Request] = []
    error_type = {
        "http": httpx.HTTPStatusError,
        "connection": httpx.ConnectError,
        "timeout": httpx.ReadTimeout,
    }[failure]

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            if failure == "http":
                return httpx.Response(503)
            raise error_type("Webhook unavailable", request=request)
        return httpx.Response(204)

    broker = create_broker()
    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(handle)
    ) as webhook:
        register_payment_subscriber(broker, service, webhook)
        async with RabbitTestBroker(broker):
            with pytest.raises(error_type):
                await _publish(broker, saved_event)

            async with outbox_database.session_scope() as session:
                failed_delivery = await session.get(
                    Payment, saved_event.payload.payment_id
                )
                assert failed_delivery is not None
                assert failed_delivery.status == status
                assert failed_delivery.processed_at is not None
                assert failed_delivery.webhook_delivered_at is None

            await _publish(broker, saved_event)
            async with outbox_database.session_scope() as session:
                delivered = await session.get(Payment, saved_event.payload.payment_id)
                assert delivered is not None
                assert delivered.status == status
                assert delivered.processed_at == failed_delivery.processed_at
                assert delivered.created_at == failed_delivery.created_at
                assert delivered.webhook_delivered_at is not None

            await _publish(broker, saved_event)

    gateway.process.assert_awaited_once_with()
    assert len(requests) == 2
    assert requests[0].url == requests[1].url
    assert requests[0].content == requests[1].content
    async with outbox_database.session_scope() as session:
        repeated = await session.get(Payment, saved_event.payload.payment_id)
        assert repeated is not None
        assert repeated.status == status
        assert repeated.processed_at == failed_delivery.processed_at
        assert repeated.webhook_delivered_at == delivered.webhook_delivered_at


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_repeated_event_resends_webhook_after_delivery_commit_failure(
    outbox_database: Database, saved_event: PaymentCreatedEvent, status: PaymentStatus
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    error = OperationalError("COMMIT", {}, RuntimeError("Connection lost"))
    fail_delivery_commit = True

    def fail_commit(session: object) -> None:
        raise error

    @asynccontextmanager
    async def session_scope() -> AsyncIterator[AsyncSession]:
        nonlocal fail_delivery_commit
        async with outbox_database.session_scope() as session:
            yield session
            payment = await session.get(Payment, saved_event.payload.payment_id)
            if (
                fail_delivery_commit
                and payment is not None
                and payment.webhook_delivered_at is not None
            ):
                fail_delivery_commit = False
                event.listen(session.sync_session, "before_commit", fail_commit)

    service = PaymentService(session_scope, gateway=gateway)
    handle = AsyncMock(return_value=httpx.Response(204))
    broker = create_broker()

    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(handle)
    ) as webhook:
        register_payment_subscriber(broker, service, webhook)
        async with RabbitTestBroker(broker):
            with pytest.raises(OperationalError) as caught:
                await _publish(broker, saved_event)
            assert caught.value is error
            handle.assert_awaited_once()

            async with outbox_database.session_scope() as session:
                failed_delivery = await session.get(
                    Payment, saved_event.payload.payment_id
                )
                assert failed_delivery is not None
                assert failed_delivery.status == status
                assert failed_delivery.processed_at is not None
                assert failed_delivery.webhook_delivered_at is None

            await _publish(broker, saved_event)
            async with outbox_database.session_scope() as session:
                delivered = await session.get(Payment, saved_event.payload.payment_id)
                assert delivered is not None
                assert delivered.status == status
                assert delivered.processed_at == failed_delivery.processed_at
                assert delivered.created_at == failed_delivery.created_at
                assert delivered.webhook_delivered_at is not None

            await _publish(broker, saved_event)

    gateway.process.assert_awaited_once_with()
    assert handle.await_count == 2
    first_request = handle.await_args_list[0].args[0]
    repeated_request = handle.await_args_list[1].args[0]
    assert repeated_request.url == first_request.url
    assert repeated_request.content == first_request.content
    async with outbox_database.session_scope() as session:
        repeated = await session.get(Payment, saved_event.payload.payment_id)
        assert repeated is not None
        assert repeated.status == status
        assert repeated.processed_at == failed_delivery.processed_at
        assert repeated.webhook_delivered_at == delivered.webhook_delivered_at


@pytest.mark.asyncio
async def test_delivery_of_unknown_payment_raises_before_sending(
    outbox_database: Database,
) -> None:
    payment_id = uuid4()
    webhook = Mock(spec=WebhookClient)

    with pytest.raises(LookupError, match=str(payment_id)):
        await PaymentService(outbox_database.session_scope).deliver_webhook(
            payment_id, webhook
        )

    webhook.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_payment_cannot_be_delivered(
    outbox_database: Database, saved_event: PaymentCreatedEvent
) -> None:
    handle = AsyncMock(return_value=httpx.Response(200))
    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(handle)
    ) as webhook:
        with pytest.raises(ValidationError):
            await PaymentService(outbox_database.session_scope).deliver_webhook(
                saved_event.payload.payment_id, webhook
            )

    handle.assert_not_awaited()
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, saved_event.payload.payment_id)
        assert payment is not None
        assert payment.webhook_delivered_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_real_rabbitmq_message_completes_payment_and_delivery(
    outbox_database: Database,
    saved_event: PaymentCreatedEvent,
    monkeypatch: pytest.MonkeyPatch,
    status: PaymentStatus,
) -> None:
    prefix = f"test-consumer-{uuid4()}-"
    exchange = copy(topology.PAYMENTS_EXCHANGE)
    queue = copy(topology.PAYMENTS_NEW_QUEUE)
    exchange.name = prefix + exchange.name
    queue.name = prefix + queue.name
    monkeypatch.setattr(topology, "PAYMENTS_EXCHANGE", exchange)
    monkeypatch.setattr(topology, "PAYMENTS_NEW_QUEUE", queue)
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    handle = AsyncMock(return_value=httpx.Response(200))
    broker = create_broker()

    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(handle)
    ) as webhook:
        register_payment_subscriber(broker, service, webhook)
        try:
            await broker.connect()
            declared_exchange = await broker.declare_exchange(exchange)
            try:
                declared_queue = await broker.declare_queue(queue)
                try:
                    await topology.declare_payment_topology(broker)
                    await broker.start()
                    await topology.publish_payment_event(broker, saved_event)
                    async with asyncio.timeout(5):
                        while True:
                            async with outbox_database.session_scope() as session:
                                payment = await session.get(
                                    Payment, saved_event.payload.payment_id
                                )
                                assert payment is not None
                                if payment.webhook_delivered_at is not None:
                                    assert payment.status == status
                                    assert payment.processed_at is not None
                                    break
                            await asyncio.sleep(0.01)
                finally:
                    await declared_queue.delete(if_unused=False, if_empty=False)
            finally:
                await declared_exchange.delete(if_unused=False)
        finally:
            await broker.stop()

    gateway.process.assert_awaited_once_with()
    handle.assert_awaited_once()
