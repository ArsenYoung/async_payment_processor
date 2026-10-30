import asyncio
from collections.abc import AsyncIterator
from copy import copy
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
import pytest_asyncio
from aio_pika import DeliveryMode
from aio_pika.exceptions import DeliveryError, PublishError
from faststream.rabbit import ExchangeType, RabbitBroker
from pamqp.commands import Basic

from payment_processor.messaging import topology
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.messaging.topology import (
    declare_payment_topology,
    publish_payment_event,
)


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
async def isolated_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[RabbitBroker]:
    exchange = copy(topology.PAYMENTS_EXCHANGE)
    queue = copy(topology.PAYMENTS_NEW_QUEUE)
    prefix = f"test-{uuid4()}-"
    exchange.name = prefix + exchange.name
    queue.name = prefix + queue.name
    monkeypatch.setattr(topology, "PAYMENTS_EXCHANGE", exchange)
    monkeypatch.setattr(topology, "PAYMENTS_NEW_QUEUE", queue)
    broker = create_broker()

    try:
        await broker.connect()
        declared_exchange = await broker.declare_exchange(exchange)
        try:
            declared_queue = await broker.declare_queue(queue)
            try:
                await declare_payment_topology(broker)
                yield broker
            finally:
                await declared_queue.delete(if_unused=False, if_empty=False)
        finally:
            await declared_exchange.delete(if_unused=False)
    finally:
        await broker.stop()


def test_payment_topology_defines_durable_direct_route() -> None:
    assert topology.PAYMENTS_EXCHANGE.name == "payments"
    assert topology.PAYMENTS_EXCHANGE.type == ExchangeType.DIRECT
    assert topology.PAYMENTS_EXCHANGE.durable is True
    assert topology.PAYMENTS_EXCHANGE.auto_delete is False
    assert topology.PAYMENTS_NEW_QUEUE.name == "payments.new"
    assert topology.PAYMENTS_NEW_QUEUE.routing() == "payment.created"
    assert topology.PAYMENTS_NEW_QUEUE.durable is True
    assert topology.PAYMENTS_NEW_QUEUE.auto_delete is False
    assert topology.PAYMENTS_NEW_QUEUE.exclusive is False


def test_consumer_broker_uses_prefetch_without_disabling_publisher_confirms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = Mock()
    constructor = Mock(return_value=broker)
    monkeypatch.setattr("payment_processor.messaging.broker.RabbitBroker", constructor)

    create_broker(prefetch_count=5)

    channel = constructor.call_args.kwargs["default_channel"]
    assert channel.prefetch_count == 5
    assert channel.publisher_confirms is True


@pytest.mark.asyncio
async def test_payment_event_reaches_queue_with_persistent_delivery_and_stable_id(
    isolated_broker: RabbitBroker, payment_event: PaymentCreatedEvent
) -> None:
    await publish_payment_event(isolated_broker, payment_event)
    await publish_payment_event(isolated_broker, payment_event)

    queue = await isolated_broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    for _ in range(2):
        message = await queue.get(timeout=5)
        async with message.process():
            assert (
                PaymentCreatedEvent.model_validate_json(message.body) == payment_event
            )
            assert message.message_id == str(payment_event.event_id)
            assert message.type == "payment.created"
            assert message.content_type == "application/json"
            assert message.delivery_mode == DeliveryMode.PERSISTENT
            assert message.exchange == topology.PAYMENTS_EXCHANGE.name
            assert message.routing_key == "payment.created"
    assert await queue.get(fail=False, timeout=5) is None


@pytest.mark.asyncio
async def test_topology_can_be_declared_again_from_another_connection(
    isolated_broker: RabbitBroker, payment_event: PaymentCreatedEvent
) -> None:
    second_broker = create_broker()
    try:
        await second_broker.connect()
        await declare_payment_topology(second_broker)
        await declare_payment_topology(second_broker)
        await publish_payment_event(second_broker, payment_event)
    finally:
        await second_broker.stop()

    queue = await isolated_broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    message = await queue.get(timeout=5)
    async with message.process():
        assert PaymentCreatedEvent.model_validate_json(message.body) == payment_event
    assert await queue.get(fail=False, timeout=5) is None


@pytest.mark.asyncio
async def test_unroutable_payment_event_raises_publish_error(
    isolated_broker: RabbitBroker, payment_event: PaymentCreatedEvent
) -> None:
    queue = await isolated_broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    exchange = await isolated_broker.declare_exchange(topology.PAYMENTS_EXCHANGE)
    await queue.unbind(exchange, routing_key="payment.created", timeout=5)

    with pytest.raises(PublishError):
        await publish_payment_event(isolated_broker, payment_event)

    assert await queue.get(fail=False, timeout=5) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmation", [None, Basic.Nack(), Basic.Reject()])
async def test_missing_or_negative_confirmation_is_rejected(
    payment_event: PaymentCreatedEvent, confirmation: object
) -> None:
    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(return_value=confirmation)

    with pytest.raises(RuntimeError, match="did not acknowledge"):
        await publish_payment_event(broker, payment_event)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("Confirmation timed out"),
        ConnectionError("Broker disconnected"),
        DeliveryError(None, Basic.Nack()),
    ],
)
async def test_publication_errors_propagate(
    payment_event: PaymentCreatedEvent, error: Exception
) -> None:
    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(side_effect=error)

    with pytest.raises(type(error)) as caught:
        await publish_payment_event(broker, payment_event)

    assert caught.value is error


@pytest.mark.asyncio
async def test_publication_wait_is_bounded(payment_event: PaymentCreatedEvent) -> None:
    waiting = asyncio.Event()
    cancelled = asyncio.Event()

    async def stalled_publication(*args: object, **kwargs: object) -> None:
        try:
            await waiting.wait()
        finally:
            cancelled.set()

    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(side_effect=stalled_publication)

    async with asyncio.timeout(1):
        with pytest.raises(TimeoutError):
            await publish_payment_event(broker, payment_event, timeout=0.01)

    assert cancelled.is_set()
