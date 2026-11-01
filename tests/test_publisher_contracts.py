import asyncio
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from aio_pika import DeliveryMode
from aio_pika.exceptions import PublishError
from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic

from payment_processor.application.ports import EventPublisher
from payment_processor.messaging import topology
from payment_processor.messaging.publisher import RabbitEventPublisher
from payment_processor.messaging.schemas import PaymentCreatedEvent


@pytest.fixture
def publisher_event(payment_payload: dict) -> PaymentCreatedEvent:
    return PaymentCreatedEvent.model_validate(
        {
            "event_id": str(uuid4()),
            "event_type": "payment.created",
            "payload": {"payment_id": str(uuid4()), **payment_payload},
        }
    )


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "confirmation", [Basic.Ack(), None, Basic.Nack(), Basic.Reject()]
)
async def test_publisher_port_requires_ack_and_preserves_envelope(
    publisher_event: PaymentCreatedEvent, confirmation: object
) -> None:
    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(return_value=confirmation)
    publisher: EventPublisher = RabbitEventPublisher(broker)
    if isinstance(confirmation, Basic.Ack):
        await publisher.publish_payment_created(publisher_event, timeout=0.5)
    else:
        with pytest.raises(RuntimeError, match="did not acknowledge"):
            await publisher.publish_payment_created(publisher_event, timeout=0.5)
    broker.publish.assert_awaited_once_with(
        publisher_event.model_dump(mode="json"),
        exchange=topology.PAYMENTS_EXCHANGE,
        routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
        mandatory=True,
        persist=True,
        timeout=0.5,
        message_id=str(publisher_event.event_id),
        message_type="payment.created",
    )


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [ConnectionError("broker unavailable"), asyncio.CancelledError()]
)
async def test_publisher_port_propagates_errors_without_retry(
    publisher_event: PaymentCreatedEvent, error: BaseException
) -> None:
    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(side_effect=error)
    publisher: EventPublisher = RabbitEventPublisher(broker)
    with pytest.raises(type(error)) as caught:
        await publisher.publish_payment_created(publisher_event, timeout=0.5)
    assert caught.value is error
    broker.publish.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publisher_port_bounds_wait_even_when_broker_ignores_timeout(
    publisher_event: PaymentCreatedEvent,
) -> None:
    cancelled = asyncio.Event()

    async def stalled(*args: object, **kwargs: object) -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    broker = Mock(spec=RabbitBroker)
    broker.publish = AsyncMock(side_effect=stalled)
    publisher: EventPublisher = RabbitEventPublisher(broker)
    async with asyncio.timeout(1):
        with pytest.raises(TimeoutError):
            await publisher.publish_payment_created(publisher_event, timeout=0.01)
    assert cancelled.is_set()
    broker.publish.assert_awaited_once()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publisher_port_delivers_persistent_event_with_stable_identity(
    routing_broker: RabbitBroker, publisher_event: PaymentCreatedEvent
) -> None:
    publisher: EventPublisher = RabbitEventPublisher(routing_broker)
    queue = await routing_broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    for _ in range(2):
        await publisher.publish_payment_created(publisher_event, timeout=2)
        message = await queue.get(timeout=2)
        async with message.process():
            assert (
                PaymentCreatedEvent.model_validate_json(message.body) == publisher_event
            )
            assert message.message_id == str(publisher_event.event_id)
            assert message.type == "payment.created"
            assert message.delivery_mode == DeliveryMode.PERSISTENT
            assert message.content_type == "application/json"
            assert message.exchange == topology.PAYMENTS_EXCHANGE.name
            assert message.routing_key == topology.PAYMENTS_NEW_QUEUE.routing()
    assert await queue.get(fail=False, timeout=2) is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publisher_port_rejects_unroutable_event(
    routing_broker: RabbitBroker, publisher_event: PaymentCreatedEvent
) -> None:
    queue = await routing_broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    exchange = await routing_broker.declare_exchange(topology.PAYMENTS_EXCHANGE)
    await queue.unbind(exchange, routing_key=topology.PAYMENTS_NEW_QUEUE.routing())
    publisher: EventPublisher = RabbitEventPublisher(routing_broker)
    with pytest.raises(PublishError):
        await publisher.publish_payment_created(publisher_event, timeout=2)
    assert await queue.get(fail=False, timeout=2) is None
