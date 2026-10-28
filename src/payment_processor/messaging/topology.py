import asyncio

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
from pamqp.commands import Basic

from payment_processor.db.models import OutboxEventType
from payment_processor.messaging.schemas import PaymentCreatedEvent

PAYMENTS_EXCHANGE = RabbitExchange(
    "payments",
    type=ExchangeType.DIRECT,
    durable=True,
    auto_delete=False,
)
PAYMENTS_NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    auto_delete=False,
    routing_key=OutboxEventType.PAYMENT_CREATED.value,
)


async def declare_payment_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    await queue.bind(exchange, routing_key=PAYMENTS_NEW_QUEUE.routing())


async def publish_payment_event(
    broker: RabbitBroker,
    event: PaymentCreatedEvent,
    *,
    timeout: float = 5.0,
) -> None:
    async with asyncio.timeout(timeout):
        confirmation = await broker.publish(
            event.model_dump(mode="json"),
            exchange=PAYMENTS_EXCHANGE,
            routing_key=PAYMENTS_NEW_QUEUE.routing(),
            mandatory=True,
            persist=True,
            timeout=timeout,
            message_id=str(event.event_id),
            message_type=event.event_type.value,
        )
    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("RabbitMQ did not acknowledge payment event publication")
