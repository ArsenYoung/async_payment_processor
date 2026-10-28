import asyncio

from faststream.rabbit import (
    ExchangeType,
    QueueType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)
from faststream.rabbit.message import RabbitMessage
from pamqp.commands import Basic

from payment_processor.db.models import OutboxEventType
from payment_processor.messaging.retry import (
    ATTEMPT_HEADER,
    RetryAttempt,
    failure_headers,
)
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


def retry_exchange() -> RabbitExchange:
    return RabbitExchange(f"{PAYMENTS_EXCHANGE.name}.retry", durable=True)


def dead_letter_exchange() -> RabbitExchange:
    return RabbitExchange(f"{PAYMENTS_EXCHANGE.name}.dead", durable=True)


def retry_queue(attempt: int) -> RabbitQueue:
    if type(attempt) is not int or attempt not in (2, 3):
        raise ValueError("Retry queue requires attempt 2 or 3")
    return RabbitQueue(
        f"{PAYMENTS_EXCHANGE.name}.retry.{attempt}",
        routing_key=f"payment.retry.{attempt}",
        queue_type=QueueType.QUORUM,
        durable=True,
        arguments={
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW_QUEUE.routing(),
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )


def dead_letter_queue() -> RabbitQueue:
    return RabbitQueue(
        f"{PAYMENTS_EXCHANGE.name}.dlq", routing_key="payment.dead", durable=True
    )


async def declare_retry_topology(broker: RabbitBroker) -> None:
    exchange = await broker.declare_exchange(retry_exchange())
    for attempt in (2, 3):
        definition = retry_queue(attempt)
        queue = await broker.declare_queue(definition)
        await queue.bind(exchange, routing_key=definition.routing())
    exchange = await broker.declare_exchange(dead_letter_exchange())
    definition = dead_letter_queue()
    queue = await broker.declare_queue(definition)
    await queue.bind(exchange, routing_key=definition.routing())


async def publish_failed_payment(
    broker: RabbitBroker,
    message: RabbitMessage,
    *,
    attempt: int,
    error: Exception,
    reason: str,
    retry: RetryAttempt | None,
    timeout: float = 5.0,
) -> None:
    exchange = retry_exchange() if retry is not None else dead_letter_exchange()
    queue = retry_queue(retry.number) if retry is not None else dead_letter_queue()
    headers = failure_headers(
        message.headers,
        attempt=attempt,
        error=error,
        reason=reason,
    )
    if retry is not None:
        headers[ATTEMPT_HEADER] = retry.number
    async with asyncio.timeout(timeout):
        confirmation = await broker.publish(
            message.body,
            exchange=exchange,
            routing_key=queue.routing(),
            headers=headers,
            mandatory=True,
            persist=True,
            timeout=timeout,
            expiration=max(0.001, retry.delay) if retry is not None else None,
            message_id=message.message_id,
            message_type=message.raw_message.type,
            correlation_id=message.correlation_id,
            content_type=message.content_type,
        )
    if not isinstance(confirmation, Basic.Ack):
        raise RuntimeError("RabbitMQ did not acknowledge failed payment publication")


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
