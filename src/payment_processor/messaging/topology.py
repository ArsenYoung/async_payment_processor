import asyncio
import logging
from time import perf_counter

from faststream.rabbit import (
    ExchangeType,
    QueueType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)
from faststream.rabbit.message import RabbitMessage
from pamqp.commands import Basic

from payment_processor.application.retry import MAX_ATTEMPTS, RetryPolicy
from payment_processor.core.logging import processing_fields
from payment_processor.domain.events import OutboxEventType
from payment_processor.messaging.retry import (
    ATTEMPT_HEADER,
    RetryAttempt,
    failure_headers,
)
from payment_processor.messaging.schemas import PaymentCreatedEvent

logger = logging.getLogger(__name__)
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
    if type(attempt) is not int or attempt < 2:
        raise ValueError("Retry queue requires an integer attempt of at least 2")
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


async def declare_retry_topology(
    broker: RabbitBroker, policy: RetryPolicy | None = None
) -> None:
    exchange = await broker.declare_exchange(retry_exchange())
    for attempt in (policy if policy is not None else RetryPolicy()).retry_attempts:
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
    max_attempts: int = MAX_ATTEMPTS,
    timeout: float = 5.0,
) -> None:
    started = perf_counter()
    exchange = retry_exchange() if retry is not None else dead_letter_exchange()
    queue = retry_queue(retry.number) if retry is not None else dead_letter_queue()
    headers = failure_headers(
        message.headers,
        attempt=attempt,
        error=error,
        reason=reason,
        max_attempts=max_attempts,
    )
    if retry is not None:
        headers[ATTEMPT_HEADER] = retry.number
    fields = processing_fields(
        operation="retry_publish" if retry else "dlq_publish",
        queue=queue.name,
        attempt=attempt,
        reason=reason,
        error_type=type(error).__name__,
    )
    if retry is not None:
        fields.update(next_attempt=retry.number, retry_delay_ms=retry.delay * 1000)
    try:
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
            raise RuntimeError(
                "RabbitMQ did not acknowledge failed payment publication"
            )
    except Exception as publication_error:
        logger.exception(
            "Failed payment transfer failed",
            extra={
                **fields,
                "outcome": "failed",
                "error_type": type(publication_error).__name__,
                "duration_ms": (perf_counter() - started) * 1000,
            },
        )
        raise
    logger.log(
        logging.WARNING if retry else logging.ERROR,
        "Payment retry scheduled" if retry else "Payment event sent to DLQ",
        extra={
            **fields,
            "outcome": "published",
            "duration_ms": (perf_counter() - started) * 1000,
        },
    )


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
