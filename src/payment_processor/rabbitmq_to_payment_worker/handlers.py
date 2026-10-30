import logging
from contextlib import ExitStack
from time import perf_counter
from uuid import UUID

from faststream.middlewares import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from faststream.rabbit.message import RabbitMessage as IncomingPaymentMessage

from payment_processor.application.commands import (
    DeliverWebhookCommand,
    ProcessPaymentCommand,
)
from payment_processor.core.logging import processing_context, processing_fields
from payment_processor.messaging import topology
from payment_processor.messaging.retry import RetryPolicy, read_attempt
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.services.payment_event_failures import (
    classify_payment_event_failure,
)
from payment_processor.services.payment_service import PaymentService
from payment_processor.services.webhooks import WebhookClient

logger = logging.getLogger(__name__)


def register_payment_subscriber(
    broker: RabbitBroker,
    payment_service: PaymentService,
    webhook_client: WebhookClient,
    *,
    retry_policy: RetryPolicy,
) -> None:
    async def decode_body(message: IncomingPaymentMessage) -> bytes:
        return message.body

    @broker.subscriber(
        topology.PAYMENTS_NEW_QUEUE,
        exchange=topology.PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.NACK_ON_ERROR,
        decoder=decode_body,
    )
    async def handle_payment(body: bytes, message: RabbitMessage) -> None:
        attempt = 1
        reason = "invalid_message"
        started = perf_counter()
        with ExitStack() as context:
            context.enter_context(
                processing_context(queue=topology.PAYMENTS_NEW_QUEUE.name)
            )
            try:
                message_event_id = str(UUID(message.message_id))
            except (ValueError, TypeError, AttributeError):
                pass
            else:
                context.enter_context(processing_context(event_id=message_event_id))
            try:
                attempt = read_attempt(message.headers)
                context.enter_context(processing_context(attempt=attempt))
                event = PaymentCreatedEvent.model_validate_json(body)
                context.enter_context(
                    processing_context(
                        payment_id=str(event.payload.payment_id),
                        event_id=str(event.event_id),
                    )
                )
                logger.info(
                    "Payment event received",
                    extra=processing_fields(operation="consume", outcome="received"),
                )
                reason = "non_retryable_processing_error"
                await payment_service.process_payment(
                    ProcessPaymentCommand(event.payload.payment_id)
                )
                await payment_service.deliver_webhook(
                    DeliverWebhookCommand(event.payload.payment_id), webhook_client
                )
            except Exception as error:
                decision = classify_payment_event_failure(
                    error,
                    message_valid=reason != "invalid_message",
                    attempt=attempt,
                    retry_policy=retry_policy,
                )
                retry = decision.retry
                reason = decision.reason
                logger.log(
                    logging.WARNING if retry else logging.ERROR,
                    "Payment event handling failed",
                    extra=processing_fields(
                        operation="consume",
                        outcome="failed",
                        reason=reason,
                        error_type=type(error).__name__,
                        duration_ms=(perf_counter() - started) * 1000,
                    ),
                    exc_info=True,
                )
                await topology.publish_failed_payment(
                    broker,
                    message,
                    attempt=attempt,
                    error=error,
                    reason=reason,
                    retry=retry,
                )
            else:
                logger.info(
                    "Payment event handling completed",
                    extra=processing_fields(
                        operation="consume",
                        outcome="completed",
                        duration_ms=(perf_counter() - started) * 1000,
                    ),
                )
