from faststream.middlewares import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitMessage
from faststream.rabbit.message import RabbitMessage as IncomingPaymentMessage

from payment_processor.messaging import topology
from payment_processor.messaging.retry import RetryPolicy, read_attempt
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.services.errors import is_retryable_processing_error
from payment_processor.services.payment_service import PaymentService
from payment_processor.services.webhooks import WebhookClient


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
        try:
            attempt = read_attempt(message.headers)
            event = PaymentCreatedEvent.model_validate_json(body)
            reason = "non_retryable_processing_error"
            await payment_service.process_payment(event.payload.payment_id)
            await payment_service.deliver_webhook(
                event.payload.payment_id, webhook_client
            )
        except Exception as error:
            retry = None
            if reason != "invalid_message" and is_retryable_processing_error(error):
                retry = retry_policy.next_retry(attempt)
                reason = "retryable_processing_error" if retry else "attempts_exhausted"
            await topology.publish_failed_payment(
                broker,
                message,
                attempt=attempt,
                error=error,
                reason=reason,
                retry=retry,
            )
