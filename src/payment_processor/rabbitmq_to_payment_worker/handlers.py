from faststream.middlewares import AckPolicy
from faststream.rabbit import RabbitBroker

from payment_processor.messaging import topology
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.services.payment_service import PaymentService
from payment_processor.services.webhooks import WebhookClient


def register_payment_subscriber(
    broker: RabbitBroker,
    payment_service: PaymentService,
    webhook_client: WebhookClient,
) -> None:
    @broker.subscriber(
        topology.PAYMENTS_NEW_QUEUE,
        exchange=topology.PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )
    async def handle_payment(event: PaymentCreatedEvent) -> None:
        await payment_service.process_payment(event.payload.payment_id)
        await payment_service.deliver_webhook(event.payload.payment_id, webhook_client)
