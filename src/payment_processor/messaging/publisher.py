import asyncio

from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic

from payment_processor.messaging import topology
from payment_processor.messaging.schemas import PaymentCreatedEvent


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish_payment_created(
        self, event: PaymentCreatedEvent, *, timeout: float
    ) -> None:
        async with asyncio.timeout(timeout):
            confirmation = await self._broker.publish(
                event.model_dump(mode="json"),
                exchange=topology.PAYMENTS_EXCHANGE,
                routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
                mandatory=True,
                persist=True,
                timeout=timeout,
                message_id=str(event.event_id),
                message_type=event.event_type.value,
            )
        if not isinstance(confirmation, Basic.Ack):
            raise RuntimeError("RabbitMQ did not acknowledge payment event publication")
