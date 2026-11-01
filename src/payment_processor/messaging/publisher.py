from faststream.rabbit import RabbitBroker

from payment_processor.application.events import PaymentCreatedEvent
from payment_processor.application.ports import EventPublisher
from payment_processor.messaging import topology


class RabbitEventPublisher(EventPublisher):
    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish_payment_created(
        self, event: PaymentCreatedEvent, *, timeout: float
    ) -> None:
        await topology.publish_payment_event(self._broker, event, timeout=timeout)
