from faststream.rabbit import Channel, RabbitBroker

from payment_processor.core.config import settings


def create_broker() -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
