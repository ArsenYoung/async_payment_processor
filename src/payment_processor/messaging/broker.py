from faststream.rabbit import Channel, RabbitBroker

from payment_processor.core.config import settings


def create_broker(*, prefetch_count: int | None = None) -> RabbitBroker:
    consumer_channel = (
        Channel(prefetch_count=prefetch_count) if prefetch_count is not None else None
    )
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=consumer_channel
        or Channel(publisher_confirms=True, on_return_raises=True),
    )
