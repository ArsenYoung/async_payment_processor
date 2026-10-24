from faststream.rabbit import RabbitBroker

from payment_processor.core.config import settings


def create_broker() -> RabbitBroker:
    return RabbitBroker(settings.rabbitmq_url)
