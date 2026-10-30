import asyncio
import logging

from payment_processor.core.config import settings
from payment_processor.core.logging import configure_library_logging, configure_logging
from payment_processor.core.worker import run_worker
from payment_processor.db.session import Database
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.retry import RetryPolicy
from payment_processor.messaging.topology import (
    declare_payment_topology,
    declare_retry_topology,
)
from payment_processor.rabbitmq_to_payment_worker.handlers import (
    register_payment_subscriber,
)
from payment_processor.services.payment_service import PaymentService
from payment_processor.services.webhooks import WebhookClient


async def run_consumer() -> None:
    await run_worker(_consume, name="rabbitmq-to-payment-worker")


async def _consume() -> None:
    database = Database(settings.database_url)
    try:
        await database.check_connection()
        async with WebhookClient(timeout=settings.webhook_timeout) as webhook_client:
            payment_service = PaymentService(database.session_scope)
            broker = create_broker(prefetch_count=settings.consumer_prefetch_count)
            try:
                register_payment_subscriber(
                    broker,
                    payment_service,
                    webhook_client,
                    retry_policy=RetryPolicy(base_delay=settings.retry_base_delay),
                )
                await broker.connect()
                configure_library_logging()
                await declare_payment_topology(broker)
                await declare_retry_topology(broker)
                await broker.start()
                logging.getLogger(__name__).info(
                    "Payment consumer ready", extra={"outcome": "ready"}
                )
                await asyncio.Event().wait()
            finally:
                await broker.stop()
    finally:
        await database.dispose()


def main() -> None:
    configure_logging("rabbitmq-to-payment-worker", settings)
    try:
        asyncio.run(run_consumer())
    except Exception:
        logging.getLogger(__name__).exception("Payment consumer failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
