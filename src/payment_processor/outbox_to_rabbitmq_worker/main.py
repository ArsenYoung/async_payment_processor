import asyncio
import logging

from payment_processor.core.config import settings
from payment_processor.core.logging import configure_library_logging, configure_logging
from payment_processor.core.worker import run_worker
from payment_processor.db.session import Database
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.topology import declare_payment_topology
from payment_processor.services.outbox_service import OutboxService


async def run_publisher() -> None:
    await run_worker(_publish, name="outbox-to-rabbitmq-worker")


async def _publish() -> None:
    database = Database(settings.database_url)
    try:
        await database.check_connection()
        broker = create_broker()
        try:
            await broker.connect()
            configure_library_logging()
            await declare_payment_topology(broker)
            service = OutboxService(database.session_scope, broker)
            logging.getLogger(__name__).info(
                "Outbox publisher ready", extra={"outcome": "ready"}
            )
            await service.run(
                batch_size=settings.outbox_batch_size,
                poll_interval=settings.outbox_poll_interval,
                publish_timeout=settings.outbox_publish_timeout,
            )
        finally:
            await broker.stop()
    finally:
        await database.dispose()


def main() -> None:
    configure_logging("outbox-to-rabbitmq-worker", settings)
    try:
        asyncio.run(run_publisher())
    except Exception:
        logging.getLogger(__name__).exception("Outbox publisher failed")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
