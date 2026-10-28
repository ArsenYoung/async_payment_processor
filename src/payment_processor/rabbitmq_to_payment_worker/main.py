import asyncio
import logging

from payment_processor.core.config import settings
from payment_processor.core.worker import run_worker
from payment_processor.db.session import Database
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.topology import declare_payment_topology


async def run_consumer() -> None:
    await run_worker(_consume, name="rabbitmq-to-payment-worker")


async def _consume() -> None:
    database = Database(settings.database_url)
    try:
        await database.check_connection()
        broker = create_broker()
        try:
            await broker.connect()
            await declare_payment_topology(broker)
            await broker.start()
            await asyncio.Event().wait()
        finally:
            await broker.stop()
    finally:
        await database.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
