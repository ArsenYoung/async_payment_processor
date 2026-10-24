import asyncio

from payment_processor.messaging.broker import create_broker


async def run_consumer() -> None:
    broker = create_broker()
    await broker.connect()

    try:
        await asyncio.Event().wait()
    finally:
        await broker.stop()


def main() -> None:
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
