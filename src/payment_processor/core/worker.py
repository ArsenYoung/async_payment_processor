import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


async def run_worker(worker: Callable[[], Awaitable[None]], *, name: str) -> None:
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_handlers = {}
    outcome = "stopped"
    logger.info("Worker starting", extra={"worker": name, "outcome": "starting"})
    try:
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            previous_handler = signal.getsignal(shutdown_signal)
            loop.add_signal_handler(shutdown_signal, shutdown.set)
            previous_handlers[shutdown_signal] = previous_handler
        async with asyncio.TaskGroup() as tasks:
            task = tasks.create_task(_supervise(worker, name), name=name)
            await shutdown.wait()
            logger.info(
                "Worker shutdown requested",
                extra={"worker": name, "outcome": "stopping"},
            )
            task.cancel()
    except asyncio.CancelledError:
        outcome = "cancelled"
        raise
    except Exception:
        outcome = "failed"
        logger.exception("Worker failed", extra={"worker": name, "outcome": outcome})
        raise
    finally:
        for shutdown_signal, previous_handler in previous_handlers.items():
            loop.remove_signal_handler(shutdown_signal)
            signal.signal(shutdown_signal, previous_handler)
        logger.info("Worker stopped", extra={"worker": name, "outcome": outcome})


async def _supervise(worker: Callable[[], Awaitable[None]], name: str) -> None:
    await worker()
    raise RuntimeError(f"Worker {name} stopped unexpectedly")
