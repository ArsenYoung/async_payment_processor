import asyncio
import signal
from collections.abc import Awaitable, Callable


async def run_worker(worker: Callable[[], Awaitable[None]], *, name: str) -> None:
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous_handlers = {}
    try:
        for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
            previous_handler = signal.getsignal(shutdown_signal)
            loop.add_signal_handler(shutdown_signal, shutdown.set)
            previous_handlers[shutdown_signal] = previous_handler
        async with asyncio.TaskGroup() as tasks:
            task = tasks.create_task(_supervise(worker, name), name=name)
            await shutdown.wait()
            task.cancel()
    finally:
        for shutdown_signal, previous_handler in previous_handlers.items():
            loop.remove_signal_handler(shutdown_signal)
            signal.signal(shutdown_signal, previous_handler)


async def _supervise(worker: Callable[[], Awaitable[None]], name: str) -> None:
    await worker()
    raise RuntimeError(f"Worker {name} stopped unexpectedly")
