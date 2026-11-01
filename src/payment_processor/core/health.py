import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from time import monotonic

HEALTH_PATH = Path("/tmp/payment-worker-health.json")
HEALTH_TIMEOUT = 2.0


class WorkerHealth:
    def __init__(
        self,
        check: Callable[[], Awaitable[bool]],
        *,
        progress: Callable[[], float] | None = None,
        progress_timeout: float = 30,
        path: Path = HEALTH_PATH,
        interval: float = 2,
        timeout: float = HEALTH_TIMEOUT,
    ) -> None:
        self.check = check
        self.progress = progress
        self.progress_timeout = progress_timeout
        self.path = path
        self.interval = interval
        self.timeout = timeout

    async def sample(self) -> bool:
        try:
            async with asyncio.timeout(self.timeout):
                ready = bool(await self.check())
                if self.progress is not None:
                    ready = (
                        ready and monotonic() - self.progress() < self.progress_timeout
                    )
        except Exception:
            ready = False
        temporary = self.path.with_suffix(f".{os.getpid()}.tmp")
        temporary.write_text(json.dumps({"ready": ready, "checked_at": monotonic()}))
        temporary.replace(self.path)
        return ready

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            await self.sample()

    @asynccontextmanager
    async def monitor(self) -> AsyncIterator[None]:
        self.path.unlink(missing_ok=True)
        task = asyncio.create_task(self.run(), name="worker-readiness")
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            self.path.unlink(missing_ok=True)


def healthy(path: Path = HEALTH_PATH, *, max_age: float = 10) -> bool:
    try:
        value = json.loads(path.read_text())
        age = monotonic() - value["checked_at"]
        return value["ready"] is True and 0 <= age < max_age
    except (OSError, ValueError, KeyError, TypeError):
        return False


if __name__ == "__main__":
    raise SystemExit(0 if healthy() else 1)
