import asyncio
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from urllib.parse import quote, unquote, urlsplit

import httpx
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import func, or_, select

from payment_processor.core.config import settings
from payment_processor.core.metrics import (
    COLLECTOR_UP,
    DLQ_SIZE,
    OUTBOX_AVAILABLE,
    OUTBOX_OLDEST,
)
from payment_processor.db.models import Outbox
from payment_processor.db.session import Database


async def collect_outbox(database: Database) -> None:
    try:
        async with asyncio.timeout(2):
            async with database.session_scope() as session:
                count, oldest = (
                    await session.execute(
                        select(
                            func.count(),
                            func.min(Outbox.created_at),
                        ).where(
                            Outbox.published_at.is_(None),
                            Outbox.quarantined_at.is_(None),
                            or_(
                                Outbox.publication_until.is_(None),
                                Outbox.publication_until <= func.clock_timestamp(),
                            ),
                            or_(
                                Outbox.next_attempt_at.is_(None),
                                Outbox.next_attempt_at <= func.clock_timestamp(),
                            ),
                        )
                    )
                ).one()
                now = await session.scalar(select(func.clock_timestamp()))
            assert now is not None
            OUTBOX_AVAILABLE.set(count)
            OUTBOX_OLDEST.set(
                max(0, (now - oldest).total_seconds()) if oldest is not None else 0
            )
        COLLECTOR_UP.labels("outbox").set(1)
    except Exception:
        COLLECTOR_UP.labels("outbox").set(0)


async def collect_dlq() -> None:
    parsed = urlsplit(settings.rabbitmq_url)
    virtual_host = unquote(parsed.path[1:]) or "/"
    url = (
        settings.rabbitmq_management_url.rstrip("/")
        + "/api/queues/"
        + quote(virtual_host, safe="")
        + "/payments.dlq"
    )
    try:
        async with asyncio.timeout(2):
            async with httpx.AsyncClient(timeout=1.5, trust_env=False) as client:
                response = await client.get(
                    url,
                    auth=(
                        unquote(parsed.username or "guest"),
                        unquote(parsed.password or "guest"),
                    ),
                )
                response.raise_for_status()
                messages = response.json()["messages"]
                if not isinstance(messages, int) or messages < 0:
                    raise ValueError("Invalid RabbitMQ queue metrics")
                DLQ_SIZE.set(messages)
        COLLECTOR_UP.labels("rabbitmq").set(1)
    except Exception:
        COLLECTOR_UP.labels("rabbitmq").set(0)


@asynccontextmanager
async def metrics_server(collect: Callable[[], Awaitable[None]]) -> AsyncIterator[None]:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            async with asyncio.timeout(5):
                raw = await reader.readuntil(b"\r\n\r\n")
                lines = raw.decode("latin-1").split("\r\n")
                authorized = any(
                    line.partition(":")[0].lower() == "x-api-key"
                    and secrets.compare_digest(
                        line.partition(":")[2].strip().encode(),
                        settings.api_key.encode(),
                    )
                    for line in lines[1:]
                )
                if not authorized:
                    status, body = "401 Unauthorized", b"Unauthorized\n"
                elif lines[0] != "GET /metrics HTTP/1.1":
                    status, body = "404 Not Found", b"Not found\n"
                else:
                    await collect()
                    status, body = "200 OK", generate_latest()
                writer.write(
                    (
                        f"HTTP/1.1 {status}\r\nContent-Type: {CONTENT_TYPE_LATEST}\r\n"
                        f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n"
                    ).encode()
                    + body
                )
                await writer.drain()
        except (
            TimeoutError,
            ValueError,
            asyncio.IncompleteReadError,
            ConnectionError,
            asyncio.LimitOverrunError,
        ):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(
        handle, "0.0.0.0", settings.metrics_port, limit=8192
    )
    try:
        async with server:
            yield
    finally:
        server.close()
        await server.wait_closed()
