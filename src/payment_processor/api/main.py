from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from payment_processor.db.session import check_database_connection, engine
from payment_processor.messaging.broker import create_broker


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None, None]:
    """Open infrastructure connections when the API starts."""
    await check_database_connection()
    broker = create_broker()
    await broker.connect()
    application.state.broker = broker

    try:
        yield
    finally:
        await broker.stop()
        await engine.dispose()


def create_app() -> FastAPI:
    return FastAPI(title="Payment Processing Service", lifespan=lifespan)


app = create_app()


@app.get("/health", tags=["health"])
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}
