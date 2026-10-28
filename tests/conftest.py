from collections.abc import AsyncGenerator, AsyncIterator, Callable
from uuid import uuid4

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, select
from sqlalchemy.schema import CreateSchema, DropSchema

from payment_processor.api.dependencies import get_payment_service
from payment_processor.api.main import create_app
from payment_processor.core.config import settings
from payment_processor.db.models import Outbox, Payment
from payment_processor.db.models.base import Base
from payment_processor.db.session import Database
from payment_processor.services.payment_service import PaymentService


@pytest_asyncio.fixture
async def database() -> AsyncGenerator[Database, None]:
    database = Database(settings.database_url)
    try:
        yield database
    finally:
        await database.dispose()


@pytest_asyncio.fixture
async def outbox_database() -> AsyncIterator[Database]:
    schema = f"test_outbox_{uuid4().hex}"
    database = Database(settings.database_url)
    database.engine = database.engine.execution_options(
        schema_translate_map={None: schema}
    )
    database.session_factory.configure(bind=database.engine)

    try:
        async with database.engine.begin() as connection:
            await connection.execute(CreateSchema(schema))
        try:
            async with database.engine.begin() as connection:
                await connection.run_sync(Base.metadata.create_all)
            yield database
        finally:
            async with database.engine.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True))
    finally:
        await database.dispose()


@pytest.fixture
def application(database: Database) -> FastAPI:
    return create_app(database=database)


@pytest.fixture
def override_payment_service(
    application: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[PaymentService], None]:
    def override(service: PaymentService) -> None:
        monkeypatch.setitem(
            application.dependency_overrides, get_payment_service, lambda: service
        )

    return override


@pytest_asyncio.fixture
async def client(application: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    async with LifespanManager(application):
        transport = ASGITransport(app=application)
        async with AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as test_client:
            yield test_client


@pytest_asyncio.fixture
async def idempotency_key(database: Database) -> AsyncGenerator[str, None]:
    key = f"test-payment-{uuid4()}"
    try:
        yield key
    finally:
        await _delete_payment(database, key)


async def _delete_payment(database: Database, idempotency_key: str) -> None:
    async with database.session_scope() as session:
        payment_id = await session.scalar(
            select(Payment.id).where(Payment.idempotency_key == idempotency_key)
        )
        if payment_id is not None:
            await session.execute(delete(Outbox).where(Outbox.payment_id == payment_id))
            await session.execute(delete(Payment).where(Payment.id == payment_id))


@pytest.fixture
def payment_payload() -> dict:
    return {
        "amount": "10.50",
        "currency": "RUB",
        "description": "Test payment",
        "metadata": {"order_id": "test-order"},
        "webhook_url": "https://example.com/webhook",
    }


@pytest.fixture
def payment_headers(idempotency_key: str) -> dict[str, str]:
    return {
        "X-API-Key": settings.api_key,
        "Idempotency-Key": idempotency_key,
    }
