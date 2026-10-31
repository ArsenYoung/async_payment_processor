from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from copy import copy
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    from fastapi import FastAPI
    from faststream.rabbit import RabbitBroker
    from httpx import AsyncClient

    from payment_processor.adapters.webhooks import WebhookClient
    from payment_processor.db.session import Database
    from payment_processor.messaging.schemas import PaymentCreatedEvent
    from payment_processor.services.payment_service import PaymentService


@pytest_asyncio.fixture
async def database() -> AsyncGenerator[Database, None]:
    from payment_processor.core.config import settings
    from payment_processor.db.session import Database

    database = Database(settings.database_url)
    try:
        yield database
    finally:
        await database.dispose()


@pytest_asyncio.fixture
async def outbox_database() -> AsyncIterator[Database]:
    from sqlalchemy.schema import CreateSchema, DropSchema

    from payment_processor.core.config import settings
    from payment_processor.db.models.base import Base
    from payment_processor.db.session import Database

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


@pytest_asyncio.fixture
async def routing_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[RabbitBroker]:
    from payment_processor.messaging import topology
    from payment_processor.messaging.broker import create_broker

    prefix = f"test-retry-{uuid4()}-"
    exchange = copy(topology.PAYMENTS_EXCHANGE)
    queue = copy(topology.PAYMENTS_NEW_QUEUE)
    exchange.name = prefix + exchange.name
    queue.name = prefix + queue.name
    monkeypatch.setattr(topology, "PAYMENTS_EXCHANGE", exchange)
    monkeypatch.setattr(topology, "PAYMENTS_NEW_QUEUE", queue)
    broker = create_broker()
    try:
        await broker.connect()
        await topology.declare_payment_topology(broker)
        await topology.declare_retry_topology(broker)
        yield broker
    finally:
        try:
            for definition in (
                topology.retry_queue(2),
                topology.retry_queue(3),
                topology.dead_letter_queue(),
                queue,
            ):
                declared = await broker.declare_queue(definition)
                await declared.delete(if_unused=False, if_empty=False)
            for definition in (
                topology.retry_exchange(),
                topology.dead_letter_exchange(),
                exchange,
            ):
                declared = await broker.declare_exchange(definition)
                await declared.delete(if_unused=False)
        finally:
            await broker.stop()


@pytest_asyncio.fixture
async def retry_event(
    outbox_database: Database, payment_payload: dict
) -> PaymentCreatedEvent:
    from sqlalchemy import select

    from payment_processor.api.schemas.payment import PaymentCreateRequest
    from payment_processor.bootstrap import create_payment_service
    from payment_processor.db.models import Outbox
    from payment_processor.messaging.schemas import PaymentCreatedEvent

    payment = await create_payment_service(
        outbox_database.session_scope
    ).create_payment(
        PaymentCreateRequest.model_validate(payment_payload), f"retry-{uuid4()}"
    )
    async with outbox_database.session_scope() as session:
        outbox = await session.scalar(
            select(Outbox).where(Outbox.payment_id == payment.payment_id)
        )
        assert outbox is not None
        return PaymentCreatedEvent.from_outbox(outbox)


@pytest.fixture
def retry_consumer(routing_broker: RabbitBroker) -> Callable:
    from faststream import BaseMiddleware

    from payment_processor.messaging.broker import create_broker
    from payment_processor.messaging.retry import RetryPolicy
    from payment_processor.rabbitmq_to_payment_worker.handlers import (
        register_payment_subscriber,
    )

    @asynccontextmanager
    async def start(service: PaymentService, webhook: WebhookClient) -> AsyncIterator:
        deliveries = []

        class ObserveDelivery(BaseMiddleware):
            async def on_receive(self) -> None:
                deliveries.append(self.msg)

        broker = create_broker()
        broker.add_middleware(ObserveDelivery)
        register_payment_subscriber(
            broker, service, webhook, retry_policy=RetryPolicy(base_delay=0.1)
        )
        try:
            await broker.connect()
            await broker.start()
            yield broker, deliveries
        finally:
            await broker.stop()

    return start


@pytest.fixture
def application(database: Database) -> FastAPI:
    from payment_processor.api.main import create_app

    return create_app(database=database)


@pytest.fixture
def override_payment_service(
    application: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[PaymentService], None]:
    from payment_processor.api.dependencies import get_payment_service

    def override(service: PaymentService) -> None:
        monkeypatch.setitem(
            application.dependency_overrides, get_payment_service, lambda: service
        )

    return override


@pytest_asyncio.fixture
async def client(application: FastAPI) -> AsyncGenerator[AsyncClient, None]:
    from asgi_lifespan import LifespanManager
    from httpx import ASGITransport, AsyncClient

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
    from sqlalchemy import delete, select

    from payment_processor.db.models import Outbox, Payment

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
    from payment_processor.core.config import settings

    return {
        "X-API-Key": settings.api_key,
        "Idempotency-Key": idempotency_key,
    }
