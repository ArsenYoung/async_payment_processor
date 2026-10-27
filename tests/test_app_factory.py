from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, Mock, call

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from payment_processor.api import main as api_main
from payment_processor.api.main import create_app
from payment_processor.core.config import settings
from payment_processor.db.session import Database


@pytest_asyncio.fixture
async def applications() -> AsyncIterator[tuple[FastAPI, FastAPI]]:
    first = create_app()
    second = create_app()
    assert first is not second
    try:
        yield first, second
    finally:
        await first.state.database.dispose()
        await second.state.database.dispose()


@pytest.mark.asyncio
async def test_applications_have_independent_database_connections(
    applications: tuple[FastAPI, FastAPI],
) -> None:
    first: Database = applications[0].state.database
    second: Database = applications[1].state.database
    assert first is not second
    assert first.engine is not second.engine
    assert first.session_factory is not second.session_factory

    await first.check_connection()
    async with second.session_factory() as session:
        connection_id = await session.scalar(text("SELECT pg_backend_pid()"))

    await first.dispose()

    async with second.session_factory() as session:
        assert await session.scalar(text("SELECT pg_backend_pid()")) == connection_id


@pytest.mark.asyncio
async def test_payment_route_uses_database_of_its_application(
    applications: tuple[FastAPI, FastAPI],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction_factories: list[Mock] = []
    for index, application in enumerate(applications):
        error = RuntimeError(f"Database of application {index}")
        transaction_factory = Mock(side_effect=error)
        transaction_factories.append(transaction_factory)
        monkeypatch.setattr(
            application.state.database, "session_scope", transaction_factory
        )

        async with AsyncClient(
            transport=ASGITransport(app=application), base_url="http://test"
        ) as client:
            with pytest.raises(RuntimeError) as caught:
                await client.post(
                    "/api/v1/payments",
                    headers={
                        "X-API-Key": settings.api_key,
                        "Idempotency-Key": "test-database-dependency",
                    },
                    json={
                        "amount": "10.50",
                        "currency": "RUB",
                        "description": "Test payment",
                        "webhook_url": "https://example.com/webhook",
                    },
                )

        assert caught.value is error

    for transaction_factory in transaction_factories:
        transaction_factory.assert_called_once_with()


@pytest.mark.asyncio
async def test_lifespan_checks_supplied_database_and_releases_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Mock(spec=Database)
    broker = Mock(connect=AsyncMock(), stop=AsyncMock())
    resources = Mock()
    resources.attach_mock(database, "database")
    resources.attach_mock(broker, "broker")
    monkeypatch.setattr(api_main, "create_broker", lambda: broker)
    application = create_app(database=database)
    assert application.state.database is database

    async with LifespanManager(application):
        assert application.state.broker is broker
        resources.running()

    assert resources.mock_calls == [
        call.database.check_connection(),
        call.broker.connect(),
        call.running(),
        call.broker.stop(),
        call.database.dispose(),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["database", "broker-connect", "body", "broker-stop"]
)
async def test_lifespan_releases_database_when_startup_or_shutdown_fails(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    error = RuntimeError(f"Failure during {failure}")
    database = Mock(spec=Database)
    broker = Mock(connect=AsyncMock(), stop=AsyncMock())
    if failure == "database":
        database.check_connection.side_effect = error
    elif failure == "broker-connect":
        broker.connect.side_effect = error
    elif failure == "broker-stop":
        broker.stop.side_effect = error
    monkeypatch.setattr(api_main, "create_broker", lambda: broker)
    application = create_app(database=database)

    with pytest.raises(RuntimeError) as caught:
        async with LifespanManager(application):
            if failure == "body":
                raise error

    assert caught.value is error
    database.dispose.assert_awaited_once_with()
    if failure == "database":
        broker.connect.assert_not_awaited()
        broker.stop.assert_not_awaited()
    else:
        broker.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_each_application_has_healthcheck(
    applications: tuple[FastAPI, FastAPI],
) -> None:
    for application in applications:
        async with AsyncClient(
            transport=ASGITransport(app=application), base_url="http://test"
        ) as client:
            response = await client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_each_application_has_protected_payment_route(
    applications: tuple[FastAPI, FastAPI],
) -> None:
    for application in applications:
        async with AsyncClient(
            transport=ASGITransport(app=application), base_url="http://test"
        ) as client:
            response = await client.post("/api/v1/payments", json={})

        assert response.status_code == 401
        assert response.json() == {"detail": "Invalid API key"}


@pytest.mark.asyncio
async def test_each_application_uses_validation_error_handler(
    applications: tuple[FastAPI, FastAPI],
) -> None:
    for application in applications:
        async with AsyncClient(
            transport=ASGITransport(app=application), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1/payments",
                headers={
                    "X-API-Key": settings.api_key,
                    "Idempotency-Key": "test-app-factory",
                },
                json={
                    "amount": "-1.00",
                    "currency": "RUB",
                    "description": "Test payment",
                    "webhook_url": "https://example.com/webhook",
                },
            )

        assert response.status_code == 422
        errors = response.json()["detail"]
        assert len(errors) == 1
        assert errors[0]["loc"] == ["body", "amount"]
        assert set(errors[0]) == {"type", "loc", "msg"}
