import asyncio
import json
import logging
from collections.abc import AsyncIterator
from io import StringIO
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from payment_processor.api import main as api_main
from payment_processor.api.dependencies import get_payment_service
from payment_processor.api.middleware import RequestLoggingMiddleware
from payment_processor.core.config import settings
from payment_processor.core.logging import ApplicationFormatter, ConsoleHandler
from payment_processor.db.session import Database
from payment_processor.services.payment_service import PaymentService


@pytest.fixture
def log_output() -> StringIO:
    return StringIO()


@pytest.fixture
def logging_application(
    outbox_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    log_output: StringIO,
) -> FastAPI:
    monkeypatch.setattr(logging.getLogger(), "handlers", [])
    monkeypatch.setattr(
        api_main, "settings", settings.model_copy(update={"log_level": "DEBUG"})
    )
    application = api_main.create_app(database=outbox_database)
    for handler in logging.getLogger().handlers:
        if isinstance(handler, ConsoleHandler):
            handler.setStream(log_output)
    return application


@pytest_asyncio.fixture
async def logging_client(logging_application: FastAPI) -> AsyncIterator[AsyncClient]:
    async with LifespanManager(logging_application):
        async with AsyncClient(
            transport=ASGITransport(
                app=logging_application, raise_app_exceptions=False
            ),
            base_url="http://test",
        ) as client:
            yield client


def request_entries(captured: str) -> list[dict]:
    return [
        entry
        for line in captured.splitlines()
        if (entry := json.loads(line))["logger"] == "payment_processor.api.middleware"
    ]


def read_logs(output: StringIO) -> str:
    captured = output.getvalue()
    output.seek(0)
    output.truncate()
    return captured


@pytest.mark.integration
@pytest.mark.asyncio
async def test_payment_requests_have_unique_request_ids_and_saved_payment_id(
    logging_client: AsyncClient,
    payment_payload: dict,
    log_output: StringIO,
) -> None:
    headers = {"X-API-Key": settings.api_key, "Idempotency-Key": str(uuid4())}
    created = await logging_client.post(
        "/api/v1/payments", headers=headers, json=payment_payload
    )
    repeated = await logging_client.post(
        "/api/v1/payments", headers=headers, json=payment_payload
    )
    payment_id = created.json()["payment_id"]
    fetched = await logging_client.get(
        f"/api/v1/payments/{payment_id}", headers=headers
    )
    assert [created.status_code, repeated.status_code, fetched.status_code] == [
        202,
        202,
        200,
    ]
    assert repeated.json()["payment_id"] == payment_id
    entries = request_entries(read_logs(log_output))
    assert len(entries) == 3
    assert len({entry["request_id"] for entry in entries}) == 3
    for entry, response in zip(entries, (created, repeated, fetched), strict=True):
        assert entry["request_id"] == response.headers["X-Request-ID"]
        assert UUID(entry["request_id"]).version == 4
        assert entry["payment_id"] == payment_id
        assert entry["status_code"] == response.status_code
        assert entry["duration_ms"] >= 0
        assert entry["level"] == "INFO"
        assert "exception" not in entry


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path,authenticated,body,expected_status",
    [
        ("GET", "/health", True, None, 200),
        ("GET", "/health", False, None, 401),
        ("GET", "/api/v1/payments/not-a-uuid", True, None, 422),
        ("POST", "/api/v1/payments", True, {}, 422),
        ("POST", "/api/v1/payments", True, "{invalid-json", 422),
        ("GET", "/unknown", True, None, 404),
        ("GET", "/docs", False, None, 200),
    ],
)
async def test_request_context_covers_success_authentication_and_validation(
    logging_client: AsyncClient,
    log_output: StringIO,
    method: str,
    path: str,
    authenticated: bool,
    body: dict | str | None,
    expected_status: int,
) -> None:
    headers = {"X-API-Key": settings.api_key} if authenticated else {}
    response = await logging_client.request(
        method,
        path,
        headers=headers,
        **({"content": body} if isinstance(body, str) else {"json": body}),
    )
    assert response.status_code == expected_status
    entries = request_entries(read_logs(log_output))
    assert len(entries) == 1
    entry = entries[0]
    assert entry["request_id"] == response.headers["X-Request-ID"]
    assert entry["method"] == method
    assert entry["path"] == path
    assert entry["status_code"] == expected_status
    assert entry["duration_ms"] >= 0
    assert "payment_id" not in entry


@pytest.mark.integration
@pytest.mark.asyncio
async def test_conflicting_creation_logs_409_without_request_body(
    logging_client: AsyncClient,
    payment_payload: dict,
    log_output: StringIO,
) -> None:
    headers = {"X-API-Key": settings.api_key, "Idempotency-Key": str(uuid4())}
    await logging_client.post("/api/v1/payments", headers=headers, json=payment_payload)
    read_logs(log_output)
    response = await logging_client.post(
        "/api/v1/payments",
        headers=headers,
        json={**payment_payload, "description": "private-conflicting-description"},
    )
    captured = read_logs(log_output)
    assert response.status_code == 409
    entries = request_entries(captured)
    assert len(entries) == 1
    assert entries[0]["status_code"] == 409
    assert entries[0]["request_id"] == response.headers["X-Request-ID"]
    assert "private-conflicting-description" not in captured


@pytest.mark.integration
@pytest.mark.asyncio
async def test_unhandled_error_keeps_request_context_and_safe_traceback(
    logging_application: FastAPI,
    logging_client: AsyncClient,
    log_output: StringIO,
) -> None:
    service = Mock(spec=PaymentService)
    service.get_payment.side_effect = RuntimeError("opaque-exception-secret")
    logging_application.dependency_overrides[get_payment_service] = lambda: service
    payment_id = str(uuid4())
    response = await logging_client.get(
        f"/api/v1/payments/{payment_id}?token=private-query",
        headers={"X-API-Key": settings.api_key},
    )
    captured = read_logs(log_output)
    assert response.status_code == 500
    assert response.text == "Internal Server Error"
    entries = request_entries(captured)
    assert len(entries) == 1
    entry = entries[0]
    assert entry["request_id"] == response.headers["X-Request-ID"]
    assert entry["payment_id"] == payment_id
    assert entry["path"] == f"/api/v1/payments/{payment_id}"
    assert entry["status_code"] == 500
    assert entry["duration_ms"] >= 0
    assert entry["level"] == "ERROR"
    assert entry["exception"]["type"] == "RuntimeError"
    assert entry["exception"]["traceback"]
    assert "opaque-exception-secret" not in captured
    assert "private-query" not in captured
    healthy = await logging_client.get(
        "/health", headers={"X-API-Key": settings.api_key}
    )
    next_entry = request_entries(read_logs(log_output))[0]
    assert next_entry["request_id"] == healthy.headers["X-Request-ID"]
    assert next_entry["request_id"] != entry["request_id"]
    assert "payment_id" not in next_entry
    assert "exception" not in next_entry


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("valid_request", [True, False])
async def test_queries_headers_and_bodies_are_not_logged(
    logging_client: AsyncClient,
    payment_payload: dict,
    log_output: StringIO,
    valid_request: bool,
) -> None:
    response = await logging_client.post(
        "/api/v1/payments?token=private-query",
        headers={
            "X-API-Key": settings.api_key if valid_request else "private-invalid-key",
            "Idempotency-Key": "private-idempotency-key",
            "X-Other-Key": "private-other-key",
            "Authorization": "Bearer private-authorization",
            "X-Request-ID": "private-incoming-request-id",
        },
        json={
            **payment_payload,
            "description": "private-description",
            "metadata": {"customer": "private-metadata"},
            "webhook_url": "https://private-webhook.example/path?token=secret",
        },
    )
    assert response.status_code == (202 if valid_request else 401)
    captured = read_logs(log_output)
    entries = request_entries(captured)
    assert len(entries) == 1
    assert entries[0]["path"] == "/api/v1/payments"
    assert UUID(response.headers["X-Request-ID"]).version == 4
    assert "private-" not in captured
    assert settings.api_key not in captured
    assert "metadata" not in captured
    assert "webhook_url" not in captured


@pytest.mark.integration
@pytest.mark.asyncio
async def test_parallel_requests_keep_their_own_payment_and_request_ids(
    logging_client: AsyncClient,
    payment_payload: dict,
    log_output: StringIO,
) -> None:
    created = await asyncio.gather(
        *(
            logging_client.post(
                "/api/v1/payments",
                headers={
                    "X-API-Key": settings.api_key,
                    "Idempotency-Key": str(uuid4()),
                },
                json=payment_payload,
            )
            for _ in range(3)
        )
    )
    assert all(response.status_code == 202 for response in created)
    payment_ids = [response.json()["payment_id"] for response in created]
    payment_ids.append(str(uuid4()))
    fetched = await asyncio.gather(
        *(
            logging_client.get(
                f"/api/v1/payments/{payment_id}",
                headers={"X-API-Key": settings.api_key},
            )
            for payment_id in payment_ids
        )
    )
    entries = request_entries(read_logs(log_output))
    by_request_id = {entry["request_id"]: entry for entry in entries}
    assert len(entries) == len(by_request_id) == 7
    for response in created:
        entry = by_request_id[response.headers["X-Request-ID"]]
        assert entry["payment_id"] == response.json()["payment_id"]
        assert entry["method"] == "POST"
    for payment_id, response in zip(payment_ids, fetched, strict=True):
        entry = by_request_id[response.headers["X-Request-ID"]]
        assert entry["payment_id"] == payment_id
        assert entry["path"] == f"/api/v1/payments/{payment_id}"
        assert entry["method"] == "GET"
        assert entry["status_code"] == response.status_code
    assert [response.status_code for response in fetched] == [200, 200, 200, 404]


@pytest.mark.unit
def test_formatter_serializes_only_allowed_request_fields() -> None:
    record = logging.makeLogRecord(
        {
            "name": "payment_processor.test",
            "msg": "HTTP request completed",
            "request_id": str(uuid4()),
            "method": "GET",
            "path": "/health",
            "status_code": 200,
            "duration_ms": 1.25,
            "metadata": {"customer": "private-metadata"},
            "headers": {"X-API-Key": "private-key"},
            "payload": "private-payload",
        }
    )
    formatted = ApplicationFormatter("api").format(record)
    entry = json.loads(formatted)
    assert entry["request_id"] == record.request_id
    assert entry["duration_ms"] == 1.25
    assert entry["status_code"] == 200
    assert "private-" not in formatted
    assert not {"metadata", "headers", "payload"} & entry.keys()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_non_http_scopes_pass_through_without_request_logging(
    capsys: pytest.CaptureFixture[str],
) -> None:
    application = AsyncMock()
    scope = {"type": "lifespan"}
    receive, send = AsyncMock(), AsyncMock()
    await RequestLoggingMiddleware(application)(scope, receive, send)
    application.assert_awaited_once_with(scope, receive, send)
    assert "state" not in scope
    assert capsys.readouterr().out == ""
