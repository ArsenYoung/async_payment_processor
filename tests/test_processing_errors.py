import asyncio
import json
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.exc import TimeoutError as DatabaseTimeoutError

from payment_processor.adapters.payment_gateway import PaymentGatewaySimulator
from payment_processor.adapters.webhooks import WebhookClient
from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.application.errors import PaymentNotFoundError
from payment_processor.bootstrap import create_payment_service
from payment_processor.db.models import Payment, PaymentStatus
from payment_processor.db.session import Database
from payment_processor.messaging.errors import is_retryable_processing_error


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("Gateway timeout"),
        ConnectionError("Gateway unavailable"),
        httpx.ConnectError("Connection failed"),
        httpx.ReadTimeout("Response timeout"),
        httpx.RemoteProtocolError("Connection closed"),
        DatabaseTimeoutError("Pool exhausted"),
        DisconnectionError("Connection closed"),
        OperationalError("COMMIT", {}, RuntimeError("Connection lost")),
        InterfaceError("SELECT 1", {}, RuntimeError("Connection closed")),
        DBAPIError(
            "COMMIT", {}, RuntimeError("Connection lost"), connection_invalidated=True
        ),
    ],
)
def test_transient_dependency_errors_are_retryable(error: BaseException) -> None:
    assert is_retryable_processing_error(error)


@pytest.mark.parametrize("status_code", [301, 400, 401, 404, 408, 429, 500, 503])
def test_unsuccessful_webhook_responses_are_retryable(status_code: int) -> None:
    request = httpx.Request("POST", "https://example.com/webhook")
    response = httpx.Response(status_code, request=request)
    with pytest.raises(httpx.HTTPStatusError) as caught:
        response.raise_for_status()

    assert is_retryable_processing_error(caught.value)


@pytest.mark.parametrize(
    ("sqlstate", "retryable"),
    [
        ("08003", True),
        ("08006", True),
        ("40001", True),
        ("40P01", True),
        ("53100", True),
        ("53300", True),
        ("55P03", True),
        ("57P01", True),
        ("57P02", True),
        ("57P03", True),
        ("23505", False),
        ("23514", False),
        ("28000", False),
        ("28P01", False),
        ("42501", False),
        ("42P01", False),
        ("57014", False),
    ],
)
@pytest.mark.parametrize("error_type", [DBAPIError, OperationalError])
def test_database_sqlstate_controls_retry(
    sqlstate: str, retryable: bool, error_type: type[DBAPIError]
) -> None:
    original = RuntimeError("Database failure")
    original.sqlstate = sqlstate
    error = error_type("SELECT 1", {}, original)

    assert is_retryable_processing_error(error) is retryable


@pytest.mark.parametrize(
    "error",
    [
        PaymentNotFoundError(uuid4()),
        ValueError("Invalid payload"),
        ValidationError.from_exception_data("PaymentCreatedEvent", []),
        TypeError("Invalid dependency"),
        RuntimeError("Unexpected failure"),
        IntegrityError("INSERT", {}, RuntimeError("Constraint violation")),
        ProgrammingError("SELECT", {}, RuntimeError("Invalid query")),
        DBAPIError("SELECT", {}, RuntimeError("Unknown database error")),
        asyncio.CancelledError(),
        KeyboardInterrupt(),
        SystemExit(),
    ],
)
def test_permanent_errors_and_cancellation_are_not_retryable(
    error: BaseException,
) -> None:
    assert not is_retryable_processing_error(error)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["process_payment", "deliver_webhook"])
async def test_missing_payment_has_explicit_non_retryable_error(
    outbox_database: Database, operation: str
) -> None:
    payment_id = uuid4()
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    webhook = AsyncMock(spec=WebhookClient)
    service = create_payment_service(outbox_database.session_scope, gateway=gateway)

    with pytest.raises(PaymentNotFoundError) as caught:
        if operation == "process_payment":
            await service.process_payment(payment_id)
        else:
            await service.deliver_webhook(payment_id, webhook)

    assert isinstance(caught.value, LookupError)
    assert caught.value.payment_id == payment_id
    assert str(payment_id) in str(caught.value)
    assert not is_retryable_processing_error(caught.value)
    gateway.process.assert_not_awaited()
    webhook.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_gateway_outcome_is_saved_and_delivered(
    outbox_database: Database, payment_payload: dict
) -> None:
    gateway = PaymentGatewaySimulator(success_rate=0.0, sleep=AsyncMock())
    service = create_payment_service(outbox_database.session_scope, gateway=gateway)
    payment = await service.create_payment(
        PaymentCreateRequest.model_validate(payment_payload), f"errors-{uuid4()}"
    )
    result = await service.process_payment(payment.payment_id)
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(handle)
    ) as webhook:
        await service.deliver_webhook(payment.payment_id, webhook)

    assert result.status == PaymentStatus.FAILED
    assert len(requests) == 1
    assert json.loads(requests[0].content)["status"] == "failed"
    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, payment.payment_id)
        assert saved is not None
        assert saved.status == PaymentStatus.FAILED
        assert saved.processed_at == result.processed_at is not None
        assert saved.webhook_delivered_at is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["timeout", "connection"])
async def test_retryable_gateway_error_preserves_pending_payment(
    outbox_database: Database, payment_payload: dict, failure: str
) -> None:
    error = (
        TimeoutError("Gateway timeout")
        if failure == "timeout"
        else ConnectionError("Gateway unavailable")
    )
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.side_effect = error
    service = create_payment_service(outbox_database.session_scope, gateway=gateway)
    payment = await service.create_payment(
        PaymentCreateRequest.model_validate(payment_payload), f"errors-{uuid4()}"
    )

    with pytest.raises(type(error)) as caught:
        await service.process_payment(payment.payment_id)

    assert caught.value is error
    assert is_retryable_processing_error(caught.value)
    gateway.process.assert_awaited_once_with(idempotency_key=payment.payment_id)
    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, payment.payment_id)
        assert saved is not None
        assert saved.status == PaymentStatus.PENDING
        assert saved.processed_at is None
        assert saved.webhook_delivered_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
@pytest.mark.parametrize("failure", ["http", "timeout", "connection"])
async def test_retryable_webhook_error_preserves_gateway_result(
    outbox_database: Database,
    payment_payload: dict,
    status: PaymentStatus,
    failure: str,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = create_payment_service(outbox_database.session_scope, gateway=gateway)
    payment = await service.create_payment(
        PaymentCreateRequest.model_validate(payment_payload), f"errors-{uuid4()}"
    )
    result = await service.process_payment(payment.payment_id)
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("Webhook timeout", request=request)
        if failure == "connection":
            raise httpx.ConnectError("Webhook unavailable", request=request)
        return httpx.Response(503)

    async with WebhookClient(
        timeout=1.0, transport=httpx.MockTransport(handle)
    ) as webhook:
        with pytest.raises(httpx.HTTPError) as caught:
            await service.deliver_webhook(payment.payment_id, webhook)

    assert is_retryable_processing_error(caught.value)
    assert len(requests) == 1
    assert json.loads(requests[0].content)["status"] == status.value
    async with outbox_database.session_scope() as session:
        saved = await session.get(Payment, payment.payment_id)
        assert saved is not None
        assert saved.status == status
        assert saved.processed_at == result.processed_at is not None
        assert saved.webhook_delivered_at is None
