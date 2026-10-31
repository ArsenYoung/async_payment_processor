import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError

from payment_processor.adapters.webhooks import (
    WebhookClient,
    WebhookRequestTimeoutError,
)
from payment_processor.application.results import PaymentDetailsResult
from payment_processor.bootstrap import create_payment_service
from payment_processor.core.config import Settings
from payment_processor.db.models import Payment, PaymentCurrency, PaymentStatus
from payment_processor.db.session import Database
from payment_processor.messaging.schemas import PaymentResultPayload


@pytest.fixture
def completed_payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("9999999999999999.99"),
        currency=PaymentCurrency.RUB,
        description="Оплата книги",
        payment_metadata={
            "заказ": [{"товар": "Книга"}, True, None, 0.5],
            "number": 9007199254740993,
        },
        idempotency_key=f"test-webhook-{uuid4()}",
        webhook_url="https://example.com/webhook",
        status=PaymentStatus.SUCCEEDED,
        created_at=datetime(2026, 10, 28, 10, 0, tzinfo=UTC),
        processed_at=datetime(2026, 10, 28, 10, 0, 3, tzinfo=UTC),
    )


@pytest.fixture
def expected_payload(completed_payment: Payment) -> dict:
    return {
        "payment_id": str(completed_payment.id),
        "status": completed_payment.status.value,
        "amount": "9999999999999999.99",
        "currency": "RUB",
        "description": "Оплата книги",
        "metadata": {
            "заказ": [{"товар": "Книга"}, True, None, 0.5],
            "number": 9007199254740993,
        },
        "created_at": "2026-10-28T10:00:00Z",
        "processed_at": "2026-10-28T10:00:03Z",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [200, 201, 202, 204, 299])
async def test_webhook_posts_result_and_reuses_client(
    completed_payment: Payment, expected_payload: dict, status_code: int
) -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code)

    async with WebhookClient(
        timeout=2.5, transport=httpx.MockTransport(handle)
    ) as client:
        await client.send(PaymentDetailsResult.from_payment(completed_payment))
        await client.send(PaymentDetailsResult.from_payment(completed_payment))

    assert len(requests) == 2
    for request in requests:
        assert request.method == "POST"
        assert str(request.url) == completed_payment.webhook_url
        assert request.headers["x-webhook-delivery-id"] == str(completed_payment.id)
        assert request.headers["content-type"] == "application/json"
        assert json.loads(request.content) == expected_payload
        assert request.extensions["timeout"] == {
            "connect": 2.5,
            "read": 2.5,
            "write": 2.5,
            "pool": 2.5,
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_webhook_sends_saved_payment_result(
    outbox_database: Database,
    completed_payment: Payment,
    expected_payload: dict,
    status: PaymentStatus,
) -> None:
    completed_payment.status = status
    expected_payload["status"] = status.value
    async with outbox_database.session_scope() as session:
        session.add(completed_payment)
    payment = await create_payment_service(outbox_database.session_scope).get_payment(
        completed_payment.id
    )
    assert payment is not None
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    async with WebhookClient(
        timeout=5.0, transport=httpx.MockTransport(handle)
    ) as client:
        await client.send(payment)

    assert len(requests) == 1
    assert str(requests[0].url) == completed_payment.webhook_url
    assert json.loads(requests[0].content) == expected_payload


@pytest.mark.parametrize("currency", list(PaymentCurrency))
def test_webhook_payload_preserves_currency_and_decimal(
    completed_payment: Payment, currency: PaymentCurrency
) -> None:
    completed_payment.currency = currency
    completed_payment.amount = Decimal("10.50")

    payload = PaymentResultPayload.from_result(
        PaymentDetailsResult.from_payment(completed_payment)
    )

    assert payload.model_dump(mode="json")["currency"] == currency.value
    assert payload.model_dump(mode="json")["amount"] == "10.50"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", PaymentStatus.PENDING),
        ("processed_at", None),
        ("processed_at", datetime(2026, 10, 28)),
    ],
)
async def test_incomplete_result_is_rejected_before_sending(
    completed_payment: Payment, field: str, value: object
) -> None:
    setattr(completed_payment, field, value)
    handle = AsyncMock(return_value=httpx.Response(200))

    async with WebhookClient(
        timeout=5.0, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ValidationError) as caught:
            await client.send(PaymentDetailsResult.from_payment(completed_payment))

    assert caught.value.errors()[0]["loc"] == (field,)
    handle.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [100, 301, 302, 307, 400, 404, 429, 500, 503])
async def test_non_success_response_propagates_without_redirect_or_retry(
    completed_payment: Payment, status_code: int
) -> None:
    handle = AsyncMock(
        return_value=httpx.Response(
            status_code, headers={"Location": "https://other.example/webhook"}
        )
    )

    async with WebhookClient(
        timeout=5.0, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            await client.send(PaymentDetailsResult.from_payment(completed_payment))

    assert caught.value.response.status_code == status_code
    assert str(caught.value.request.url) == completed_payment.webhook_url
    handle.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type",
    [
        httpx.ConnectError,
        httpx.ReadError,
        httpx.ConnectTimeout,
        httpx.ReadTimeout,
        httpx.WriteTimeout,
        httpx.PoolTimeout,
    ],
)
async def test_network_failure_propagates_without_retry(
    completed_payment: Payment, error_type: type[httpx.RequestError]
) -> None:
    error = error_type("Webhook unavailable")
    handle = AsyncMock(side_effect=error)

    async with WebhookClient(
        timeout=5.0, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(error_type) as caught:
            await client.send(PaymentDetailsResult.from_payment(completed_payment))

    assert caught.value is error
    handle.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "http", "network", "cancellation"])
async def test_client_closes_transport_on_exit(
    completed_payment: Payment, monkeypatch: pytest.MonkeyPatch, failure: str | None
) -> None:
    handle = AsyncMock(return_value=httpx.Response(200))
    error: BaseException | None = None
    if failure == "http":
        handle.return_value = httpx.Response(500)
    elif failure == "network":
        error = httpx.ConnectError("Connection lost")
        handle.side_effect = error
    elif failure == "cancellation":
        error = asyncio.CancelledError()
        handle.side_effect = error
    transport = httpx.MockTransport(handle)
    close = AsyncMock(wraps=transport.aclose)
    monkeypatch.setattr(transport, "aclose", close)
    client = WebhookClient(timeout=5.0, transport=transport)

    async def send() -> None:
        async with client:
            await client.send(PaymentDetailsResult.from_payment(completed_payment))

    if failure is None:
        await send()
    else:
        error_type = type(error) if error is not None else httpx.HTTPStatusError
        with pytest.raises(error_type):
            await send()

    close.assert_awaited_once_with()
    with pytest.raises(RuntimeError, match="client has been closed"):
        await client.send(PaymentDetailsResult.from_payment(completed_payment))


@pytest.mark.asyncio
async def test_webhook_request_deadline_covers_slow_response_headers(
    completed_payment: Payment,
) -> None:
    async def handle(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.2)
        return httpx.Response(204)

    async with WebhookClient(
        timeout=5.0,
        request_deadline=0.01,
        transport=httpx.MockTransport(handle),
    ) as client:
        with pytest.raises(WebhookRequestTimeoutError):
            await client.send(PaymentDetailsResult.from_payment(completed_payment))


@pytest.mark.asyncio
async def test_webhook_stream_does_not_consume_unused_large_response_body(
    completed_payment: Payment,
) -> None:
    class LargeResponseStream(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            raise AssertionError("webhook response body must not be consumed")
            yield b""

        async def aclose(self) -> None:
            self.closed = True

    response_stream = LargeResponseStream()

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204, stream=response_stream)

    async with WebhookClient(
        timeout=5.0,
        request_deadline=1.0,
        transport=httpx.MockTransport(handle),
    ) as client:
        await client.send(PaymentDetailsResult.from_payment(completed_payment))

    assert response_stream.closed


@pytest.mark.parametrize(
    "timeout", [0, -0.1, float("nan"), float("inf"), float("-inf")]
)
def test_webhook_timeout_rejects_invalid_values(timeout: float) -> None:
    values = {
        "_env_file": None,
        "database_url": "postgresql+asyncpg://test:test@localhost/test",
        "rabbitmq_url": "amqp://test:test@localhost/",
        "api_key": "test-key",
        "webhook_timeout": timeout,
    }
    with pytest.raises(ValidationError) as caught:
        Settings(**values)
    assert caught.value.errors()[0]["loc"] == ("webhook_timeout",)
    with pytest.raises(ValueError, match="positive and finite"):
        WebhookClient(timeout=timeout)


def test_webhook_timeout_default_and_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "_env_file": None,
        "database_url": "postgresql+asyncpg://test:test@localhost/test",
        "rabbitmq_url": "amqp://test:test@localhost/",
        "api_key": "test-key",
    }
    monkeypatch.delenv("WEBHOOK_TIMEOUT", raising=False)
    assert Settings(**values).webhook_timeout == 5.0
    monkeypatch.setenv("WEBHOOK_TIMEOUT", "2.5")
    assert Settings(**values).webhook_timeout == 2.5


def test_webhook_request_deadline_default_and_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {
        "_env_file": None,
        "database_url": "postgresql+asyncpg://test:test@localhost/test",
        "rabbitmq_url": "amqp://test:test@localhost/",
        "api_key": "test-key",
    }
    monkeypatch.delenv("WEBHOOK_REQUEST_DEADLINE", raising=False)
    assert Settings(**values).webhook_request_deadline == 5.0
    monkeypatch.setenv("WEBHOOK_REQUEST_DEADLINE", "2.5")
    assert Settings(**values).webhook_request_deadline == 2.5


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["response_lost", "server_error"])
async def test_delivery_id_survives_retry_with_new_client_and_receiver_deduplicates(
    completed_payment: Payment, expected_payload: dict, failure: str
) -> None:
    requests: list[httpx.Request] = []
    accepted: set[str] = set()
    effects: list[dict] = []

    def receive(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        delivery_id = request.headers["x-webhook-delivery-id"]
        if delivery_id not in accepted:
            accepted.add(delivery_id)
            effects.append(json.loads(request.content))
        if len(requests) == 1:
            if failure == "response_lost":
                raise httpx.ReadTimeout("Receiver accepted but response was lost")
            return httpx.Response(503)
        return httpx.Response(204)

    snapshot = PaymentDetailsResult.from_payment(completed_payment)
    error = httpx.ReadTimeout if failure == "response_lost" else httpx.HTTPStatusError
    async with WebhookClient(
        timeout=1, transport=httpx.MockTransport(receive)
    ) as client:
        with pytest.raises(error):
            await client.send(snapshot)
    async with WebhookClient(
        timeout=1, transport=httpx.MockTransport(receive)
    ) as restarted_client:
        await restarted_client.send(snapshot)
    assert len(requests) == 2
    assert accepted == {str(completed_payment.id)}
    assert effects == [expected_payload]
    assert requests[0].content == requests[1].content


@pytest.mark.asyncio
async def test_different_payments_have_distinct_delivery_ids(
    completed_payment: Payment,
) -> None:
    delivery_ids: list[str] = []

    def receive(request: httpx.Request) -> httpx.Response:
        delivery_ids.append(request.headers["x-webhook-delivery-id"])
        return httpx.Response(204)

    async with WebhookClient(
        timeout=1, transport=httpx.MockTransport(receive)
    ) as client:
        await client.send(PaymentDetailsResult.from_payment(completed_payment))
        completed_payment.id = uuid4()
        await client.send(PaymentDetailsResult.from_payment(completed_payment))
    assert len(set(delivery_ids)) == 2
    assert delivery_ids[1] == str(completed_payment.id)
