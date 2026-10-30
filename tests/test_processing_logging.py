import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.core import worker as worker_module
from payment_processor.core.config import settings
from payment_processor.core.logging import (
    ApplicationFormatter,
    processing_context,
    processing_fields,
)
from payment_processor.db.models import Outbox, Payment, PaymentStatus
from payment_processor.db.session import Database
from payment_processor.messaging import topology
from payment_processor.messaging.retry import ATTEMPT_HEADER, RetryAttempt
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.services.outbox_service import OutboxService, PublicationResult
from payment_processor.services.payment_gateway_simulator import PaymentGatewaySimulator
from payment_processor.services.payment_service import PaymentService
from payment_processor.services.webhooks import WebhookClient


@pytest.fixture(autouse=True)
def processing_log_level(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="payment_processor")


@pytest.fixture
def payment_payload() -> dict:
    return {
        "amount": "10.50",
        "currency": "RUB",
        "description": "private-description",
        "metadata": {"customer": "private-customer"},
        "webhook_url": "https://private-webhook.example/path?token=private-token",
    }


def entries(caplog: pytest.LogCaptureFixture) -> list[dict]:
    formatter = ApplicationFormatter(
        "consumer", (settings.api_key, settings.database_url, settings.rabbitmq_url)
    )
    output = [
        formatter.format(record)
        for record in caplog.records
        if record.name.startswith("payment_processor")
    ]
    assert "private-" not in "\n".join(output)
    return [json.loads(line) for line in output]


async def wait_for_log(
    caplog: pytest.LogCaptureFixture, operation: str, outcome: str, count: int = 1
) -> None:
    async with asyncio.timeout(10):
        while (
            sum(
                entry.get("operation") == operation and entry.get("outcome") == outcome
                for entry in entries(caplog)
            )
            < count
        ):
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("successful_attempt", [1, 2, 3])
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_real_processing_path_logs_saved_results_and_separate_attempts(
    outbox_database: Database,
    routing_broker: RabbitBroker,
    retry_event: PaymentCreatedEvent,
    retry_consumer: Callable,
    caplog: pytest.LogCaptureFixture,
    successful_attempt: int,
    status: PaymentStatus,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = status
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    requests = 0

    async def respond(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(503 if requests < successful_attempt else 204)

    async with WebhookClient(
        timeout=1, transport=httpx.MockTransport(respond)
    ) as webhook:
        async with retry_consumer(service, webhook):
            assert await OutboxService(
                outbox_database.session_scope, routing_broker
            ).publish_batch(10) == PublicationResult(1, 0)
            await wait_for_log(caplog, "consume", "completed")
            await topology.publish_payment_event(routing_broker, retry_event)
            await wait_for_log(caplog, "consume", "completed", 2)

    captured = entries(caplog)
    for entry in captured:
        if "operation" not in entry:
            continue
        assert entry["payment_id"] == str(retry_event.payload.payment_id)
        assert entry["event_id"] == str(retry_event.event_id)
        if "duration_ms" in entry:
            assert entry["duration_ms"] >= 0
    published = [
        entry for entry in captured if entry.get("operation") == "outbox_publish"
    ]
    assert len(published) == 1
    assert published[0]["publication_attempt"] == 1
    assert published[0]["outcome"] == "published"
    assert "attempt" not in published[0]
    received = [entry for entry in captured if entry.get("outcome") == "received"]
    assert [entry["attempt"] for entry in received] == [
        *range(1, successful_attempt + 1),
        1,
    ]
    retries = [entry for entry in captured if entry.get("operation") == "retry_publish"]
    assert [entry["next_attempt"] for entry in retries] == list(
        range(2, successful_attempt + 1)
    )
    for entry in retries:
        assert entry["outcome"] == "published"
        assert entry["reason"] == "retryable_processing_error"
        assert entry["error_type"] == "HTTPStatusError"
        assert entry["queue"] == topology.retry_queue(entry["next_attempt"]).name
        assert entry["retry_delay_ms"] == 100 * 2 ** (entry["attempt"] - 1)
    processed = [
        entry for entry in captured if entry.get("operation") == "payment_process"
    ]
    assert processed[0]["outcome"] == "processed"
    assert all(entry["payment_status"] == status.value for entry in processed)
    assert all(entry["outcome"] == "already_processed" for entry in processed[1:])
    deliveries = [
        entry for entry in captured if entry.get("operation") == "webhook_delivery"
    ]
    assert [entry["outcome"] for entry in deliveries] == [
        *(["failed"] * (successful_attempt - 1)),
        "delivered",
        "already_delivered",
    ]
    http = [entry for entry in captured if entry.get("operation") == "webhook_http"]
    assert [entry["status_code"] for entry in http] == [
        *([503] * (successful_attempt - 1)),
        204,
    ]
    gateway.process.assert_awaited_once()
    payment = await service.get_payment(retry_event.payload.payment_id)
    assert payment is not None
    assert payment.status == status
    assert payment.webhook_delivered_at is not None
    assert processing_fields() == {}


@pytest.mark.asyncio
async def test_real_dlq_logs_exhaustion_and_preserves_event_context(
    outbox_database: Database,
    routing_broker: RabbitBroker,
    retry_event: PaymentCreatedEvent,
    retry_consumer: Callable,
    caplog: pytest.LogCaptureFixture,
) -> None:
    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.side_effect = TimeoutError("private-gateway-error")
    service = PaymentService(outbox_database.session_scope, gateway=gateway)
    async with WebhookClient(timeout=1) as webhook:
        async with retry_consumer(service, webhook):
            await topology.publish_payment_event(routing_broker, retry_event)
            await wait_for_log(caplog, "dlq_publish", "published")
    captured = entries(caplog)
    dead = [entry for entry in captured if entry.get("operation") == "dlq_publish"]
    assert len(dead) == 1
    assert dead[0]["event_id"] == str(retry_event.event_id)
    assert dead[0]["payment_id"] == str(retry_event.payload.payment_id)
    assert dead[0]["queue"] == topology.dead_letter_queue().name
    assert dead[0]["attempt"] == 3
    assert dead[0]["reason"] == "attempts_exhausted"
    assert dead[0]["error_type"] == "TimeoutError"
    assert dead[0]["level"] == "ERROR"
    assert "next_attempt" not in dead[0]
    failures = [entry for entry in captured if entry.get("operation") == "consume"]
    assert [
        entry["exception"]["type"] for entry in failures if "exception" in entry
    ] == ["TimeoutError", "TimeoutError", "TimeoutError"]
    queue = await routing_broker.declare_queue(topology.dead_letter_queue())
    message = await queue.get(timeout=2)
    async with message.process():
        assert message.message_id == str(retry_event.event_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body,attempt", [(b"{private-invalid-json", 3), (None, "private-invalid-attempt")]
)
async def test_real_invalid_messages_keep_safe_available_context(
    outbox_database: Database,
    routing_broker: RabbitBroker,
    retry_event: PaymentCreatedEvent,
    retry_consumer: Callable,
    caplog: pytest.LogCaptureFixture,
    body: bytes | None,
    attempt: object,
) -> None:
    service = PaymentService(outbox_database.session_scope)
    async with WebhookClient(timeout=1) as webhook:
        async with retry_consumer(service, webhook):
            await routing_broker.publish(
                body if body else retry_event.model_dump(mode="json"),
                exchange=topology.PAYMENTS_EXCHANGE,
                routing_key=topology.PAYMENTS_NEW_QUEUE.routing(),
                headers={ATTEMPT_HEADER: attempt},
                message_id=str(retry_event.event_id),
            )
            await wait_for_log(caplog, "dlq_publish", "published")
    dead = next(
        entry for entry in entries(caplog) if entry.get("operation") == "dlq_publish"
    )
    assert dead["reason"] == "invalid_message"
    assert dead["event_id"] == str(retry_event.event_id)
    assert dead["attempt"] == (3 if body else 1)
    assert "payment_id" not in dead
    assert not any(
        entry.get("operation") == "payment_process" for entry in entries(caplog)
    )


@pytest.mark.asyncio
async def test_real_outbox_retry_logs_only_saved_publication_attempts(
    outbox_database: Database,
    routing_broker: RabbitBroker,
    retry_event: PaymentCreatedEvent,
    caplog: pytest.LogCaptureFixture,
) -> None:
    exchange = await routing_broker.declare_exchange(topology.PAYMENTS_EXCHANGE)
    queue = await routing_broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    await queue.unbind(exchange, routing_key=topology.PAYMENTS_NEW_QUEUE.routing())
    service = OutboxService(outbox_database.session_scope, routing_broker)
    try:
        assert await service.publish_batch(10) == PublicationResult(0, 1)
    finally:
        await queue.bind(exchange, routing_key=topology.PAYMENTS_NEW_QUEUE.routing())
    assert await service.publish_batch(10) == PublicationResult(0, 0)
    async with outbox_database.session_scope() as session:
        await session.execute(
            update(Outbox)
            .where(Outbox.published_at.is_(None))
            .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        )
    assert await service.publish_batch(10) == PublicationResult(1, 0)
    captured = [
        entry for entry in entries(caplog) if entry.get("operation") == "outbox_publish"
    ]
    assert [entry["outcome"] for entry in captured] == ["failed", "published"]
    assert [entry["publication_attempt"] for entry in captured] == [1, 2]
    assert all(entry["event_id"] == str(retry_event.event_id) for entry in captured)
    assert all("attempt" not in entry for entry in captured)
    assert captured[0]["exception"]["traceback"]
    async with outbox_database.session_scope() as session:
        saved = await session.get(Outbox, retry_event.event_id)
        assert saved is not None
        assert saved.attempts == 2
        assert saved.published_at is not None
    caplog.clear()
    assert await service.publish_batch(10) == PublicationResult(0, 0)
    assert entries(caplog) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["outbox", "payment", "webhook"])
async def test_rollback_never_logs_a_saved_success(
    outbox_database: Database,
    routing_broker: RabbitBroker,
    retry_event: PaymentCreatedEvent,
    caplog: pytest.LogCaptureFixture,
    operation: str,
) -> None:
    @asynccontextmanager
    async def failing_commit() -> AsyncIterator[AsyncSession]:
        async with outbox_database.session_scope() as session:
            yield session
            raise RuntimeError("private-commit-error")

    gateway = AsyncMock(spec=PaymentGatewaySimulator)
    gateway.process.return_value = PaymentStatus.SUCCEEDED
    service = PaymentService(failing_commit, gateway=gateway)
    if operation == "webhook":
        await PaymentService(
            outbox_database.session_scope, gateway=gateway
        ).process_payment(retry_event.payload.payment_id)
    caplog.clear()
    async with WebhookClient(
        timeout=1, transport=httpx.MockTransport(lambda request: httpx.Response(204))
    ) as webhook:
        with pytest.raises(RuntimeError):
            if operation == "outbox":
                await OutboxService(failing_commit, routing_broker).publish_batch(10)
            elif operation == "payment":
                await service.process_payment(retry_event.payload.payment_id)
            else:
                await service.deliver_webhook(retry_event.payload.payment_id, webhook)
    captured = entries(caplog)
    assert not any(
        entry.get("outcome") in ("published", "processed", "delivered")
        for entry in captured
    )
    assert any(entry.get("error_type") == "RuntimeError" for entry in captured)
    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, retry_event.payload.payment_id)
        outbox = await session.get(Outbox, retry_event.event_id)
        assert payment is not None and outbox is not None
        assert payment.webhook_delivered_at is None
        assert outbox.published_at is None
        assert outbox.attempts == 0
        assert payment.status == (
            PaymentStatus.SUCCEEDED if operation == "webhook" else PaymentStatus.PENDING
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("retry", [RetryAttempt(2, 0.1), None])
@pytest.mark.parametrize("confirmed", [True, False])
async def test_transfer_result_is_logged_after_broker_confirmation(
    routing_broker: RabbitBroker,
    retry_event: PaymentCreatedEvent,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    retry: RetryAttempt | None,
    confirmed: bool,
) -> None:
    await topology.publish_payment_event(routing_broker, retry_event)
    queue = await routing_broker.declare_queue(topology.PAYMENTS_NEW_QUEUE)
    incoming = await queue.get(timeout=2)
    received = asyncio.Event()
    release = asyncio.Event()
    publish = routing_broker.publish

    async def delayed_confirmation(*args: object, **kwargs: object) -> object:
        confirmation = await publish(*args, **kwargs)
        received.set()
        await release.wait()
        return confirmation if confirmed else None

    monkeypatch.setattr(routing_broker, "publish", delayed_confirmation)
    from faststream.rabbit.message import RabbitMessage

    message = RabbitMessage(incoming, body=incoming.body)

    async def transfer() -> None:
        with processing_context(
            event_id=str(retry_event.event_id),
            payment_id=str(retry_event.payload.payment_id),
        ):
            await topology.publish_failed_payment(
                routing_broker,
                message,
                attempt=1 if retry else 3,
                error=TimeoutError("private-transfer-reason"),
                reason="retryable_processing_error" if retry else "attempts_exhausted",
                retry=retry,
            )

    task = asyncio.create_task(transfer())
    try:
        async with asyncio.timeout(5):
            await received.wait()
            assert entries(caplog) == []
            release.set()
            if confirmed:
                await task
            else:
                with pytest.raises(RuntimeError):
                    await task
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await incoming.ack()
    [result] = entries(caplog)
    assert result["outcome"] == ("published" if confirmed else "failed")
    assert result["event_id"] == str(retry_event.event_id)
    assert result["duration_ms"] >= 0
    if not confirmed:
        assert result["error_type"] == "RuntimeError"
        assert result["exception"]["type"] == "RuntimeError"


@pytest.mark.asyncio
async def test_parallel_contexts_are_reset_after_failure_and_cancellation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("payment_processor.test")
    ready = asyncio.Event()

    async def record(number: int) -> None:
        with processing_context(event_id=f"event-{number}", attempt=number):
            await ready.wait()
            logger.info("Context captured", extra=processing_fields(operation="test"))
            if number == 2:
                raise RuntimeError("private-context-error")
            if number == 3:
                raise asyncio.CancelledError()

    tasks = [asyncio.create_task(record(number)) for number in (1, 2, 3)]
    ready.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert isinstance(results[1], RuntimeError)
    assert isinstance(results[2], asyncio.CancelledError)
    logger.info("Outside context", extra=processing_fields(operation="outside"))
    captured = entries(caplog)
    assert {(entry["event_id"], entry["attempt"]) for entry in captured[:3]} == {
        ("event-1", 1),
        ("event-2", 2),
        ("event-3", 3),
    }
    assert "event_id" not in captured[-1]
    assert "attempt" not in captured[-1]
    with processing_context(event_id="outer"):
        with processing_context(event_id="inner"):
            assert processing_fields()["event_id"] == "inner"
        assert processing_fields()["event_id"] == "outer"
    assert processing_fields() == {}


@pytest.mark.asyncio
async def test_worker_failure_logs_lifecycle_and_safe_traceback(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *args: None)
    monkeypatch.setattr(loop, "remove_signal_handler", lambda *args: True)
    monkeypatch.setattr(worker_module.signal, "signal", lambda *args: None)
    worker = AsyncMock(side_effect=RuntimeError("private-worker-error"))
    with pytest.raises(ExceptionGroup):
        await worker_module.run_worker(worker, name="test-worker")
    captured = entries(caplog)
    assert [entry["outcome"] for entry in captured] == ["starting", "failed", "failed"]
    assert all(entry["worker"] == "test-worker" for entry in captured)
    assert captured[1]["exception"]["type"] == "ExceptionGroup"
    assert captured[1]["exception"]["exceptions"][0]["type"] == "RuntimeError"


def test_formatter_excludes_sensitive_processing_fields() -> None:
    record = logging.makeLogRecord(
        {
            "name": "payment_processor.test",
            "msg": "Processing completed",
            "event_id": str(uuid4()),
            "queue": "payments.new",
            "attempt": 3,
            "publication_attempt": 5,
            "outcome": "completed",
            "duration_ms": 0.5,
            "payload": "private-payload",
            "metadata": {"customer": "private-customer"},
            "webhook_url": "https://private-webhook.example",
            "headers": "private-headers",
        }
    )
    formatted = ApplicationFormatter("consumer").format(record)
    entry = json.loads(formatted)
    assert entry["attempt"] == 3
    assert entry["publication_attempt"] == 5
    assert entry["event_id"] == record.event_id
    assert "private-" not in formatted
