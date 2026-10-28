import asyncio
import signal
from copy import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy.exc import TimeoutError as DatabaseTimeoutError

from payment_processor.api.schemas.payment import PaymentCreateRequest
from payment_processor.core import worker as worker_module
from payment_processor.core.config import Settings
from payment_processor.db.models import Outbox
from payment_processor.db.session import Database
from payment_processor.messaging import topology
from payment_processor.messaging.broker import create_broker
from payment_processor.messaging.schemas import PaymentCreatedEvent
from payment_processor.outbox_to_rabbitmq_worker import main as publisher_main
from payment_processor.services import outbox as outbox_module
from payment_processor.services.outbox import OutboxService, PublicationResult
from payment_processor.services.payments import PaymentService


@pytest.fixture
def settings_values(monkeypatch: pytest.MonkeyPatch) -> dict:
    for name in (
        "OUTBOX_BATCH_SIZE",
        "OUTBOX_POLL_INTERVAL",
        "OUTBOX_PUBLISH_TIMEOUT",
    ):
        monkeypatch.delenv(name, raising=False)
    return {
        "_env_file": None,
        "database_url": "postgresql+asyncpg://test:test@localhost/test",
        "rabbitmq_url": "amqp://test:test@localhost/",
        "api_key": "test-key",
    }


def test_outbox_settings_defaults(settings_values: dict) -> None:
    settings = Settings(**settings_values)
    assert settings.outbox_batch_size == 100
    assert settings.outbox_poll_interval == 1.0
    assert settings.outbox_publish_timeout == 5.0


def test_outbox_settings_read_environment(
    settings_values: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OUTBOX_BATCH_SIZE", "7")
    monkeypatch.setenv("OUTBOX_POLL_INTERVAL", "0.25")
    monkeypatch.setenv("OUTBOX_PUBLISH_TIMEOUT", "2.5")

    settings = Settings(**settings_values)

    assert settings.outbox_batch_size == 7
    assert settings.outbox_poll_interval == 0.25
    assert settings.outbox_publish_timeout == 2.5


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("outbox_batch_size", 0),
        ("outbox_batch_size", -1),
        ("outbox_batch_size", 1.5),
        *[
            (name, value)
            for name in ("outbox_poll_interval", "outbox_publish_timeout")
            for value in (0, -0.1, float("nan"), float("inf"), float("-inf"))
        ],
    ],
)
def test_invalid_outbox_settings_are_rejected(
    settings_values: dict, name: str, value: object
) -> None:
    with pytest.raises(ValidationError) as caught:
        Settings(**{**settings_values, name: value})

    assert caught.value.errors()[0]["loc"] == (name,)


@pytest.fixture
def publisher_loop(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    service = OutboxService(Mock(), Mock())
    service.publish_batch = AsyncMock()
    pause = AsyncMock(side_effect=asyncio.CancelledError())
    monkeypatch.setattr(outbox_module, "asyncio", SimpleNamespace(sleep=pause))
    operations = Mock()
    operations.attach_mock(service.publish_batch, "publish")
    operations.attach_mock(pause, "pause")
    return SimpleNamespace(service=service, pause=pause, operations=operations)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [PublicationResult(0, 0), PublicationResult(2, 0), PublicationResult(1, 1)],
)
async def test_publisher_pauses_after_empty_successful_and_failed_batches(
    publisher_loop: SimpleNamespace, result: PublicationResult
) -> None:
    publisher_loop.service.publish_batch.return_value = result

    with pytest.raises(asyncio.CancelledError):
        await publisher_loop.service.run(
            batch_size=7, poll_interval=0.25, publish_timeout=2.5
        )

    assert publisher_loop.operations.mock_calls == [
        call.publish(7, timeout=2.5),
        call.pause(0.25),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        OperationalError("SELECT 1", {}, RuntimeError("Database unavailable")),
        InterfaceError("SELECT 1", {}, RuntimeError("Connection lost")),
        DBAPIError(
            "SELECT 1", {}, RuntimeError("Disconnected"), connection_invalidated=True
        ),
        DisconnectionError("Pool disconnected"),
        DatabaseTimeoutError("Pool exhausted"),
        ConnectionError("Database connection refused"),
        TimeoutError("Database connection timed out"),
    ],
)
async def test_temporary_database_failure_is_logged_and_retried_after_pause(
    publisher_loop: SimpleNamespace, caplog: pytest.LogCaptureFixture, error: Exception
) -> None:
    publisher_loop.service.publish_batch.side_effect = [error, PublicationResult(1, 0)]
    publisher_loop.pause.side_effect = [None, asyncio.CancelledError()]

    with pytest.raises(asyncio.CancelledError):
        await publisher_loop.service.run(
            batch_size=7, poll_interval=0.25, publish_timeout=2.5
        )

    assert publisher_loop.operations.mock_calls == [
        call.publish(7, timeout=2.5),
        call.pause(0.25),
        call.publish(7, timeout=2.5),
        call.pause(0.25),
    ]
    assert "Outbox database unavailable" in caplog.text
    assert any(
        record.exc_info and record.exc_info[1] is error for record in caplog.records
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        RuntimeError("Publisher bug"),
        IntegrityError("UPDATE outbox", {}, RuntimeError("Constraint violated")),
        ProgrammingError("SELECT outbox", {}, RuntimeError("Invalid query")),
        ValueError("Invalid configuration"),
        asyncio.CancelledError(),
    ],
)
async def test_unexpected_error_or_cancellation_leaves_publisher_loop(
    publisher_loop: SimpleNamespace, error: BaseException
) -> None:
    publisher_loop.service.publish_batch.side_effect = error

    with pytest.raises(type(error)) as caught:
        await publisher_loop.service.run(
            batch_size=7, poll_interval=0.25, publish_timeout=2.5
        )

    assert caught.value is error
    publisher_loop.pause.assert_not_awaited()


@pytest_asyncio.fixture
async def loop_signals(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    handlers = {}
    register = Mock(
        side_effect=lambda name, callback: handlers.__setitem__(name, callback)
    )
    remove = Mock(return_value=True)
    restore = Mock()
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", register)
    monkeypatch.setattr(loop, "remove_signal_handler", remove)
    monkeypatch.setattr(
        worker_module,
        "signal",
        SimpleNamespace(
            SIGINT=signal.SIGINT,
            SIGTERM=signal.SIGTERM,
            getsignal=Mock(return_value=signal.SIG_DFL),
            signal=restore,
        ),
    )
    return SimpleNamespace(
        handlers=handlers, register=register, remove=remove, restore=restore
    )


@pytest.fixture
def publisher_runtime(
    monkeypatch: pytest.MonkeyPatch, loop_signals: SimpleNamespace
) -> SimpleNamespace:
    database = Mock(spec=Database)
    broker = Mock(connect=AsyncMock(), stop=AsyncMock())
    service = Mock(spec=OutboxService)
    database_factory = Mock(return_value=database)
    broker_factory = Mock(return_value=broker)
    service_factory = Mock(return_value=service)
    declare = AsyncMock()
    operations = Mock()
    operations.attach_mock(database, "database")
    operations.attach_mock(broker, "broker")
    operations.attach_mock(service, "service")
    operations.attach_mock(declare, "declare")
    entered = asyncio.Event()
    waiting = asyncio.Event()

    async def run_publisher(**kwargs: object) -> None:
        entered.set()
        try:
            await waiting.wait()
        finally:
            operations.publisher_stopped()

    service.run.side_effect = run_publisher
    monkeypatch.setattr(publisher_main, "Database", database_factory)
    monkeypatch.setattr(publisher_main, "create_broker", broker_factory)
    monkeypatch.setattr(publisher_main, "OutboxService", service_factory)
    monkeypatch.setattr(publisher_main, "declare_payment_topology", declare)
    monkeypatch.setattr(
        publisher_main,
        "settings",
        SimpleNamespace(
            database_url="test-database-url",
            outbox_batch_size=7,
            outbox_poll_interval=0.25,
            outbox_publish_timeout=2.5,
        ),
    )
    return SimpleNamespace(
        database=database,
        broker=broker,
        service=service,
        database_factory=database_factory,
        broker_factory=broker_factory,
        service_factory=service_factory,
        declare=declare,
        operations=operations,
        entered=entered,
        signals=loop_signals,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown_signal", [signal.SIGINT, signal.SIGTERM])
async def test_publisher_stops_publisher_before_closing_broker_and_database(
    publisher_runtime: SimpleNamespace, shutdown_signal: signal.Signals
) -> None:
    runtime = publisher_runtime
    task = asyncio.create_task(publisher_main.run_publisher())
    try:
        async with asyncio.timeout(2):
            await runtime.entered.wait()
            runtime.signals.handlers[shutdown_signal]()
            await task
    finally:
        await _finish_task(task)

    assert runtime.operations.mock_calls == [
        call.database.check_connection(),
        call.broker.connect(),
        call.declare(runtime.broker),
        call.service.run(batch_size=7, poll_interval=0.25, publish_timeout=2.5),
        call.publisher_stopped(),
        call.broker.stop(),
        call.database.dispose(),
    ]
    runtime.service_factory.assert_called_once_with(
        runtime.database.session_scope, runtime.broker
    )
    _assert_signal_cleanup(runtime.signals)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["database", "broker", "topology"])
async def test_signal_during_startup_closes_opened_resources(
    publisher_runtime: SimpleNamespace, stage: str
) -> None:
    runtime = publisher_runtime
    entered = asyncio.Event()
    waiting = asyncio.Event()

    async def stalled(*args: object, **kwargs: object) -> None:
        entered.set()
        await waiting.wait()

    if stage == "database":
        runtime.database.check_connection.side_effect = stalled
    elif stage == "broker":
        runtime.broker.connect.side_effect = stalled
    else:
        runtime.declare.side_effect = stalled

    task = asyncio.create_task(publisher_main.run_publisher())
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            runtime.signals.handlers[signal.SIGTERM]()
            await task
    finally:
        await _finish_task(task)

    runtime.database.dispose.assert_awaited_once_with()
    runtime.service.run.assert_not_awaited()
    if stage == "database":
        runtime.broker.stop.assert_not_awaited()
    else:
        runtime.broker.stop.assert_awaited_once_with()
    _assert_signal_cleanup(runtime.signals)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage",
    [
        "database-check",
        "broker-create",
        "broker-connect",
        "topology",
        "service-create",
        "publisher",
        "broker-stop",
        "database-dispose",
    ],
)
async def test_publisher_failure_is_reported_and_resources_are_closed(
    publisher_runtime: SimpleNamespace, stage: str
) -> None:
    runtime = publisher_runtime
    error = RuntimeError(f"Failure during {stage}")
    operation = {
        "database-check": runtime.database.check_connection,
        "broker-create": runtime.broker_factory,
        "broker-connect": runtime.broker.connect,
        "topology": runtime.declare,
        "service-create": runtime.service_factory,
        "publisher": runtime.service.run,
        "broker-stop": runtime.broker.stop,
        "database-dispose": runtime.database.dispose,
    }[stage]
    operation.side_effect = error
    task = asyncio.create_task(publisher_main.run_publisher())
    try:
        async with asyncio.timeout(2):
            if stage in ("broker-stop", "database-dispose"):
                await runtime.entered.wait()
                runtime.signals.handlers[signal.SIGTERM]()
            with pytest.raises(ExceptionGroup) as caught:
                await task
    finally:
        await _finish_task(task)

    assert caught.value.exceptions == (error,)
    runtime.database.dispose.assert_awaited_once_with()
    if stage in ("database-check", "broker-create"):
        runtime.broker.stop.assert_not_awaited()
    else:
        runtime.broker.stop.assert_awaited_once_with()
    _assert_signal_cleanup(runtime.signals)


@pytest.mark.asyncio
async def test_running_publisher_publishes_new_payment_automatically(
    outbox_database: Database,
    payment_payload: dict,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exchange = copy(topology.PAYMENTS_EXCHANGE)
    queue = copy(topology.PAYMENTS_NEW_QUEUE)
    prefix = f"test-lifecycle-{uuid4()}-"
    exchange.name = prefix + exchange.name
    queue.name = prefix + queue.name
    monkeypatch.setattr(topology, "PAYMENTS_EXCHANGE", exchange)
    monkeypatch.setattr(topology, "PAYMENTS_NEW_QUEUE", queue)
    monkeypatch.setattr(publisher_main, "Database", lambda _: outbox_database)
    monkeypatch.setattr(
        publisher_main,
        "settings",
        SimpleNamespace(
            database_url="isolated-test-database",
            outbox_batch_size=1,
            outbox_poll_interval=0.01,
            outbox_publish_timeout=2.0,
        ),
    )
    started = asyncio.Event()
    committed = asyncio.Event()

    class ObservedService(OutboxService):
        async def publish_batch(
            self, limit: int, *, timeout: float = 5.0
        ) -> PublicationResult:
            result = await super().publish_batch(limit, timeout=timeout)
            started.set()
            if result.published:
                committed.set()
            return result

    monkeypatch.setattr(publisher_main, "OutboxService", ObservedService)
    observer = create_broker()
    try:
        await observer.connect()
        declared_exchange = await observer.declare_exchange(exchange)
        try:
            declared_queue = await observer.declare_queue(queue)
            try:
                task = asyncio.create_task(publisher_main._publish())
                try:
                    async with asyncio.timeout(5):
                        await started.wait()
                        service = PaymentService(outbox_database.session_scope)
                        payment = await service.create_payment(
                            PaymentCreateRequest.model_validate(payment_payload),
                            f"test-lifecycle-{uuid4()}",
                        )
                        await committed.wait()
                        message = await declared_queue.get(timeout=2)
                        async with message.process():
                            event = PaymentCreatedEvent.model_validate_json(
                                message.body
                            )
                            assert event.payload.payment_id == payment.id
                        async with outbox_database.session_scope() as session:
                            record = await session.scalar(
                                select(Outbox).where(Outbox.payment_id == payment.id)
                            )
                            assert record is not None
                            assert record.published_at is not None
                            assert record.attempts == 1
                        task.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await task
                finally:
                    await _finish_task(task)
            finally:
                await declared_queue.delete(if_unused=False, if_empty=False)
        finally:
            await declared_exchange.delete(if_unused=False)
    finally:
        await observer.stop()


async def _finish_task(task: asyncio.Task) -> None:
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def _assert_signal_cleanup(loop_signals: SimpleNamespace) -> None:
    assert loop_signals.remove.call_args_list == [
        call(signal.SIGINT),
        call(signal.SIGTERM),
    ]
    assert loop_signals.restore.call_args_list == [
        call(signal.SIGINT, signal.SIG_DFL),
        call(signal.SIGTERM, signal.SIG_DFL),
    ]
