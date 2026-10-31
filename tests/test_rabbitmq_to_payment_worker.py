import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, call

import pytest

from payment_processor.adapters.webhooks import WebhookClient
from payment_processor.db.session import Database
from payment_processor.rabbitmq_to_payment_worker import main as consumer_main
from payment_processor.services.payment_service import PaymentService


@pytest.fixture
def consumer_runtime(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    database = Mock(spec=Database)
    broker = Mock(
        connect=AsyncMock(), start=AsyncMock(), stop=AsyncMock(), publish=AsyncMock()
    )
    declare = AsyncMock()
    declare_retry = AsyncMock()
    webhook = MagicMock(spec=WebhookClient)
    webhook.__aenter__.return_value = webhook
    webhook_factory = Mock(return_value=webhook)
    service = Mock(spec=PaymentService)
    service_factory = Mock(return_value=service)
    register = Mock()
    entered = asyncio.Event()

    async def start() -> None:
        entered.set()

    broker.start.side_effect = start
    monkeypatch.setattr(consumer_main, "Database", Mock(return_value=database))
    monkeypatch.setattr(consumer_main, "create_broker", Mock(return_value=broker))
    monkeypatch.setattr(consumer_main, "declare_payment_topology", declare)
    monkeypatch.setattr(consumer_main, "declare_retry_topology", declare_retry)
    monkeypatch.setattr(consumer_main, "WebhookClient", webhook_factory)
    monkeypatch.setattr(consumer_main, "create_payment_service", service_factory)
    monkeypatch.setattr(consumer_main, "register_payment_subscriber", register)
    operations = Mock()
    operations.attach_mock(database, "database")
    operations.attach_mock(broker, "broker")
    operations.attach_mock(declare, "declare")
    operations.attach_mock(declare_retry, "declare_retry")
    operations.attach_mock(webhook, "webhook")
    operations.attach_mock(register, "register")
    return SimpleNamespace(
        database=database,
        broker=broker,
        declare=declare,
        declare_retry=declare_retry,
        webhook=webhook,
        webhook_factory=webhook_factory,
        service=service,
        service_factory=service_factory,
        register=register,
        entered=entered,
        operations=operations,
    )


@pytest.mark.asyncio
async def test_consumer_uses_its_own_worker_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_worker = AsyncMock()
    monkeypatch.setattr(consumer_main, "run_worker", run_worker)

    await consumer_main.run_consumer()

    run_worker.assert_awaited_once_with(
        consumer_main._consume, name="rabbitmq-to-payment-worker"
    )


@pytest.mark.asyncio
async def test_consumer_starts_subscribers_without_reading_or_publishing_outbox(
    consumer_runtime: SimpleNamespace,
) -> None:
    runtime = consumer_runtime
    task = asyncio.create_task(consumer_main._consume())
    try:
        async with asyncio.timeout(2):
            await runtime.entered.wait()
            runtime.database.session_scope.assert_not_called()
            runtime.broker.publish.assert_not_awaited()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        await _finish_task(task)

    assert runtime.operations.mock_calls == [
        call.database.check_connection(),
        call.webhook.__aenter__(),
        call.register(
            runtime.broker,
            runtime.service,
            runtime.webhook,
            retry_policy=runtime.register.call_args.kwargs["retry_policy"],
        ),
        call.broker.connect(),
        call.declare(runtime.broker),
        call.declare_retry(
            runtime.broker, runtime.register.call_args.kwargs["retry_policy"]
        ),
        call.broker.start(),
        call.broker.stop(),
        call.webhook.__aexit__(
            asyncio.CancelledError,
            runtime.webhook.__aexit__.call_args.args[1],
            runtime.webhook.__aexit__.call_args.args[2],
        ),
        call.database.dispose(),
    ]
    policy = runtime.register.call_args.kwargs["retry_policy"]
    assert policy.max_attempts == consumer_main.settings.retry_max_attempts
    assert policy.backoff_multiplier == consumer_main.settings.retry_backoff_multiplier
    assert policy.next_retry(1).delay == consumer_main.settings.retry_base_delay
    runtime.webhook_factory.assert_called_once_with(
        timeout=consumer_main.settings.webhook_timeout,
        request_deadline=consumer_main.settings.webhook_request_deadline,
    )
    consumer_main.create_broker.assert_called_once_with(
        prefetch_count=consumer_main.settings.consumer_prefetch_count
    )
    runtime.service_factory.assert_called_once_with(runtime.database.session_scope)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage",
    [
        "database",
        "webhook-enter",
        "registration",
        "broker",
        "topology",
        "retry-topology",
        "subscribers",
        "broker-stop",
        "webhook-exit",
        "database-dispose",
    ],
)
async def test_consumer_failure_closes_opened_resources(
    consumer_runtime: SimpleNamespace, stage: str
) -> None:
    runtime = consumer_runtime
    error = RuntimeError(f"Failure during {stage}")
    operation = {
        "database": runtime.database.check_connection,
        "webhook-enter": runtime.webhook.__aenter__,
        "registration": runtime.register,
        "broker": runtime.broker.connect,
        "topology": runtime.declare,
        "retry-topology": runtime.declare_retry,
        "subscribers": runtime.broker.start,
        "broker-stop": runtime.broker.stop,
        "webhook-exit": runtime.webhook.__aexit__,
        "database-dispose": runtime.database.dispose,
    }[stage]
    operation.side_effect = error
    task = asyncio.create_task(consumer_main._consume())
    try:
        async with asyncio.timeout(2):
            if stage in ("broker-stop", "webhook-exit", "database-dispose"):
                await runtime.entered.wait()
                task.cancel()
            with pytest.raises(RuntimeError) as caught:
                await task
    finally:
        await _finish_task(task)

    assert caught.value is error
    runtime.database.dispose.assert_awaited_once_with()
    if stage in ("database", "webhook-enter"):
        runtime.broker.stop.assert_not_awaited()
        runtime.webhook.__aexit__.assert_not_awaited()
    else:
        runtime.broker.stop.assert_awaited_once_with()
        runtime.webhook.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage",
    [
        "database",
        "webhook-enter",
        "broker",
        "topology",
        "retry-topology",
        "subscribers",
    ],
)
async def test_cancellation_during_consumer_startup_closes_opened_resources(
    consumer_runtime: SimpleNamespace, stage: str
) -> None:
    runtime = consumer_runtime
    entered = asyncio.Event()
    waiting = asyncio.Event()

    async def stalled(*args: object, **kwargs: object) -> None:
        entered.set()
        await waiting.wait()

    operation = {
        "database": runtime.database.check_connection,
        "webhook-enter": runtime.webhook.__aenter__,
        "broker": runtime.broker.connect,
        "topology": runtime.declare,
        "retry-topology": runtime.declare_retry,
        "subscribers": runtime.broker.start,
    }[stage]
    operation.side_effect = stalled
    task = asyncio.create_task(consumer_main._consume())
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        await _finish_task(task)

    runtime.database.dispose.assert_awaited_once_with()
    if stage in ("database", "webhook-enter"):
        runtime.broker.stop.assert_not_awaited()
        runtime.webhook.__aexit__.assert_not_awaited()
    else:
        runtime.broker.stop.assert_awaited_once_with()
        runtime.webhook.__aexit__.assert_awaited_once()


async def _finish_task(task: asyncio.Task) -> None:
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)
