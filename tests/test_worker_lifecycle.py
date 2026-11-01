import asyncio
import signal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest
import pytest_asyncio

from payment_processor.core import worker as worker_module

pytestmark = pytest.mark.unit


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name", ["outbox-to-rabbitmq-worker", "rabbitmq-to-payment-worker"]
)
@pytest.mark.parametrize("shutdown_signal", [signal.SIGINT, signal.SIGTERM])
async def test_signal_waits_for_named_worker_cleanup(
    loop_signals: SimpleNamespace, name: str, shutdown_signal: signal.Signals
) -> None:
    entered = asyncio.Event()
    stopped = asyncio.Event()
    waiting = asyncio.Event()

    async def work() -> None:
        assert asyncio.current_task().get_name() == name
        entered.set()
        try:
            await waiting.wait()
        finally:
            stopped.set()

    task = asyncio.create_task(worker_module.run_worker(work, name=name))
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            loop_signals.handlers[shutdown_signal]()
            await task
    finally:
        await _finish_task(task)

    assert stopped.is_set()
    _assert_signal_cleanup(loop_signals)


@pytest.mark.asyncio
async def test_worker_exception_terminates_supervisor(
    loop_signals: SimpleNamespace,
) -> None:
    error = RuntimeError("Worker failed")
    work = AsyncMock(side_effect=error)

    async with asyncio.timeout(2):
        with pytest.raises(ExceptionGroup) as caught:
            await worker_module.run_worker(work, name="test-worker")

    assert caught.value.exceptions == (error,)
    _assert_signal_cleanup(loop_signals)


@pytest.mark.asyncio
async def test_unexpected_worker_return_is_reported(
    loop_signals: SimpleNamespace,
) -> None:
    work = AsyncMock(return_value=None)

    async with asyncio.timeout(2):
        with pytest.raises(ExceptionGroup) as caught:
            await worker_module.run_worker(work, name="test-worker")

    [error] = caught.value.exceptions
    assert isinstance(error, RuntimeError)
    assert str(error) == "Worker test-worker stopped unexpectedly"
    _assert_signal_cleanup(loop_signals)


@pytest.mark.asyncio
async def test_external_cancellation_waits_for_worker_cleanup(
    loop_signals: SimpleNamespace,
) -> None:
    entered = asyncio.Event()
    stopped = asyncio.Event()
    waiting = asyncio.Event()

    async def work() -> None:
        entered.set()
        try:
            await waiting.wait()
        finally:
            stopped.set()

    task = asyncio.create_task(worker_module.run_worker(work, name="test-worker"))
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        await _finish_task(task)

    assert stopped.is_set()
    _assert_signal_cleanup(loop_signals)


@pytest.mark.asyncio
async def test_repeated_signal_does_not_interrupt_cleanup(
    loop_signals: SimpleNamespace,
) -> None:
    entered = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    waiting = asyncio.Event()

    async def work() -> None:
        entered.set()
        try:
            await waiting.wait()
        finally:
            cleaning.set()
            await release.wait()

    task = asyncio.create_task(worker_module.run_worker(work, name="test-worker"))
    try:
        async with asyncio.timeout(2):
            await entered.wait()
            loop_signals.handlers[signal.SIGTERM]()
            await cleaning.wait()
            loop_signals.handlers[signal.SIGTERM]()
            release.set()
            await task
    finally:
        release.set()
        await _finish_task(task)

    _assert_signal_cleanup(loop_signals)


@pytest.mark.asyncio
async def test_signal_registration_failure_restores_installed_handler(
    loop_signals: SimpleNamespace,
) -> None:
    error = RuntimeError("Signal registration failed")
    loop_signals.register.side_effect = [None, error]
    work = AsyncMock()

    with pytest.raises(RuntimeError) as caught:
        await worker_module.run_worker(work, name="test-worker")

    assert caught.value is error
    work.assert_not_awaited()
    assert loop_signals.remove.call_args_list == [call(signal.SIGINT)]
    assert loop_signals.restore.call_args_list == [call(signal.SIGINT, signal.SIG_DFL)]


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
