import json
import logging
import sys
from collections.abc import Iterator
from types import ModuleType
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from asgi_lifespan import LifespanManager
from pydantic import ValidationError
from uvicorn import Config
from uvicorn.lifespan.on import LifespanOn

from payment_processor.api import main as api_main
from payment_processor.core.config import Settings
from payment_processor.core.logging import ApplicationFormatter, configure_logging
from payment_processor.db.session import Database
from payment_processor.messaging.broker import create_broker
from payment_processor.outbox_to_rabbitmq_worker import main as publisher_main
from payment_processor.rabbitmq_to_payment_worker import main as consumer_main


@pytest.fixture
def logging_state() -> Iterator[None]:
    root = logging.getLogger()
    loggers = [root] + [
        logger
        for logger in root.manager.loggerDict.values()
        if isinstance(logger, logging.Logger)
    ]
    states = {
        logger: (
            logger.handlers[:],
            logger.filters[:],
            logger.level,
            logger.propagate,
            logger.disabled,
        )
        for logger in loggers
    }
    root.handlers = []
    try:
        yield
    finally:
        for logger in [root] + [
            logger
            for logger in root.manager.loggerDict.values()
            if isinstance(logger, logging.Logger)
        ]:
            if logger in states:
                handlers, filters, level, propagate, disabled = states[logger]
                logger.handlers = handlers
                logger.filters = filters
                logger.setLevel(level)
                logger.propagate = propagate
                logger.disabled = disabled
            else:
                logger.handlers.clear()
                logger.filters.clear()
                logger.setLevel(logging.NOTSET)
                logger.propagate = True
                logger.disabled = False


@pytest.fixture
def logging_settings(monkeypatch: pytest.MonkeyPatch) -> Settings:
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    return Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://user:database-password@localhost/payments",
        rabbitmq_url="amqp://user:rabbit%2Fpassword@localhost/",
        api_key="private-api-key",
    )


def test_logging_defaults_to_info(logging_settings: Settings) -> None:
    assert logging_settings.log_level == "INFO"


@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_log_level_is_read_from_environment(
    logging_settings: Settings, monkeypatch: pytest.MonkeyPatch, level: str
) -> None:
    monkeypatch.setenv("LOG_LEVEL", f" {level.lower()} ")
    settings = Settings(
        _env_file=None,
        **logging_settings.model_dump(exclude={"log_level"}),
    )
    assert settings.log_level == level


@pytest.mark.parametrize("value", ["", "TRACE", "WARN", "NOTSET", "10", "unknown"])
def test_unknown_log_level_is_rejected_without_echoing_its_value(
    logging_settings: Settings, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("LOG_LEVEL", value)
    with pytest.raises(ValidationError) as caught:
        Settings(
            _env_file=None,
            **logging_settings.model_dump(exclude={"log_level"}),
        )
    assert caught.value.errors()[0]["loc"] == ("log_level",)
    assert "input_value=" not in str(caught.value)


def test_formatter_uses_utc_and_keeps_each_entry_on_one_line() -> None:
    record = logging.LogRecord(
        "payment_processor.test", logging.INFO, __file__, 1, "First\nSecond", (), None
    )
    record.created = 0
    formatted = ApplicationFormatter("api").format(record)
    assert len(formatted.splitlines()) == 1
    assert json.loads(formatted) == {
        "timestamp": "1970-01-01T00:00:00.000Z",
        "level": "INFO",
        "component": "api",
        "logger": "payment_processor.test",
        "message": "First\nSecond",
    }


@pytest.mark.usefixtures("logging_state")
def test_reconfiguration_emits_once_and_updates_level_and_component(
    logging_settings: Settings, capsys: pytest.CaptureFixture[str]
) -> None:
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logger = logging.getLogger("payment_processor.test")
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.propagate = False
    configure_logging("api", logging_settings)
    logger.info("First entry")
    logging_settings.log_level = "DEBUG"
    configure_logging("outbox-to-rabbitmq-worker", logging_settings)
    configure_logging("outbox-to-rabbitmq-worker", logging_settings)
    logger.debug("Outbox batch: %d published", 3)
    entries = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [
        (entry["component"], entry["level"], entry["message"]) for entry in entries
    ] == [
        ("api", "INFO", "First entry"),
        ("outbox-to-rabbitmq-worker", "DEBUG", "Outbox batch: 3 published"),
    ]


@pytest.mark.usefixtures("logging_state")
@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_selected_level_applies_to_application_and_server(
    logging_settings: Settings, capsys: pytest.CaptureFixture[str], level: str
) -> None:
    logging_settings.log_level = level
    configure_logging("api", logging_settings)
    for name in ("payment_processor.test", "uvicorn.error"):
        logger = logging.getLogger(name)
        for number in (10, 20, 30, 40, 50):
            logger.log(number, "Level check")
    entries = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    threshold = logging.getLevelNamesMapping()[level]
    assert len(entries) == 2 * len(
        [number for number in (10, 20, 30, 40, 50) if number >= threshold]
    )
    assert all(
        logging.getLevelNamesMapping()[entry["level"]] >= threshold for entry in entries
    )


@pytest.mark.usefixtures("logging_state")
def test_uvicorn_default_handlers_are_replaced_and_query_is_not_logged(
    logging_settings: Settings, capsys: pytest.CaptureFixture[str]
) -> None:
    Config("payment_processor.api.main:app")
    configure_logging("api", logging_settings)
    logging.getLogger("uvicorn.error").info("Application startup complete.")
    logging.getLogger("uvicorn.access").info(
        '%s - "%s %s HTTP/%s" %d',
        "127.0.0.1:1234",
        "GET",
        "/health?private-query=value",
        "1.1",
        200,
    )
    captured = capsys.readouterr()
    entries = [json.loads(line) for line in captured.out.splitlines()]
    assert captured.err == ""
    assert [entry["message"] for entry in entries] == [
        "Application startup complete.",
        "GET /health 200",
    ]
    assert "private-query" not in captured.out


@pytest.mark.usefixtures("logging_state")
@pytest.mark.parametrize(
    "message,arguments",
    [
        ("Key %s", ("private-api-key",)),
        ("Password %s", ("database-password",)),
        ("Password %s", ("rabbit/password",)),
        ("Password %s", ("rabbit%2Fpassword",)),
        ("URL https://example.org/private-hook?token=webhook-secret", ()),
        ("URL amqp://unknown:unknown-password@host/", ()),
        ("X-API-Key: unknown-key", ()),
        ("X-Other-Key: another-key", ()),
        ("Authorization: Bearer unknown-token", ()),
        ("headers={'X-API-Key': 'unknown-key'}", ()),
        ('metadata={"customer": {"name": "private-customer"}} trailing', ()),
        ('payload={"data": ["private-payload"]}', ()),
        ("request_body=private-body", ()),
        ("response_body=private-response", ()),
        ("Data %s", ({"customer": "private-customer"},)),
        ("Data %s", (["private-payload"],)),
        ("Error %s", (RuntimeError("private-error"),)),
        ("Error %d", (RuntimeError("private-error"),)),
    ],
)
def test_sensitive_data_is_redacted_from_messages(
    logging_settings: Settings,
    capsys: pytest.CaptureFixture[str],
    message: str,
    arguments: tuple,
) -> None:
    configure_logging("api", logging_settings)
    logging.getLogger("payment_processor.test").error(message, *arguments)
    captured = capsys.readouterr()
    assert captured.err == ""
    entry = json.loads(captured.out)
    assert "[REDACTED" in entry["message"]
    for secret in (
        "private-api-key",
        "database-password",
        "rabbit/password",
        "rabbit%2Fpassword",
        "example.org",
        "private-hook",
        "webhook-secret",
        "unknown-password",
        "unknown-key",
        "another-key",
        "unknown-token",
        "private-customer",
        "private-payload",
        "private-body",
        "private-response",
        "private-error",
    ):
        assert secret not in captured.out


@pytest.mark.usefixtures("logging_state")
def test_payload_in_extras_and_stack_info_is_not_serialized(
    logging_settings: Settings, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging("api", logging_settings)
    logging.getLogger("payment_processor.test").info(
        "Payment operation completed",
        extra={"metadata": {"customer": "extra-secret"}, "sinfo": "source-secret"},
        stack_info=True,
    )
    captured = capsys.readouterr()
    assert json.loads(captured.out)["message"] == "Payment operation completed"
    assert "extra-secret" not in captured.out
    assert "source-secret" not in captured.out


@pytest.mark.usefixtures("logging_state")
def test_chained_exception_groups_keep_frames_without_messages_source_or_notes(
    logging_settings: Settings, capsys: pytest.CaptureFixture[str]
) -> None:
    configure_logging("rabbitmq-to-payment-worker", logging_settings)
    try:
        try:
            raise ValueError("opaque-sensitive-value")
        except ValueError as cause:
            cause.add_note("private-note")
            raise ExceptionGroup(
                "private-group", [cause, RuntimeError("private-child")]
            ) from cause
    except Exception:
        logging.getLogger("payment_processor.test").exception(
            "Payment operation failed"
        )
    captured = capsys.readouterr()
    entry = json.loads(captured.out)
    error = entry["exception"]
    assert error["type"] == "ExceptionGroup"
    assert error["cause"]["type"] == "ValueError"
    assert [child["type"] for child in error["exceptions"]] == [
        "ValueError",
        "RuntimeError",
    ]
    assert error["traceback"][0]["file"] == __file__
    assert error["traceback"][0]["function"] == (
        "test_chained_exception_groups_keep_frames_without_messages_source_or_notes"
    )
    assert error["traceback"][0]["line"] > 0
    for secret in (
        "opaque-sensitive-value",
        "private-note",
        "private-group",
        "private-child",
        "raise ExceptionGroup",
    ):
        assert secret not in captured.out


@pytest.mark.usefixtures("logging_state")
def test_dependency_logs_cannot_dump_payload_even_at_debug(
    logging_settings: Settings, capsys: pytest.CaptureFixture[str]
) -> None:
    logging_settings.log_level = "DEBUG"
    configure_logging("api", logging_settings)
    for name in (
        "httpx",
        "httpcore.http11",
        "aio_pika",
        "aiormq.channel",
        "sqlalchemy.engine.Engine",
    ):
        logger = logging.getLogger(name)
        logger.debug("transport-body-secret")
        logger.info("https://example.org/private-webhook")
        logger.error("unexpected-private-payload")
    captured = capsys.readouterr()
    entries = [json.loads(line) for line in captured.out.splitlines()]
    assert len(entries) == 5
    assert all(entry["message"] == "Dependency event" for entry in entries)
    assert "secret" not in captured.out
    assert "private" not in captured.out


@pytest.mark.usefixtures("logging_state")
@pytest.mark.parametrize("module", [publisher_main, consumer_main])
def test_worker_entrypoints_log_fatal_errors_without_raw_tracebacks(
    logging_settings: Settings,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
) -> None:
    async def fail() -> None:
        raise RuntimeError("opaque-worker-secret")

    monkeypatch.setattr(module, "settings", logging_settings)
    monkeypatch.setattr(
        module, "run_publisher" if module is publisher_main else "run_consumer", fail
    )
    with pytest.raises(SystemExit) as caught:
        module.main()
    assert caught.value.code == 1
    captured = capsys.readouterr()
    entry = json.loads(captured.out)
    assert entry["component"] == (
        "outbox-to-rabbitmq-worker"
        if module is publisher_main
        else "rabbitmq-to-payment-worker"
    )
    assert entry["exception"]["type"] == "RuntimeError"
    assert "opaque-worker-secret" not in captured.out
    assert captured.err == ""


@pytest.mark.asyncio
@pytest.mark.usefixtures("logging_state")
async def test_api_lifespan_errors_keep_safe_traceback(
    logging_settings: Settings,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(api_main, "settings", logging_settings)
    database = Mock(spec=Database)
    database.check_connection = AsyncMock(
        side_effect=RuntimeError("opaque-startup-secret")
    )
    application = api_main.create_app(database=database)
    with pytest.raises(RuntimeError):
        async with LifespanManager(application):
            pass
    entry = json.loads(capsys.readouterr().out)
    assert entry["message"] == "API lifespan failed"
    assert entry["exception"]["type"] == "RuntimeError"
    assert entry["exception"]["traceback"]
    assert "opaque-startup-secret" not in json.dumps(entry)
    database.dispose.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.usefixtures("logging_state")
async def test_uvicorn_startup_failure_does_not_print_starlette_raw_traceback(
    logging_settings: Settings,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(api_main, "settings", logging_settings)
    database = Mock(spec=Database)
    database.check_connection.side_effect = RuntimeError("opaque-lifespan-secret")
    application = api_main.create_app(database=database)
    lifespan = LifespanOn(Config(application, log_config=None))
    await lifespan.startup()
    captured = capsys.readouterr()
    entries = [json.loads(line) for line in captured.out.splitlines()]
    assert lifespan.should_exit
    assert any(
        entry.get("exception", {}).get("type") == "RuntimeError" for entry in entries
    )
    assert "opaque-lifespan-secret" not in captured.out
    assert captured.err == ""


@pytest.mark.asyncio
@pytest.mark.usefixtures("logging_state")
@pytest.mark.parametrize(
    "component", ["outbox-to-rabbitmq-worker", "rabbitmq-to-payment-worker"]
)
async def test_process_configures_faststream_logger_created_during_connection(
    logging_settings: Settings,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    component: str,
) -> None:
    configure_logging(component, logging_settings)
    broker = create_broker()
    monkeypatch.setattr(broker, "_connect", AsyncMock(return_value=Mock()))
    monkeypatch.setattr(broker, "stop", AsyncMock())
    database = Mock(spec=Database)
    error = RuntimeError("Stop test process")

    def log_and_stop(*args: object, **kwargs: object) -> None:
        logger = logging.getLogger("faststream.access.rabbit")
        logger.info("Received")
        try:
            raise ValueError("opaque-broker-payload")
        except ValueError:
            logger.exception("ValueError: opaque-broker-payload")
        raise error

    module = {
        "api": api_main,
        "outbox-to-rabbitmq-worker": publisher_main,
        "rabbitmq-to-payment-worker": consumer_main,
    }[component]
    monkeypatch.setattr(module, "settings", logging_settings)
    monkeypatch.setattr(module, "create_broker", lambda **kwargs: broker)
    monkeypatch.setattr(module, "Database", Mock(return_value=database))
    if component == "outbox-to-rabbitmq-worker":
        monkeypatch.setattr(publisher_main, "declare_payment_topology", AsyncMock())
        monkeypatch.setattr(
            publisher_main,
            "OutboxService",
            Mock(return_value=Mock(run=AsyncMock(side_effect=log_and_stop))),
        )
        with pytest.raises(RuntimeError) as caught:
            await publisher_main._publish()
    else:
        webhook = MagicMock()
        webhook.__aenter__ = AsyncMock(return_value=webhook)
        webhook.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr(consumer_main, "WebhookClient", Mock(return_value=webhook))
        monkeypatch.setattr(consumer_main, "declare_payment_topology", AsyncMock())
        monkeypatch.setattr(consumer_main, "declare_retry_topology", AsyncMock())
        monkeypatch.setattr(broker, "start", AsyncMock(side_effect=log_and_stop))
        with pytest.raises(RuntimeError) as caught:
            await consumer_main._consume()
    assert caught.value is error
    captured = capsys.readouterr()
    entries = [
        entry
        for line in captured.out.splitlines()
        if (entry := json.loads(line))["logger"] == "faststream.access.rabbit"
    ]
    assert len(entries) == 2
    assert all(entry["component"] == component for entry in entries)
    assert [entry["message"] for entry in entries] == [
        "Received",
        "Broker operation failed",
    ]
    assert entries[1]["exception"]["type"] == "ValueError"
    assert "opaque-broker-payload" not in captured.out
    assert captured.err == ""
    database.dispose.assert_awaited_once()
