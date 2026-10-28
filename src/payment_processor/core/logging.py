from __future__ import annotations

import json
import logging
import re
import sys
import traceback
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlsplit

if TYPE_CHECKING:
    from payment_processor.core.config import Settings


_URL = re.compile(r"\b[a-z][a-z0-9+.-]*://[^\s<>\"']+", re.IGNORECASE)
_SENSITIVE_FIELD = re.compile(
    r"\b(?:[\w-]*key|password|passwd|secret|token|authorization|cookie|"
    r"headers|metadata|payload|(?:request_|response_)?body|webhook_url)"
    r"[\"']?\s*[:=]\s*.*",
    re.IGNORECASE | re.DOTALL,
)
_APPLICATION_LOGGERS = ("payment_processor", "uvicorn", "faststream")
_DEPENDENCY_LOGGERS = (
    "aio_pika",
    "aiormq",
    "asyncio",
    "asyncpg",
    "httpcore",
    "httpx",
    "sqlalchemy",
)


class ApplicationFormatter(logging.Formatter):
    def __init__(self, component: str, sensitive_values: Iterable[str] = ()) -> None:
        super().__init__()
        self.component = component
        secrets = set(sensitive_values)
        for value in tuple(secrets):
            if "://" in value:
                try:
                    password = urlsplit(value).password
                except ValueError:
                    continue
                if password:
                    secrets.update((password, unquote(password)))
        self._secrets = sorted(filter(None, secrets), key=len, reverse=True)

    def _redact(self, value: str) -> str:
        for secret in self._secrets:
            value = value.replace(secret, "[REDACTED]")
        value = _URL.sub("[REDACTED URL]", value)
        return _SENSITIVE_FIELD.sub("[REDACTED]", value)

    def _message(self, record: logging.LogRecord) -> str:
        if record.name == "uvicorn.access":
            if isinstance(record.args, tuple) and len(record.args) == 5:
                _, method, path, _, status = record.args
                path = str(path).split("?", 1)[0]
                return self._redact(f"{method} {path} {status}")
            return "HTTP request completed"
        if record.name.startswith("uvicorn"):
            if record.exc_info or record.levelno >= logging.ERROR:
                return "Server operation failed"
        elif record.name.startswith("faststream"):
            if record.exc_info or record.levelno >= logging.WARNING:
                return "Broker operation failed"
            return (
                record.msg
                if record.msg in ("Received", "Processed")
                else "Broker event"
            )
        elif not record.name.startswith("payment_processor"):
            return "Dependency event"
        if not isinstance(record.msg, str):
            return "[REDACTED]"
        if "Traceback (most recent call last)" in record.msg:
            return "[REDACTED exception details]"
        arguments: dict[str, object] | tuple[object, ...]
        if isinstance(record.args, dict):
            if "%(" in record.msg:
                arguments = {
                    key: self._argument(value) for key, value in record.args.items()
                }
            else:
                arguments = ("[REDACTED]",)
        else:
            arguments = tuple(self._argument(value) for value in (record.args or ()))
        try:
            message = record.msg % arguments if arguments else record.msg
        except (TypeError, ValueError, KeyError, OverflowError):
            return "[REDACTED invalid log message]"
        return self._redact(message)

    def _argument(self, value: object) -> object:
        if isinstance(value, str):
            return self._redact(value)
        if value is None or isinstance(value, (bool, int, float)):
            return value
        return "[REDACTED]"

    def _exception(self, error: BaseException, seen: set[int]) -> dict[str, object]:
        if id(error) in seen:
            return {"type": type(error).__name__}
        seen.add(id(error))
        result: dict[str, object] = {
            "type": type(error).__name__,
            "traceback": [
                {
                    "file": self._redact(frame.f_code.co_filename),
                    "line": line,
                    "function": frame.f_code.co_name,
                }
                for frame, line in traceback.walk_tb(error.__traceback__)
            ],
        }
        if error.__cause__ is not None:
            result["cause"] = self._exception(error.__cause__, seen)
        elif error.__context__ is not None and not error.__suppress_context__:
            result["context"] = self._exception(error.__context__, seen)
        if isinstance(error, BaseExceptionGroup):
            result["exceptions"] = [
                self._exception(child, seen) for child in error.exceptions
            ]
        return result

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "component": self.component,
            "logger": self._redact(record.name),
            "message": self._message(record),
        }
        if record.exc_info and record.exc_info[1] is not None:
            entry["exception"] = self._exception(record.exc_info[1], set())
        return json.dumps(entry, ensure_ascii=True)


class ConsoleHandler(logging.StreamHandler):
    pass


def configure_logging(component: str, settings: Settings) -> None:
    formatter = ApplicationFormatter(
        component,
        (settings.api_key, settings.database_url, settings.rabbitmq_url),
    )
    root = logging.getLogger()
    handler = next(
        (handler for handler in root.handlers if isinstance(handler, ConsoleHandler)),
        None,
    )
    if handler is None:
        handler = ConsoleHandler(sys.stdout)
        root.handlers = [handler]
    handler.setFormatter(formatter)
    handler.setLevel(settings.log_level)
    root.setLevel(settings.log_level)
    configure_library_logging()


def configure_library_logging() -> None:
    root = logging.getLogger()
    for namespace in (*_APPLICATION_LOGGERS, *_DEPENDENCY_LOGGERS):
        names = [namespace] + [
            name for name in root.manager.loggerDict if name.startswith(namespace + ".")
        ]
        for name in names:
            logger = logging.getLogger(name)
            logger.handlers.clear()
            logger.setLevel(logging.NOTSET)
            logger.propagate = True
            logger.disabled = False
        if namespace in _DEPENDENCY_LOGGERS:
            logging.getLogger(namespace).setLevel(max(logging.WARNING, root.level))
