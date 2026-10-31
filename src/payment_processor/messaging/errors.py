import httpx
from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    InterfaceError,
    OperationalError,
)
from sqlalchemy.exc import TimeoutError as DatabaseTimeoutError

from payment_processor.application.errors import PersistenceUnavailableError


def is_retryable_processing_error(error: BaseException) -> bool:
    if isinstance(
        error,
        (
            httpx.HTTPError,
            PersistenceUnavailableError,
            TimeoutError,
            ConnectionError,
            DatabaseTimeoutError,
            DisconnectionError,
        ),
    ):
        return True
    if isinstance(error, DBAPIError):
        if error.connection_invalidated:
            return True
        sqlstate = getattr(error.orig, "sqlstate", None)
        if isinstance(sqlstate, str):
            return sqlstate.startswith(("08", "40", "53")) or sqlstate in {
                "55P03",
                "57P01",
                "57P02",
                "57P03",
            }
        return isinstance(error, (OperationalError, InterfaceError))
    return False
