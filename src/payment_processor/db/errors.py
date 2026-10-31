from sqlalchemy.exc import (
    DBAPIError,
    DisconnectionError,
    InterfaceError,
    OperationalError,
)
from sqlalchemy.exc import TimeoutError as DatabaseTimeoutError


def is_database_unavailable(error: BaseException) -> bool:
    if isinstance(error, (DisconnectionError, DatabaseTimeoutError)):
        return True
    return isinstance(error, DBAPIError) and (
        error.connection_invalidated
        or isinstance(error, (OperationalError, InterfaceError))
    )
