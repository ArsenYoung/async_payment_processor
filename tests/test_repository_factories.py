from contextlib import asynccontextmanager
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError

from payment_processor.application.errors import (
    IdempotencyKeyViolationError,
    PersistenceUnavailableError,
)
from payment_processor.db.repositories.factories import (
    outbox_repository_scope,
    payment_repository_scope,
)


def integrity_error(sqlstate: str | None, constraint: str | None) -> IntegrityError:
    cause = Exception("constraint violation")
    if constraint is not None:
        cause.constraint_name = constraint
    original = Exception("database error")
    if sqlstate is not None:
        original.sqlstate = sqlstate
    original.__cause__ = cause
    return IntegrityError("INSERT", {}, original)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["flush", "commit"])
@pytest.mark.parametrize(
    ("sqlstate", "constraint", "translated"),
    [
        ("23505", "uq_payments_idempotency_key", True),
        ("23505", "pk_outbox", False),
        ("23514", "uq_payments_idempotency_key", False),
        ("23503", "fk_outbox_payment_id_payments", False),
        (None, None, False),
    ],
)
async def test_scope_translates_only_idempotency_violation_after_transaction_exit(
    stage: str, sqlstate: str | None, constraint: str | None, translated: bool
) -> None:
    error = integrity_error(sqlstate, constraint)
    exited = False
    session = object()
    repository = object()
    repository_factory = Mock(return_value=repository)

    @asynccontextmanager
    async def session_scope():
        nonlocal exited
        try:
            yield session
            if stage == "commit":
                raise error
        finally:
            exited = True

    scope = payment_repository_scope(session_scope, repository_factory)

    with pytest.raises(
        IdempotencyKeyViolationError if translated else IntegrityError
    ) as caught:
        async with scope() as store:
            assert store is repository
            if stage == "flush":
                raise error

    assert exited
    repository_factory.assert_called_once_with(session)
    if translated:
        assert caught.value.__cause__ is error
    else:
        assert caught.value is error


@pytest.mark.asyncio
async def test_scope_preserves_non_integrity_error() -> None:
    error = RuntimeError("database unavailable")

    @asynccontextmanager
    async def session_scope():
        yield object()
        raise error

    scope = payment_repository_scope(session_scope, Mock())

    with pytest.raises(RuntimeError) as caught:
        async with scope():
            pass

    assert caught.value is error


@pytest.mark.asyncio
async def test_scope_commits_without_error() -> None:
    committed = False
    repository = object()

    @asynccontextmanager
    async def session_scope():
        nonlocal committed
        yield object()
        committed = True

    scope = payment_repository_scope(session_scope, Mock(return_value=repository))

    async with scope() as store:
        assert store is repository
        assert not committed

    assert committed


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["query", "commit"])
@pytest.mark.parametrize("unavailable", [False, True])
async def test_outbox_scope_translates_only_temporary_database_errors(
    stage: str, unavailable: bool
) -> None:
    error = (OperationalError if unavailable else ProgrammingError)(
        "SELECT", {}, RuntimeError("database error")
    )
    exited = False

    @asynccontextmanager
    async def session_scope():
        nonlocal exited
        try:
            yield object()
            if stage == "commit":
                raise error
        finally:
            exited = True

    scope = outbox_repository_scope(session_scope, Mock())

    with pytest.raises(
        PersistenceUnavailableError if unavailable else ProgrammingError
    ) as caught:
        async with scope():
            if stage == "query":
                raise error

    assert exited
    if unavailable:
        assert caught.value.__cause__ is error
    else:
        assert caught.value is error
