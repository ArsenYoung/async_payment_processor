import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.models import Outbox, OutboxEventType, Payment
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.db.session import Database
from payment_processor.services.payment_service import PaymentService


@pytest.mark.asyncio
async def test_repeated_request_returns_same_payment(
    client: AsyncClient,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    first = await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=payment_payload,
    )
    second = await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=payment_payload,
    )

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]


@pytest.mark.asyncio
async def test_reused_key_with_different_payload_returns_conflict(
    client: AsyncClient,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=payment_payload,
    )
    different_payload = payment_payload.copy()
    different_payload["description"] = "Different payment"

    response = await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=different_payload,
    )

    assert response.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("original_metadata", "repeated_metadata"),
    [
        ({"flag": True}, {"flag": 1}),
        ({"nested": {"enabled": False}}, {"nested": {"enabled": 0}}),
    ],
)
async def test_reused_key_with_boolean_number_metadata_collision_returns_conflict(
    client: AsyncClient,
    payment_payload: dict,
    payment_headers: dict[str, str],
    original_metadata: dict,
    repeated_metadata: dict,
) -> None:
    original_payload = deepcopy(payment_payload)
    original_payload["metadata"] = original_metadata
    repeated_payload = deepcopy(original_payload)
    repeated_payload["metadata"] = repeated_metadata

    first = await client.post(
        "/api/v1/payments", headers=payment_headers, json=original_payload
    )
    response = await client.post(
        "/api/v1/payments", headers=payment_headers, json=repeated_payload
    )

    assert first.status_code == 202
    assert response.status_code == 409


@pytest.mark.asyncio
async def test_reused_key_metadata_json_order_rules(
    client: AsyncClient,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    original_payload = deepcopy(payment_payload)
    original_payload["metadata"] = {
        "outer": {"first": 1, "second": 2},
        "items": ["first", "second"],
    }
    reordered_keys_payload = deepcopy(original_payload)
    reordered_keys_payload["metadata"] = {
        "items": ["first", "second"],
        "outer": {"second": 2, "first": 1.0},
    }
    reordered_array_payload = deepcopy(original_payload)
    reordered_array_payload["metadata"]["items"] = ["second", "first"]

    first = await client.post(
        "/api/v1/payments", headers=payment_headers, json=original_payload
    )
    same_metadata = await client.post(
        "/api/v1/payments", headers=payment_headers, json=reordered_keys_payload
    )
    conflicting_metadata = await client.post(
        "/api/v1/payments", headers=payment_headers, json=reordered_array_payload
    )

    assert first.status_code == 202
    assert same_metadata.status_code == 202
    assert same_metadata.json() == first.json()
    assert conflicting_metadata.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("different_payload", [False, True], ids=["same", "different"])
async def test_concurrent_requests_preserve_idempotency(
    database: Database,
    client: AsyncClient,
    idempotency_key: str,
    override_payment_service: Callable[[PaymentService], None],
    different_payload: bool,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    tracking = _synchronize_payment_creation(database, override_payment_service)
    payloads = [payment_payload.copy(), payment_payload.copy()]
    if different_payload:
        payloads[1]["description"] = "Different payment"

    responses = await _post_concurrently(client, payment_headers, payloads)

    expected_statuses = [202, 409] if different_payload else [202, 202]
    assert sorted(response.status_code for response in responses) == expected_statuses
    if not different_payload:
        assert responses[0].json() == responses[1].json()
    winner = next(
        index for index, response in enumerate(responses) if response.status_code == 202
    )
    payment_id = UUID(responses[winner].json()["payment_id"])

    async with database.session_factory() as session:
        payments = (
            await session.scalars(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
        ).all()
        assert len(payments) == 1
        assert payments[0].id == payment_id
        assert payments[0].description == payloads[winner]["description"]
        outbox_entries = (
            await session.scalars(select(Outbox).where(Outbox.payment_id == payment_id))
        ).all()
        assert len(outbox_entries) == 1
        assert outbox_entries[0].event_type == OutboxEventType.PAYMENT_CREATED
        assert outbox_entries[0].payload == {
            "payment_id": str(payment_id),
            **payloads[winner],
        }

    assert len(tracking.initial_sessions) == 2
    assert tracking.initial_sessions[0] is not tracking.initial_sessions[1]
    assert len(tracking.transactions) == 3
    assert len(tracking.errors) == 1
    error = tracking.errors[0]
    assert getattr(error.orig, "sqlstate", None) == "23505"
    assert (
        getattr(error.orig.__cause__, "constraint_name", None)
        == "uq_payments_idempotency_key"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("different_payload", [False, True], ids=["same", "different"])
async def test_duplicate_key_error_is_recovered_after_rollback(
    database: Database,
    client: AsyncClient,
    idempotency_key: str,
    override_payment_service: Callable[[PaymentService], None],
    different_payload: bool,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    first = await client.post(
        "/api/v1/payments", headers=payment_headers, json=payment_payload
    )
    assert first.status_code == 202
    repositories = _miss_first_payment_lookup(database, override_payment_service)
    payload = payment_payload.copy()
    if different_payload:
        payload["description"] = "Different payment"

    response = await client.post(
        "/api/v1/payments", headers=payment_headers, json=payload
    )

    assert len(repositories) == 2
    if different_payload:
        assert response.status_code == 409
    else:
        assert response.status_code == 202
        assert response.json() == first.json()
    async with database.session_factory() as session:
        payment_count = await session.scalar(
            select(func.count())
            .select_from(Payment)
            .where(Payment.idempotency_key == idempotency_key)
        )
        outbox_count = await session.scalar(
            select(func.count())
            .select_from(Outbox)
            .where(Outbox.payment_id == UUID(first.json()["payment_id"]))
        )
        assert payment_count == 1
        assert outbox_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("violation", "sqlstate", "constraint_name"),
    [
        ("duplicate-outbox", "23505", "pk_outbox"),
        ("negative-attempts", "23514", "ck_outbox_attempts_non_negative"),
        ("missing-payment", "23503", "fk_outbox_payment_id_payments"),
    ],
)
async def test_unrelated_integrity_error_is_not_treated_as_duplicate_key(
    database: Database,
    client: AsyncClient,
    override_payment_service: Callable[[PaymentService], None],
    violation: str,
    sqlstate: str,
    constraint_name: str,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    first = await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=payment_payload,
    )
    assert first.status_code == 202
    payment_id = UUID(first.json()["payment_id"])
    async with database.session_factory() as session:
        outbox_id = await session.scalar(
            select(Outbox.id).where(Outbox.payment_id == payment_id)
        )
    assert outbox_id is not None

    repository_factory = _insert_invalid_outbox(
        database, override_payment_service, violation, payment_id, outbox_id
    )
    with pytest.raises(IntegrityError) as caught:
        await client.post(
            "/api/v1/payments",
            headers=payment_headers,
            json=payment_payload,
        )

    assert repository_factory.call_count == 1
    assert getattr(caught.value.orig, "sqlstate", None) == sqlstate
    assert (
        getattr(caught.value.orig.__cause__, "constraint_name", None) == constraint_name
    )


@pytest.mark.asyncio
async def test_duplicate_key_error_is_preserved_if_payment_cannot_be_found(
    database: Database,
    client: AsyncClient,
    override_payment_service: Callable[[PaymentService], None],
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    first = await client.post(
        "/api/v1/payments",
        headers=payment_headers,
        json=payment_payload,
    )
    assert first.status_code == 202
    repository_factory, errors = _hide_existing_payment(
        database, override_payment_service
    )

    with pytest.raises(IntegrityError) as caught:
        await client.post(
            "/api/v1/payments",
            headers=payment_headers,
            json=payment_payload,
        )

    assert repository_factory.call_count == 2
    assert len(errors) == 1
    assert caught.value is errors[0]
    assert getattr(caught.value.orig, "sqlstate", None) == "23505"
    assert (
        getattr(caught.value.orig.__cause__, "constraint_name", None)
        == "uq_payments_idempotency_key"
    )


@pytest.mark.asyncio
async def test_integrity_error_without_postgres_details_is_preserved(
    database: Database,
    client: AsyncClient,
    override_payment_service: Callable[[PaymentService], None],
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    error = IntegrityError(None, None, ValueError("Unexpected constraint error"))
    repository = Mock(spec=PaymentRepository)
    repository.find_by_idempotency_key = AsyncMock(side_effect=error)
    repository_factory = Mock(return_value=repository)
    override_payment_service(PaymentService(database.session_scope, repository_factory))

    with pytest.raises(IntegrityError) as caught:
        await client.post(
            "/api/v1/payments",
            headers=payment_headers,
            json=payment_payload,
        )

    assert caught.value is error
    assert repository_factory.call_count == 1
    repository.save_with_outbox.assert_not_awaited()


class MissingPaymentRepository(PaymentRepository):
    async def find_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        return None


@dataclass
class _ConcurrentTransactions:
    initial_sessions: list[AsyncSession] = field(default_factory=list)
    transactions: list[AsyncSession] = field(default_factory=list)
    errors: list[IntegrityError] = field(default_factory=list)


def _synchronize_payment_creation(
    database: Database,
    override_payment_service: Callable[[PaymentService], None],
) -> _ConcurrentTransactions:
    barrier = asyncio.Barrier(2)
    tracking = _ConcurrentTransactions()

    class SynchronizedRepository(PaymentRepository):
        async def find_by_idempotency_key(self, key: str) -> Payment | None:
            existing = await super().find_by_idempotency_key(key)
            if existing is None:
                tracking.initial_sessions.append(self.session)
                await barrier.wait()
            else:
                assert len(tracking.errors) == 1
                assert all(
                    self.session is not session for session in tracking.initial_sessions
                )
            return existing

    @asynccontextmanager
    async def tracked_session_scope() -> AsyncIterator[AsyncSession]:
        try:
            async with database.session_scope() as session:
                tracking.transactions.append(session)
                yield session
        except IntegrityError as error:
            assert not session.in_transaction()
            tracking.errors.append(error)
            raise

    override_payment_service(
        PaymentService(tracked_session_scope, SynchronizedRepository)
    )
    return tracking


async def _post_concurrently(
    client: AsyncClient,
    payment_headers: dict[str, str],
    payloads: list[dict],
) -> list[Response]:
    async with asyncio.timeout(10):
        async with asyncio.TaskGroup() as tasks:
            requests = [
                tasks.create_task(
                    client.post(
                        "/api/v1/payments", headers=payment_headers, json=payload
                    )
                )
                for payload in payloads
            ]
    return [request.result() for request in requests]


def _miss_first_payment_lookup(
    database: Database,
    override_payment_service: Callable[[PaymentService], None],
) -> list[PaymentRepository]:
    repositories: list[PaymentRepository] = []

    def repository_factory(session: AsyncSession) -> PaymentRepository:
        repository: PaymentRepository
        if not repositories:
            repository = MissingPaymentRepository(session)
        else:
            assert not repositories[0].session.in_transaction()
            assert session is not repositories[0].session
            repository = PaymentRepository(session)
        repositories.append(repository)
        return repository

    override_payment_service(PaymentService(database.session_scope, repository_factory))
    return repositories


def _insert_invalid_outbox(
    database: Database,
    override_payment_service: Callable[[PaymentService], None],
    violation: str,
    payment_id: UUID,
    outbox_id: UUID,
) -> Mock:
    class InvalidOutboxRepository(MissingPaymentRepository):
        async def save_with_outbox(self, payment: Payment) -> Payment:
            self.session.add(
                Outbox(
                    id=outbox_id if violation == "duplicate-outbox" else uuid4(),
                    payment_id=(
                        uuid4() if violation == "missing-payment" else payment_id
                    ),
                    event_type=OutboxEventType.PAYMENT_CREATED,
                    payload={},
                    attempts=-1 if violation == "negative-attempts" else 0,
                )
            )
            await self.session.flush()
            pytest.fail("The database must reject this constraint violation")

    repository_factory = Mock(side_effect=InvalidOutboxRepository)
    override_payment_service(PaymentService(database.session_scope, repository_factory))
    return repository_factory


def _hide_existing_payment(
    database: Database,
    override_payment_service: Callable[[PaymentService], None],
) -> tuple[Mock, list[IntegrityError]]:
    errors: list[IntegrityError] = []

    class UnfindablePaymentRepository(MissingPaymentRepository):
        async def save_with_outbox(self, payment: Payment) -> Payment:
            try:
                return await super().save_with_outbox(payment)
            except IntegrityError as error:
                errors.append(error)
                raise

    repository_factory = Mock(side_effect=UnfindablePaymentRepository)
    override_payment_service(PaymentService(database.session_scope, repository_factory))

    return repository_factory, errors
