from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import event, func, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapper, Session

from payment_processor.application.payment_events import build_payment_created_payload
from payment_processor.db.models import Outbox, Payment
from payment_processor.db.repositories.payments import SqlAlchemyPaymentRepository
from payment_processor.db.session import Database
from payment_processor.domain.payment import Payment as DomainPayment
from payment_processor.domain.payment import PaymentCurrency

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_outbox_insert_failure_rolls_back_payment_and_allows_retry(
    database: Database,
    client: AsyncClient,
    idempotency_key: str,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    payload = payment_payload.copy()
    with _fail_outbox_insert(idempotency_key) as failed_payment_ids:
        with pytest.raises(IntegrityError) as caught:
            await client.post("/api/v1/payments", headers=payment_headers, json=payload)

    assert getattr(caught.value.orig, "sqlstate", None) == "23514"
    assert (
        getattr(caught.value.orig.__cause__, "constraint_name", None)
        == "ck_outbox_attempts_non_negative"
    )
    assert len(failed_payment_ids) == 1
    async with database.session_factory() as session:
        payment_count = await session.scalar(
            select(func.count())
            .select_from(Payment)
            .where(Payment.idempotency_key == idempotency_key)
        )
        outbox_count = await session.scalar(
            select(func.count())
            .select_from(Outbox)
            .where(Outbox.payment_id == failed_payment_ids[0])
        )
        assert payment_count == 0
        assert outbox_count == 0

    response = await client.post(
        "/api/v1/payments", headers=payment_headers, json=payload
    )

    assert response.status_code == 202
    payment_id = UUID(response.json()["payment_id"])
    async with database.session_factory() as session:
        payments = (
            await session.scalars(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
        ).all()
        assert len(payments) == 1
        assert payments[0].id == payment_id
        outbox_count = await session.scalar(
            select(func.count())
            .select_from(Outbox)
            .where(Outbox.payment_id == payment_id)
        )
        assert outbox_count == 1


@contextmanager
def _fail_outbox_insert(idempotency_key: str) -> Iterator[list[UUID]]:
    failed_payment_ids: list[UUID] = []

    def invalidate_outbox(
        mapper: Mapper[Outbox], connection: Connection, outbox: Outbox
    ) -> None:
        payment_id = connection.scalar(
            select(Payment.id).where(
                Payment.id == outbox.payment_id,
                Payment.idempotency_key == idempotency_key,
            )
        )
        assert payment_id is not None
        failed_payment_ids.append(payment_id)
        outbox.attempts = -1

    event.listen(Outbox, "before_insert", invalidate_outbox)
    try:
        yield failed_payment_ids
    finally:
        event.remove(Outbox, "before_insert", invalidate_outbox)


@pytest.mark.asyncio
async def test_commit_failure_rolls_back_payment_and_outbox(
    outbox_database: Database,
) -> None:
    payment = DomainPayment(
        id=uuid4(),
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Commit failure",
        payment_metadata={},
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
    )
    error = RuntimeError("commit failed")

    def fail_commit(session: Session) -> None:
        raise error

    with pytest.raises(RuntimeError, match="commit failed") as caught:
        async with outbox_database.session_scope() as session:
            repository = SqlAlchemyPaymentRepository(session)
            await repository.save_with_outbox(
                payment, event_payload=build_payment_created_payload(payment)
            )
            await session.flush()
            assert await session.get(Payment, payment.id) is not None
            assert (
                await session.scalar(
                    select(Outbox).where(Outbox.payment_id == payment.id)
                )
                is not None
            )
            event.listen(session.sync_session, "before_commit", fail_commit, once=True)

    assert caught.value is error
    async with outbox_database.session_scope() as session:
        assert await session.get(Payment, payment.id) is None
        assert (
            await session.scalar(select(Outbox).where(Outbox.payment_id == payment.id))
            is None
        )
