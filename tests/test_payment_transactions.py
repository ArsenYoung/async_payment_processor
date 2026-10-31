from collections.abc import Iterator
from contextlib import contextmanager
from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import event, func, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Mapper

from payment_processor.db.models import Outbox, Payment
from payment_processor.db.session import Database

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
