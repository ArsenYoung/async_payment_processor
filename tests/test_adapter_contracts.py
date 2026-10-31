from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import select

from payment_processor.application.records import NewPayment
from payment_processor.db.models import Outbox, Payment
from payment_processor.db.repositories.factories import payment_repository_scope
from payment_processor.db.session import Database
from payment_processor.domain.payment import PaymentCurrency, PaymentStatus
from payment_processor.services.payment_events import build_payment_created_payload


@pytest.mark.asyncio
async def test_payment_adapter_persists_plain_data_with_outbox(
    outbox_database: Database,
) -> None:
    draft = NewPayment(
        id=uuid4(),
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Adapter contract",
        payment_metadata={"items": [{"quantity": 1}]},
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
    )
    scope = payment_repository_scope(outbox_database.session_scope)

    async with scope() as repository:
        repository.set_event_payload(build_payment_created_payload(draft))
        saved = await repository.save_with_outbox(draft)
        assert saved.id == draft.id
        assert saved.status == PaymentStatus.PENDING
        assert saved.created_at is not None

    async with outbox_database.session_scope() as session:
        payment = await session.get(Payment, draft.id)
        event = await session.scalar(
            select(Outbox).where(Outbox.payment_id == draft.id)
        )
        assert payment is not None
        assert payment.payment_metadata == draft.payment_metadata
        assert event is not None
        assert event.payload == {
            "payment_id": str(draft.id),
            "amount": "10.50",
            "currency": "RUB",
            "description": draft.description,
            "metadata": draft.payment_metadata,
            "webhook_url": draft.webhook_url,
        }
