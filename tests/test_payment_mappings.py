from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from payment_processor.db.mappers.payment import to_domain, to_model, update_model
from payment_processor.db.models.payment import Payment as PaymentModel
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.db.session import Database
from payment_processor.domain.payment import Payment, PaymentCurrency, PaymentStatus


def make_payment(status: PaymentStatus = PaymentStatus.PENDING) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("9999999999999999.99"),
        currency=PaymentCurrency.EUR,
        description="Mapper test",
        payment_metadata={"items": [{"quantity": 1}]},
        idempotency_key=f"mapper-{uuid4()}",
        webhook_url="https://example.com/webhook",
        created_at=datetime(2026, 10, 31, tzinfo=UTC),
        status=status,
        processed_at=(None if status == PaymentStatus.PENDING else datetime.now(UTC)),
        webhook_delivered_at=(
            None if status == PaymentStatus.PENDING else datetime.now(UTC)
        ),
    )


@pytest.mark.parametrize("status", list(PaymentStatus))
def test_mapping_preserves_all_payment_fields(status: PaymentStatus) -> None:
    payment = make_payment(status)

    model = to_model(payment)
    restored = to_domain(model)

    assert isinstance(model, PaymentModel)
    assert isinstance(restored, Payment)
    assert restored == payment
    assert restored is not payment


def test_metadata_does_not_share_nested_values_between_layers() -> None:
    payment = make_payment()
    model = to_model(payment)
    restored = to_domain(model)

    payment.payment_metadata["items"][0]["quantity"] = 2
    restored.payment_metadata["items"][0]["quantity"] = 3

    assert model.payment_metadata == {"items": [{"quantity": 1}]}
    assert payment.payment_metadata == {"items": [{"quantity": 2}]}
    assert restored.payment_metadata == {"items": [{"quantity": 3}]}


def test_mapping_rejects_updating_another_payment() -> None:
    payment = make_payment()
    model = to_model(payment)

    with pytest.raises(ValueError, match="identity"):
        update_model(model, make_payment())

    assert to_domain(model) == payment


@pytest.mark.asyncio
async def test_repository_requires_explicit_save_for_domain_changes(
    outbox_database: Database,
) -> None:
    payment = make_payment()
    async with outbox_database.session_scope() as session:
        session.add(to_model(payment))

    async with outbox_database.session_scope() as session:
        repository = PaymentRepository(session)
        loaded = await repository.find_by_id_for_update(payment.id)
        assert isinstance(loaded, Payment)
        loaded.process(PaymentStatus.SUCCEEDED)

    async with outbox_database.session_scope() as session:
        repository = PaymentRepository(session)
        unchanged = await repository.find_by_id(payment.id)
        assert unchanged is not None
        assert unchanged.status == PaymentStatus.PENDING
        await repository.save(loaded)

    async with outbox_database.session_scope() as session:
        restored = await PaymentRepository(session).find_by_id(payment.id)
        assert restored == loaded
