from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.mappers.payment import to_domain, to_model, update_model
from payment_processor.db.models import Outbox, OutboxEventType
from payment_processor.db.models.payment import Payment as PaymentModel
from payment_processor.domain.payment import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self._event_payload: dict[str, object] | None = None

    def set_event_payload(self, payload: dict[str, object]) -> None:
        self._event_payload = payload

    async def find_by_id(self, payment_id: UUID) -> Payment | None:
        model = await self.session.get(PaymentModel, payment_id)
        return to_domain(model) if model is not None else None

    async def find_by_id_for_update(self, payment_id: UUID) -> Payment | None:
        result = await self.session.execute(
            select(PaymentModel).where(PaymentModel.id == payment_id).with_for_update()
        )
        model = result.scalar_one_or_none()
        return to_domain(model) if model is not None else None

    async def find_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self.session.execute(
            select(PaymentModel).where(PaymentModel.idempotency_key == idempotency_key)
        )
        model = result.scalar_one_or_none()
        return to_domain(model) if model is not None else None

    async def save(self, payment: Payment) -> None:
        model = await self.session.get(PaymentModel, payment.id)
        if model is None:
            raise ValueError("Cannot save a payment that does not exist")
        update_model(model, payment)
        await self.session.flush()

    async def mark_webhook_delivered(self, payment: Payment) -> None:
        payment.mark_webhook_delivered()
        await self.save(payment)

    async def save_with_outbox(self, payment: Payment) -> Payment:
        if self._event_payload is None:
            raise ValueError("Event payload is required")
        model = to_model(payment)
        self.session.add(model)
        await self.session.flush()
        await self.session.refresh(model)
        self.session.add(
            Outbox(
                payment_id=payment.id,
                event_type=OutboxEventType.PAYMENT_CREATED,
                payload=self._event_payload,
            )
        )
        return to_domain(model)


def is_idempotency_key_violation(error: IntegrityError) -> bool:
    original = error.orig
    postgres_error = original.__cause__ if original is not None else None
    return (
        getattr(original, "sqlstate", None) == "23505"
        and getattr(postgres_error, "constraint_name", None)
        == "uq_payments_idempotency_key"
    )
