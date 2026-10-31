from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.application.ports import (
    PaymentRepository as PaymentRepositoryPort,
)
from payment_processor.application.records import PaymentInput
from payment_processor.db.mappers.payment import to_domain, to_model, update_model
from payment_processor.db.models import Outbox, OutboxEventType
from payment_processor.db.models.payment import Payment as PaymentModel
from payment_processor.domain.payment import Payment


class SqlAlchemyPaymentRepository(PaymentRepositoryPort):
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

    async def save_with_outbox(self, payment: PaymentInput) -> Payment:
        if self._event_payload is None:
            raise ValueError("Event payload is required")
        if not isinstance(payment, Payment):
            payment = Payment(
                id=payment.id,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                payment_metadata=payment.payment_metadata,
                idempotency_key=payment.idempotency_key,
                webhook_url=payment.webhook_url,
            )
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


PaymentRepository = SqlAlchemyPaymentRepository


def is_idempotency_key_violation(error: IntegrityError) -> bool:
    original = error.orig
    postgres_error = original.__cause__ if original is not None else None
    return (
        getattr(original, "sqlstate", None) == "23505"
        and getattr(postgres_error, "constraint_name", None)
        == "uq_payments_idempotency_key"
    )
