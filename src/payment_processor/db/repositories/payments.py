from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.application.ports import (
    PaymentRepository as PaymentRepositoryPort,
)
from payment_processor.application.records import PaymentInput, PaymentRecord
from payment_processor.db.models import Outbox, OutboxEventType, Payment
from payment_processor.services.payment_rules import can_deliver_webhook


class SqlAlchemyPaymentRepository(PaymentRepositoryPort):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self._event_payload: dict[str, object] | None = None

    def set_event_payload(self, payload: dict[str, object]) -> None:
        self._event_payload = payload

    async def find_by_id(self, payment_id: UUID) -> Payment | None:
        return await self.session.get(Payment, payment_id)

    async def find_by_id_for_update(self, payment_id: UUID) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        return result.scalar_one_or_none()

    async def find_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        result = await self.session.execute(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()

    async def mark_webhook_delivered(self, payment: PaymentRecord) -> None:
        if not can_deliver_webhook(payment.status, payment.processed_at):
            raise ValueError("Webhook delivery requires a completed payment")
        if payment.webhook_delivered_at is None:
            payment.webhook_delivered_at = datetime.now(UTC)
            await self.session.flush()

    async def save_with_outbox(self, payment: PaymentInput) -> PaymentRecord:
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
        self.session.add(payment)
        await self.session.flush()
        await self.session.refresh(payment)
        self.session.add(
            Outbox(
                payment_id=payment.id,
                event_type=OutboxEventType.PAYMENT_CREATED,
                payload=self._event_payload,
            )
        )
        return payment


PaymentRepository = SqlAlchemyPaymentRepository


def is_idempotency_key_violation(error: IntegrityError) -> bool:
    original = error.orig
    postgres_error = original.__cause__ if original is not None else None
    return (
        getattr(original, "sqlstate", None) == "23505"
        and getattr(postgres_error, "constraint_name", None)
        == "uq_payments_idempotency_key"
    )
