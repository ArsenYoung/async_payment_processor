from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.models import Outbox, OutboxEventType, Payment
from payment_processor.messaging.schemas import PaymentCreatedPayload


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

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

    async def save_with_outbox(self, payment: Payment) -> Payment:
        self.session.add(payment)
        await self.session.flush()
        await self.session.refresh(payment)
        self.session.add(
            Outbox(
                payment_id=payment.id,
                event_type=OutboxEventType.PAYMENT_CREATED,
                payload=PaymentCreatedPayload.model_validate(
                    {
                        "payment_id": payment.id,
                        "amount": payment.amount,
                        "currency": payment.currency,
                        "description": payment.description,
                        "metadata": payment.payment_metadata,
                        "webhook_url": payment.webhook_url,
                    }
                ).model_dump(mode="json"),
            )
        )
        return payment


def is_idempotency_key_violation(error: IntegrityError) -> bool:
    original = error.orig
    postgres_error = original.__cause__ if original is not None else None
    return (
        getattr(original, "sqlstate", None) == "23505"
        and getattr(postgres_error, "constraint_name", None)
        == "uq_payments_idempotency_key"
    )
