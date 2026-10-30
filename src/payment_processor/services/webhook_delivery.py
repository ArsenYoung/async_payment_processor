import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from time import perf_counter
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.core.logging import processing_fields
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.services.errors import PaymentNotFoundError
from payment_processor.services.payment_rules import can_deliver_webhook
from payment_processor.services.ports import WebhookSender

logger = logging.getLogger(__name__)


class WebhookDeliveryService:
    def __init__(
        self,
        session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        repository_factory: Callable[
            [AsyncSession], PaymentRepository
        ] = PaymentRepository,
    ) -> None:
        self._session_scope_factory = session_scope_factory
        self._repository_factory = repository_factory

    async def deliver(self, payment_id: UUID, sender: WebhookSender) -> None:
        started = perf_counter()
        fields = processing_fields(
            payment_id=str(payment_id), operation="webhook_delivery"
        )
        try:
            async with self._session_scope_factory() as session:
                repository = self._repository_factory(session)
                payment = await repository.find_by_id_for_update(payment_id)
                if payment is None:
                    raise PaymentNotFoundError(payment_id)
                outcome = "already_delivered"
                if payment.webhook_delivered_at is None:
                    if not can_deliver_webhook(payment.status, payment.processed_at):
                        raise ValueError(
                            "Webhook delivery requires a completed payment"
                        )
                    await sender.send(payment)
                    await repository.mark_webhook_delivered(payment)
                    outcome = "delivered"
        except Exception as error:
            logger.warning(
                "Webhook delivery failed",
                extra={
                    **fields,
                    "outcome": "failed",
                    "duration_ms": (perf_counter() - started) * 1000,
                    "error_type": type(error).__name__,
                },
                exc_info=True,
            )
            raise
        logger.info(
            "Webhook delivery completed",
            extra={
                **fields,
                "outcome": outcome,
                "duration_ms": (perf_counter() - started) * 1000,
            },
        )
