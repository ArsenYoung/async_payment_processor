import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from time import perf_counter
from uuid import UUID

from payment_processor.application.errors import PaymentNotFoundError
from payment_processor.application.results import PaymentDetailsResult
from payment_processor.core.logging import processing_fields
from payment_processor.services.ports import WebhookDeliveryStore, WebhookSender

logger = logging.getLogger(__name__)


class DeliverPaymentWebhookUseCase:
    def __init__(
        self,
        store_scope_factory: Callable[
            [], AbstractAsyncContextManager[WebhookDeliveryStore]
        ],
    ) -> None:
        self._store_scope_factory = store_scope_factory

    async def deliver(self, payment_id: UUID, sender: WebhookSender) -> None:
        started = perf_counter()
        fields = processing_fields(
            payment_id=str(payment_id), operation="webhook_delivery"
        )
        try:
            async with self._store_scope_factory() as repository:
                payment = await repository.find_by_id_for_update(payment_id)
                if payment is None:
                    raise PaymentNotFoundError(payment_id)
                outcome = "already_delivered"
                if payment.webhook_delivered_at is None:
                    await sender.send(PaymentDetailsResult.from_payment(payment))
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


WebhookDeliveryService = DeliverPaymentWebhookUseCase
