import asyncio
import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from time import perf_counter
from uuid import UUID

from payment_processor.application.results import PaymentDetailsResult
from payment_processor.core.logging import processing_fields
from payment_processor.services.payment_leases import (
    EXTERNAL_CALL_TIMEOUT,
    payment_lease,
)
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
            async with payment_lease(
                self._store_scope_factory,
                payment_id,
                "webhook",
                lambda payment: payment.webhook_delivered_at is None,
            ) as (payment, token):
                outcome = "already_delivered"
                if token is not None:
                    snapshot = PaymentDetailsResult.from_payment(payment)
                    async with asyncio.timeout(EXTERNAL_CALL_TIMEOUT):
                        await sender.send(snapshot)
                    async with self._store_scope_factory() as repository:
                        await repository.verify_lease(payment_id, "webhook", token)
                        current = await repository.find_by_id_for_update(payment_id)
                        if current is None:
                            raise LookupError("Payment disappeared during delivery")
                        await repository.mark_webhook_delivered(current)
                        await repository.release_lease(payment_id, "webhook", token)
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
