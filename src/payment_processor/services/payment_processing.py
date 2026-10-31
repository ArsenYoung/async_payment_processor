import asyncio
import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from time import perf_counter
from uuid import UUID

from payment_processor.application.errors import PaymentNotFoundError
from payment_processor.application.results import PaymentDetailsResult
from payment_processor.core.logging import processing_fields
from payment_processor.services.payment_leases import (
    EXTERNAL_CALL_TIMEOUT,
    payment_lease,
)
from payment_processor.services.ports import PaymentGateway, PaymentProcessingStore

logger = logging.getLogger(__name__)


class ProcessPaymentUseCase:
    def __init__(
        self,
        store_scope_factory: Callable[
            [], AbstractAsyncContextManager[PaymentProcessingStore]
        ],
        gateway: PaymentGateway,
    ) -> None:
        self._store_scope_factory = store_scope_factory
        self._gateway = gateway

    async def process(self, payment_id: UUID) -> PaymentDetailsResult:
        started = perf_counter()
        fields = processing_fields(
            payment_id=str(payment_id), operation="payment_process"
        )
        try:
            async with payment_lease(
                self._store_scope_factory,
                payment_id,
                "processing",
                lambda payment: payment.can_process,
            ) as (payment, token):
                outcome = "already_processed"
                if token is not None:
                    async with asyncio.timeout(EXTERNAL_CALL_TIMEOUT):
                        status = await self._gateway.process(idempotency_key=payment_id)
                    async with self._store_scope_factory() as repository:
                        await repository.verify_lease(payment_id, "processing", token)
                        payment = await repository.find_by_id_for_update(payment_id)
                        if payment is None:
                            raise PaymentNotFoundError(payment_id)
                        payment.process(status)
                        await repository.save(payment)
                        await repository.release_lease(payment_id, "processing", token)
                    outcome = "processed"
        except Exception as error:
            logger.warning(
                "Payment processing failed",
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
            "Payment processing completed",
            extra={
                **fields,
                "outcome": outcome,
                "payment_status": payment.status.value,
                "duration_ms": (perf_counter() - started) * 1000,
            },
        )
        return PaymentDetailsResult.from_payment(payment)


PaymentProcessingService = ProcessPaymentUseCase
