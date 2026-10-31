import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from uuid import UUID, uuid4

from payment_processor.application.errors import PaymentNotFoundError
from payment_processor.application.ports import LeaseOperation, PaymentProcessingStore
from payment_processor.domain.payment import Payment

logger = logging.getLogger(__name__)
EXTERNAL_CALL_TIMEOUT = 30.0
LEASE_SECONDS = 60.0


@asynccontextmanager
async def payment_lease(
    scope: Callable[[], AbstractAsyncContextManager[PaymentProcessingStore]],
    payment_id: UUID,
    operation: LeaseOperation,
    needs_work: Callable[[Payment], bool],
) -> AsyncIterator[tuple[Payment, UUID | None]]:
    token = uuid4()
    needed = False
    try:
        while True:
            async with scope() as repository:
                payment = await repository.find_by_id_for_update(payment_id)
                if payment is None:
                    raise PaymentNotFoundError(payment_id)
                needed = needs_work(payment)
                claimed = needed and await repository.claim_lease(
                    payment_id, operation, token, LEASE_SECONDS
                )
            if not needed or claimed:
                break
            await asyncio.sleep(0.05)
        yield payment, token if claimed else None
    finally:
        if needed:
            try:
                async with scope() as repository:
                    await repository.release_lease(payment_id, operation, token)
            except Exception:
                logger.exception("Payment lease cleanup failed; lease will expire")
