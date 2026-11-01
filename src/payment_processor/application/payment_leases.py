import asyncio
import logging
import random
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from uuid import UUID, uuid4

from payment_processor.application.errors import LeaseBusyError, PaymentNotFoundError
from payment_processor.application.ports import LeaseOperation, PaymentProcessingStore
from payment_processor.domain.payment import Payment

logger = logging.getLogger(__name__)
EXTERNAL_CALL_TIMEOUT = 30.0
LEASE_SECONDS = 60.0
LEASE_WAIT_SECONDS = 5.0
LEASE_POLL_INTERVAL = 0.25


@asynccontextmanager
async def payment_lease(
    scope: Callable[[], AbstractAsyncContextManager[PaymentProcessingStore]],
    payment_id: UUID,
    operation: LeaseOperation,
    needs_work: Callable[[Payment], bool],
) -> AsyncIterator[tuple[Payment, UUID | None]]:
    token = uuid4()
    claimed = False
    wait = asyncio.timeout(LEASE_WAIT_SECONDS)
    try:
        delay = LEASE_POLL_INTERVAL
        try:
            async with wait:
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
                    await asyncio.sleep(random.uniform(delay / 2, delay))
                    delay = min(1.0, delay * 2)
        except TimeoutError as error:
            if wait.expired():
                raise LeaseBusyError(
                    f"Payment {operation} lease wait expired"
                ) from error
            raise
        yield payment, token if claimed else None
    finally:
        if claimed:
            try:
                async with asyncio.timeout(2):
                    async with scope() as repository:
                        await repository.release_lease(payment_id, operation, token)
            except Exception:
                logger.exception("Payment lease cleanup failed; lease will expire")
