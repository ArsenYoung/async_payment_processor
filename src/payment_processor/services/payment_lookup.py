from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from uuid import UUID

from payment_processor.application.results import PaymentDetailsResult
from payment_processor.services.ports import PaymentReader


class GetPaymentUseCase:
    def __init__(
        self,
        store_scope_factory: Callable[[], AbstractAsyncContextManager[PaymentReader]],
    ) -> None:
        self._store_scope_factory = store_scope_factory

    async def execute(self, payment_id: UUID) -> PaymentDetailsResult | None:
        async with self._store_scope_factory() as repository:
            payment = await repository.find_by_id(payment_id)
            return (
                PaymentDetailsResult.from_payment(payment)
                if payment is not None
                else None
            )
