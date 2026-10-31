import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError

from payment_processor.db.models import Payment
from payment_processor.db.repositories.payments import is_idempotency_key_violation
from payment_processor.services.contracts import CreatePaymentCommand
from payment_processor.services.payment_events import build_payment_created_payload
from payment_processor.services.payment_rules import matches_create_command
from payment_processor.services.ports import PaymentCreationStore, PaymentReader

logger = logging.getLogger(__name__)


class IdempotencyConflictError(Exception):
    pass


class CreatePaymentUseCase:
    def __init__(
        self,
        store_scope_factory: Callable[
            [], AbstractAsyncContextManager[PaymentCreationStore]
        ],
    ) -> None:
        self._store_scope_factory = store_scope_factory

    async def create(
        self, command: CreatePaymentCommand
    ) -> Payment:
        try:
            return await self._create(command)
        except IntegrityError as error:
            return await self._recover(command, error)

    async def get(self, payment_id: UUID) -> Payment | None:
        async with self._store_scope_factory() as repository:
            reader: PaymentReader = repository
            return await reader.find_by_id(payment_id)

    async def _create(
        self, command: CreatePaymentCommand
    ) -> Payment:
        async with self._store_scope_factory() as repository:
            existing = await repository.find_by_idempotency_key(command.idempotency_key)
            if existing is not None:
                return self._reuse(existing, command)
            payment = Payment(
                id=uuid4(),
                amount=command.amount,
                currency=command.currency,
                description=command.description,
                payment_metadata=command.metadata,
                idempotency_key=command.idempotency_key,
                webhook_url=command.webhook_url,
            )
            repository.set_event_payload(build_payment_created_payload(payment))
            return await repository.save_with_outbox(payment)

    async def _recover(
        self, command: CreatePaymentCommand, error: IntegrityError
    ) -> Payment:
        if not is_idempotency_key_violation(error):
            raise error
        async with self._store_scope_factory() as repository:
            existing = await repository.find_by_idempotency_key(command.idempotency_key)
            if existing is None:
                raise error
            return self._reuse(existing, command)

    def _reuse(self, payment: Payment, command: CreatePaymentCommand) -> Payment:
        if not matches_create_command(payment, command):
            raise IdempotencyConflictError
        return payment


PaymentCreationService = CreatePaymentUseCase
