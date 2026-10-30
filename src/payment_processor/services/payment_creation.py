import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any
from uuid import uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.db.models import Payment
from payment_processor.db.repositories.payments import (
    PaymentRepository,
    is_idempotency_key_violation,
)
from payment_processor.services.contracts import CreatePaymentCommand
from payment_processor.services.payment_events import build_payment_created_payload
from payment_processor.services.payment_rules import matches_create_command

logger = logging.getLogger(__name__)


class IdempotencyConflictError(Exception):
    pass


class PaymentCreationService:
    def __init__(
        self,
        session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
        repository_factory: Callable[
            [AsyncSession], PaymentRepository
        ] = PaymentRepository,
    ) -> None:
        self._session_scope_factory = session_scope_factory
        self._repository_factory = repository_factory

    async def create(
        self, command: CreatePaymentCommand, idempotency_key: str
    ) -> Payment:
        try:
            return await self._create(command, idempotency_key)
        except IntegrityError as error:
            return await self._recover(command, idempotency_key, error)

    async def get(self, payment_id: Any) -> Payment | None:
        async with self._session_scope_factory() as session:
            return await self._repository_factory(session).find_by_id(payment_id)

    async def _create(
        self, command: CreatePaymentCommand, idempotency_key: str
    ) -> Payment:
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            existing = await repository.find_by_idempotency_key(idempotency_key)
            if existing is not None:
                return self._reuse(existing, command)
            payment = Payment(
                id=uuid4(),
                amount=command.amount,
                currency=command.currency,
                description=command.description,
                payment_metadata=command.metadata,
                idempotency_key=idempotency_key,
                webhook_url=command.webhook_url,
            )
            repository.set_event_payload(build_payment_created_payload(payment))
            return await repository.save_with_outbox(payment)

    async def _recover(
        self, command: CreatePaymentCommand, idempotency_key: str, error: IntegrityError
    ) -> Payment:
        if not is_idempotency_key_violation(error):
            raise error
        async with self._session_scope_factory() as session:
            repository = self._repository_factory(session)
            existing = await repository.find_by_idempotency_key(idempotency_key)
            if existing is None:
                raise error
            return self._reuse(existing, command)

    def _reuse(self, payment: Payment, command: CreatePaymentCommand) -> Payment:
        if not matches_create_command(payment, command):
            raise IdempotencyConflictError
        return payment
