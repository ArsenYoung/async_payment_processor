from collections.abc import Callable
from contextlib import AbstractAsyncContextManager

from sqlalchemy.ext.asyncio import AsyncSession

from payment_processor.adapters.payment_gateway import PaymentGatewaySimulator
from payment_processor.application.ports import EventPublisher, PaymentGateway
from payment_processor.db.repositories.factories import (
    outbox_repository_scope,
    payment_repository_scope,
)
from payment_processor.db.repositories.outbox import OutboxRepository
from payment_processor.db.repositories.payments import PaymentRepository
from payment_processor.messaging import topology
from payment_processor.services.outbox_service import OutboxService
from payment_processor.services.payment_service import PaymentService


def create_payment_service(
    session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    repository_factory: Callable[[AsyncSession], PaymentRepository] = PaymentRepository,
    *,
    gateway: PaymentGateway | None = None,
) -> PaymentService:
    return PaymentService(
        payment_repository_scope(session_scope_factory, repository_factory),
        gateway=gateway if gateway is not None else PaymentGatewaySimulator(),
    )


def create_outbox_service(
    session_scope_factory: Callable[[], AbstractAsyncContextManager[AsyncSession]],
    publisher: EventPublisher,
    repository_factory: Callable[[AsyncSession], OutboxRepository] = OutboxRepository,
) -> OutboxService:
    return OutboxService(
        outbox_repository_scope(session_scope_factory, repository_factory),
        publisher,
        queue_name=topology.PAYMENTS_NEW_QUEUE.name,
    )
