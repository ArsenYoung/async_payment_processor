from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from payment_processor.application.commands import CreatePaymentCommand
from payment_processor.application.results import PaymentDetailsResult
from payment_processor.db.models import Payment, PaymentCurrency, PaymentStatus
from payment_processor.services.payment_creation import CreatePaymentUseCase
from payment_processor.services.payment_lookup import GetPaymentUseCase
from payment_processor.services.payment_processing import ProcessPaymentUseCase
from payment_processor.services.webhook_delivery import DeliverPaymentWebhookUseCase


def store_scope(repository: object) -> object:
    @asynccontextmanager
    async def scope():
        yield repository

    return scope()


def payment(status: PaymentStatus = PaymentStatus.PENDING) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Application test payment",
        payment_metadata={"order_id": "application-test"},
        status=status,
        idempotency_key=f"application-{uuid4()}",
        webhook_url="https://example.com/webhook",
        created_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_create_use_case_saves_payment_and_event_payload() -> None:
    saved = payment()
    repository = AsyncMock()
    repository.set_event_payload = Mock()
    repository.find_by_idempotency_key.return_value = None
    repository.save_with_outbox.return_value = saved

    command = CreatePaymentCommand(
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Application test payment",
        metadata={"order_id": "application-test"},
        webhook_url="https://example.com/webhook",
        idempotency_key="application-key",
    )

    result = await CreatePaymentUseCase(
        lambda: store_scope(repository)
    ).create(command)

    assert result is saved
    repository.set_event_payload.assert_called_once()
    repository.save_with_outbox.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_use_case_reads_payment_by_id() -> None:
    saved = payment()
    repository = AsyncMock()
    repository.find_by_id.return_value = saved

    result = await GetPaymentUseCase(
        lambda: store_scope(repository)
    ).execute(saved.id)

    assert result is saved
    repository.find_by_id.assert_awaited_once_with(saved.id)


@pytest.mark.asyncio
async def test_process_use_case_calls_gateway_for_pending_payment() -> None:
    saved = payment()
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = saved
    gateway = AsyncMock()
    gateway.process.return_value = PaymentStatus.SUCCEEDED

    result = await ProcessPaymentUseCase(
        lambda: store_scope(repository), gateway
    ).process(saved.id)

    assert result is saved
    assert saved.status == PaymentStatus.SUCCEEDED
    gateway.process.assert_awaited_once()


@pytest.mark.asyncio
async def test_webhook_use_case_marks_delivery_after_sender_succeeds() -> None:
    saved = payment(PaymentStatus.SUCCEEDED)
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = saved
    sender = AsyncMock()

    await DeliverPaymentWebhookUseCase(
        lambda: store_scope(repository)
    ).deliver(saved.id, sender)

    sender.send.assert_awaited_once_with(saved)
    repository.mark_webhook_delivered.assert_awaited_once_with(saved)


def test_payment_details_result_maps_without_exposing_orm_to_api() -> None:
    saved = payment(PaymentStatus.SUCCEEDED)

    result = PaymentDetailsResult.from_payment(saved)

    assert result.payment_id == saved.id
    assert result.metadata == saved.payment_metadata
    assert result.status == PaymentStatus.SUCCEEDED
