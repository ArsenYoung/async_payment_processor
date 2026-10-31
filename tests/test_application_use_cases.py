from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from payment_processor.application.commands import CreatePaymentCommand
from payment_processor.application.results import (
    PaymentCreatedResult,
    PaymentDetailsResult,
)
from payment_processor.domain.payment import Payment, PaymentCurrency, PaymentStatus
from payment_processor.services.errors import PaymentNotFoundError
from payment_processor.services.payment_creation import (
    CreatePaymentUseCase,
    IdempotencyConflictError,
)
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
        processed_at=None if status == PaymentStatus.PENDING else datetime.now(UTC),
        webhook_delivered_at=None,
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

    result = await CreatePaymentUseCase(lambda: store_scope(repository)).create(command)

    assert result.payment_id == saved.id
    assert isinstance(result, PaymentCreatedResult)
    repository.set_event_payload.assert_called_once()
    repository.save_with_outbox.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_use_case_reads_payment_by_id() -> None:
    saved = payment()
    repository = AsyncMock()
    repository.find_by_id.return_value = saved

    result = await GetPaymentUseCase(lambda: store_scope(repository)).execute(saved.id)

    assert result.payment_id == saved.id
    assert isinstance(result, PaymentDetailsResult)
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

    assert result.payment_id == saved.id
    assert isinstance(result, PaymentDetailsResult)
    assert saved.status == PaymentStatus.SUCCEEDED
    gateway.process.assert_awaited_once()
    repository.save.assert_awaited_once_with(saved)


@pytest.mark.asyncio
async def test_webhook_use_case_marks_delivery_after_sender_succeeds() -> None:
    saved = payment(PaymentStatus.SUCCEEDED)
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = saved
    sender = AsyncMock()

    await DeliverPaymentWebhookUseCase(lambda: store_scope(repository)).deliver(
        saved.id, sender
    )

    sender.send.assert_awaited_once_with(PaymentDetailsResult.from_payment(saved))
    repository.mark_webhook_delivered.assert_awaited_once_with(saved)


def test_payment_details_result_maps_without_exposing_orm_to_api() -> None:
    saved = payment(PaymentStatus.SUCCEEDED)

    result = PaymentDetailsResult.from_payment(saved)

    assert result.payment_id == saved.id
    assert result.metadata == saved.payment_metadata
    assert result.status == PaymentStatus.SUCCEEDED


def command_for(saved: Payment) -> CreatePaymentCommand:
    return CreatePaymentCommand(
        amount=saved.amount,
        currency=saved.currency,
        description=saved.description,
        metadata=saved.payment_metadata,
        webhook_url=saved.webhook_url,
        idempotency_key=saved.idempotency_key,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_create_reuses_existing_payment_or_rejects_conflict(
    conflict: bool,
) -> None:
    saved = payment()
    command = command_for(saved)
    if conflict:
        saved.amount += Decimal("1")
    repository = AsyncMock()
    repository.find_by_idempotency_key.return_value = saved
    use_case = CreatePaymentUseCase(lambda: store_scope(repository))

    if conflict:
        with pytest.raises(IdempotencyConflictError):
            await use_case.create(command)
    else:
        result = await use_case.create(command)
        assert result == PaymentCreatedResult.from_payment(saved)
    repository.save_with_outbox.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_unknown_payment_returns_none() -> None:
    repository = AsyncMock()
    repository.find_by_id.return_value = None

    assert (
        await GetPaymentUseCase(lambda: store_scope(repository)).execute(uuid4())
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_process_completed_payment_preserves_result(
    status: PaymentStatus,
) -> None:
    saved = payment(status)
    expected = PaymentDetailsResult.from_payment(saved)
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = saved
    gateway = AsyncMock()

    result = await ProcessPaymentUseCase(
        lambda: store_scope(repository), gateway
    ).process(saved.id)

    assert result == expected
    gateway.process.assert_not_awaited()
    repository.save.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_gateway_failure_propagates_without_changing_payment() -> None:
    saved = payment()
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = saved
    gateway = AsyncMock()
    error = RuntimeError("gateway unavailable")
    gateway.process.side_effect = error

    with pytest.raises(RuntimeError) as caught:
        await ProcessPaymentUseCase(lambda: store_scope(repository), gateway).process(
            saved.id
        )

    assert caught.value is error
    assert saved.status == PaymentStatus.PENDING
    assert saved.processed_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["process", "webhook"])
async def test_missing_payment_does_not_call_external_dependency(
    operation: str,
) -> None:
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = None
    dependency = AsyncMock()
    payment_id = uuid4()

    with pytest.raises(PaymentNotFoundError):
        if operation == "process":
            await ProcessPaymentUseCase(
                lambda: store_scope(repository), dependency
            ).process(payment_id)
        else:
            await DeliverPaymentWebhookUseCase(lambda: store_scope(repository)).deliver(
                payment_id, dependency
            )

    dependency.process.assert_not_awaited()
    dependency.send.assert_not_awaited()
    repository.mark_webhook_delivered.assert_not_awaited()


@pytest.mark.asyncio
async def test_webhook_already_delivered_skips_sender() -> None:
    saved = payment(PaymentStatus.SUCCEEDED)
    saved.webhook_delivered_at = datetime.now(UTC)
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = saved
    sender = AsyncMock()

    await DeliverPaymentWebhookUseCase(lambda: store_scope(repository)).deliver(
        saved.id, sender
    )

    sender.send.assert_not_awaited()
    repository.mark_webhook_delivered.assert_not_awaited()


@pytest.mark.asyncio
async def test_webhook_failure_does_not_mark_delivery() -> None:
    saved = payment(PaymentStatus.FAILED)
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = saved
    sender = AsyncMock()
    error = RuntimeError("sender unavailable")
    sender.send.side_effect = error

    with pytest.raises(RuntimeError) as caught:
        await DeliverPaymentWebhookUseCase(lambda: store_scope(repository)).deliver(
            saved.id, sender
        )

    assert caught.value is error
    repository.mark_webhook_delivered.assert_not_awaited()


def test_details_result_is_a_snapshot_of_nested_metadata_and_state() -> None:
    saved = payment()
    saved.payment_metadata = {"items": [{"quantity": 1}]}

    result = PaymentDetailsResult.from_payment(saved)
    saved.payment_metadata["items"][0]["quantity"] = 2
    saved.status = PaymentStatus.FAILED

    assert result.metadata == {"items": [{"quantity": 1}]}
    assert result.status == PaymentStatus.PENDING
    result.metadata["items"].append({"quantity": 3})
    assert saved.payment_metadata == {"items": [{"quantity": 2}]}


@pytest.mark.asyncio
@pytest.mark.parametrize("unique_key", [False, True])
async def test_create_recovers_only_idempotency_constraint_violation(
    unique_key: bool,
) -> None:
    saved = payment()
    repository = AsyncMock()
    repository.set_event_payload = Mock()
    repository.find_by_idempotency_key.side_effect = [None, saved]
    cause = Exception("constraint violation")
    cause.constraint_name = "uq_payments_idempotency_key" if unique_key else "other"
    original = Exception("duplicate")
    original.sqlstate = "23505"
    original.__cause__ = cause
    error = IntegrityError("INSERT", {}, original)
    repository.save_with_outbox.side_effect = error
    use_case = CreatePaymentUseCase(lambda: store_scope(repository))

    if unique_key:
        result = await use_case.create(command_for(saved))
        assert result == PaymentCreatedResult.from_payment(saved)
        assert repository.find_by_idempotency_key.await_count == 2
    else:
        with pytest.raises(IntegrityError) as caught:
            await use_case.create(command_for(saved))
        assert caught.value is error
        assert repository.find_by_idempotency_key.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create", "process"])
async def test_transaction_failure_propagates_instead_of_returning_result(
    operation: str,
) -> None:
    saved = payment()
    repository = AsyncMock()
    repository.find_by_idempotency_key.return_value = saved
    repository.find_by_id_for_update.return_value = saved
    gateway = AsyncMock()
    gateway.process.return_value = PaymentStatus.SUCCEEDED
    error = RuntimeError("commit failed")

    @asynccontextmanager
    async def failing_scope():
        yield repository
        raise error

    with pytest.raises(RuntimeError) as caught:
        if operation == "create":
            await CreatePaymentUseCase(failing_scope).create(command_for(saved))
        else:
            await ProcessPaymentUseCase(failing_scope, gateway).process(saved.id)

    assert caught.value is error


@pytest.mark.asyncio
async def test_invalid_gateway_outcome_is_not_saved() -> None:
    saved = payment()
    repository = AsyncMock()
    repository.find_by_id_for_update.return_value = saved
    gateway = AsyncMock()
    gateway.process.return_value = PaymentStatus.PENDING

    with pytest.raises(ValueError):
        await ProcessPaymentUseCase(lambda: store_scope(repository), gateway).process(
            saved.id
        )

    assert saved.status == PaymentStatus.PENDING
    assert saved.processed_at is None
    repository.save.assert_not_awaited()
