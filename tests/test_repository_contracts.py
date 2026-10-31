from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from decimal import Decimal
from uuid import uuid4

import pytest
from unit.fakes import MemoryPayments

from payment_processor.application.errors import LeaseLostError
from payment_processor.application.ports import PaymentRepository
from payment_processor.db.repositories.factories import payment_repository_scope
from payment_processor.domain.payment import Payment, PaymentCurrency, PaymentStatus
from payment_processor.services.payment_events import build_payment_created_payload

RepositoryScope = Callable[[], AbstractAsyncContextManager[PaymentRepository]]


@pytest.fixture(
    params=[
        pytest.param("memory", marks=pytest.mark.unit),
        pytest.param("sqlalchemy", marks=pytest.mark.integration),
    ]
)
def repository_scope(request: pytest.FixtureRequest) -> RepositoryScope:
    if request.param == "memory":
        return MemoryPayments().scope
    database = request.getfixturevalue("outbox_database")
    return payment_repository_scope(database.session_scope)


@pytest.fixture
def contract_payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("10.50"),
        currency=PaymentCurrency.RUB,
        description="Repository port contract",
        payment_metadata={"items": [{"quantity": 1}]},
        idempotency_key=str(uuid4()),
        webhook_url="https://example.com/webhook",
    )


async def seed(scope: RepositoryScope, payment: Payment) -> None:
    async with scope() as repository:
        repository.set_event_payload(build_payment_created_payload(payment))
        saved = await repository.save_with_outbox(payment)
        assert isinstance(saved, Payment)
        assert saved.id == payment.id


@pytest.mark.asyncio
async def test_repository_port_reads_detached_entities_and_saves_explicitly(
    repository_scope: RepositoryScope, contract_payment: Payment
) -> None:
    await seed(repository_scope, contract_payment)
    async with repository_scope() as repository:
        assert await repository.find_by_id(uuid4()) is None
        assert await repository.find_by_idempotency_key(str(uuid4())) is None
        found = await repository.find_by_idempotency_key(
            contract_payment.idempotency_key
        )
        assert isinstance(found, Payment)
        found.payment_metadata["items"][0]["quantity"] = 2
        found.process(PaymentStatus.FAILED)
    async with repository_scope() as repository:
        original = await repository.find_by_id_for_update(contract_payment.id)
        assert original is not None
        assert original.status == PaymentStatus.PENDING
        assert original.payment_metadata == {"items": [{"quantity": 1}]}
        await repository.save(found)
    async with repository_scope() as repository:
        saved = await repository.find_by_id(contract_payment.id)
        assert saved is not None
        assert saved.status == PaymentStatus.FAILED
        assert saved.processed_at is not None
        assert saved.payment_metadata == {"items": [{"quantity": 2}]}
        await repository.mark_webhook_delivered(saved)
    async with repository_scope() as repository:
        delivered = await repository.find_by_id(contract_payment.id)
        assert delivered is not None
        assert delivered.webhook_delivered_at is not None
        assert delivered.status == PaymentStatus.FAILED


@pytest.mark.asyncio
async def test_repository_port_rolls_back_creation_and_changes(
    repository_scope: RepositoryScope, contract_payment: Payment
) -> None:
    with pytest.raises(RuntimeError, match="abort creation"):
        async with repository_scope() as repository:
            repository.set_event_payload(
                build_payment_created_payload(contract_payment)
            )
            await repository.save_with_outbox(contract_payment)
            raise RuntimeError("abort creation")
    async with repository_scope() as repository:
        assert await repository.find_by_id(contract_payment.id) is None
    await seed(repository_scope, contract_payment)
    with pytest.raises(RuntimeError, match="abort update"):
        async with repository_scope() as repository:
            found = await repository.find_by_id_for_update(contract_payment.id)
            assert found is not None
            found.process(PaymentStatus.SUCCEEDED)
            await repository.save(found)
            await repository.mark_webhook_delivered(found)
            raise RuntimeError("abort update")
    async with repository_scope() as repository:
        saved = await repository.find_by_id(contract_payment.id)
        assert saved is not None
        assert saved.status == PaymentStatus.PENDING
        assert saved.processed_at is None
        assert saved.webhook_delivered_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["processing", "webhook"])
async def test_repository_port_preserves_lease_ownership(
    repository_scope: RepositoryScope, contract_payment: Payment, operation
) -> None:
    await seed(repository_scope, contract_payment)
    owner, other = uuid4(), uuid4()
    async with repository_scope() as repository:
        assert not await repository.claim_lease(uuid4(), operation, owner, 60)
        assert await repository.claim_lease(contract_payment.id, operation, owner, 60)
    async with repository_scope() as repository:
        assert not await repository.claim_lease(
            contract_payment.id, operation, other, 60
        )
        await repository.release_lease(contract_payment.id, operation, other)
        await repository.verify_lease(contract_payment.id, operation, owner)
    with pytest.raises(LeaseLostError):
        async with repository_scope() as repository:
            await repository.verify_lease(contract_payment.id, operation, other)
    async with repository_scope() as repository:
        await repository.release_lease(contract_payment.id, operation, owner)
        assert await repository.claim_lease(contract_payment.id, operation, other, 60)
