from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import UUID

from payment_processor.application.errors import (
    IdempotencyKeyViolationError,
    LeaseLostError,
)
from payment_processor.application.ports import LeaseOperation, PaymentRepository
from payment_processor.application.records import PaymentInput
from payment_processor.application.results import PaymentDetailsResult
from payment_processor.domain.payment import Payment, PaymentStatus


class MemoryPayments:
    def __init__(self) -> None:
        self.payments: dict[UUID, Payment] = {}
        self.events: list[dict[str, object]] = []
        self.leases: dict[tuple[UUID, LeaseOperation], tuple[UUID, datetime]] = {}
        self.active_scopes = 0
        self.commits = 0
        self.fail_commit_at: int | None = None

    @asynccontextmanager
    async def scope(self) -> AsyncIterator[PaymentRepository]:
        payments, events, leases = deepcopy((self.payments, self.events, self.leases))
        self.active_scopes += 1
        try:
            yield MemoryPaymentRepository(self)
            if self.fail_commit_at == self.commits + 1:
                self.fail_commit_at = None
                raise RuntimeError("commit failed")
            self.commits += 1
        except BaseException:
            self.payments, self.events, self.leases = payments, events, leases
            raise
        finally:
            self.active_scopes -= 1


class MemoryPaymentRepository(PaymentRepository):
    def __init__(self, store: MemoryPayments) -> None:
        self.store = store
        self.payload: dict[str, object] | None = None

    async def find_by_id(self, payment_id: UUID) -> Payment | None:
        return deepcopy(self.store.payments.get(payment_id))

    async def find_by_id_for_update(self, payment_id: UUID) -> Payment | None:
        return await self.find_by_id(payment_id)

    async def find_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        return next(
            (
                deepcopy(p)
                for p in self.store.payments.values()
                if p.idempotency_key == idempotency_key
            ),
            None,
        )

    def set_event_payload(self, payload: dict[str, object]) -> None:
        self.payload = deepcopy(payload)

    async def save_with_outbox(self, payment: PaymentInput) -> Payment:
        if self.payload is None:
            raise ValueError("Event payload is required")
        if await self.find_by_idempotency_key(payment.idempotency_key) is not None:
            raise IdempotencyKeyViolationError
        saved = Payment(
            id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            payment_metadata=payment.payment_metadata,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
        )
        self.store.payments[saved.id] = deepcopy(saved)
        self.store.events.append(deepcopy(self.payload))
        return saved

    async def save(self, payment: Payment) -> None:
        if payment.id not in self.store.payments:
            raise ValueError("Cannot save a payment that does not exist")
        self.store.payments[payment.id] = deepcopy(payment)

    async def mark_webhook_delivered(self, payment: Payment) -> None:
        payment.mark_webhook_delivered()
        await self.save(payment)

    async def claim_lease(
        self, payment_id: UUID, operation: LeaseOperation, token: UUID, seconds: float
    ) -> bool:
        if seconds <= 0:
            raise ValueError("Lease duration must be positive")
        lease = self.store.leases.get((payment_id, operation))
        if payment_id not in self.store.payments or (
            lease is not None and lease[1] > datetime.now(UTC)
        ):
            return False
        self.store.leases[payment_id, operation] = (
            token,
            datetime.now(UTC) + timedelta(seconds=seconds),
        )
        return True

    async def verify_lease(
        self, payment_id: UUID, operation: LeaseOperation, token: UUID
    ) -> None:
        lease = self.store.leases.get((payment_id, operation))
        if lease is None or lease[0] != token or lease[1] <= datetime.now(UTC):
            raise LeaseLostError("Payment lease was lost")

    async def release_lease(
        self, payment_id: UUID, operation: LeaseOperation, token: UUID
    ) -> None:
        lease = self.store.leases.get((payment_id, operation))
        if lease is not None and lease[0] == token:
            del self.store.leases[payment_id, operation]


class FakeGateway:
    def __init__(self, store: MemoryPayments) -> None:
        self.store = store
        self.status = PaymentStatus.SUCCEEDED
        self.error: Exception | None = None
        self.calls: list[UUID] = []

    async def process(self, *, idempotency_key: UUID) -> PaymentStatus:
        assert self.store.active_scopes == 0
        self.calls.append(idempotency_key)
        if self.error is not None:
            raise self.error
        return self.status


class FakeWebhookSender:
    def __init__(self, store: MemoryPayments) -> None:
        self.store = store
        self.error: Exception | None = None
        self.calls: list[PaymentDetailsResult] = []

    async def send(self, payment: PaymentDetailsResult) -> None:
        assert self.store.active_scopes == 0
        self.calls.append(deepcopy(payment))
        if self.error is not None:
            raise self.error
