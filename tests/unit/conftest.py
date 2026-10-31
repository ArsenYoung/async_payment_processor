import pytest

from .fakes import FakeGateway, FakeWebhookSender, MemoryPayments


@pytest.fixture
def memory_payments() -> MemoryPayments:
    return MemoryPayments()


@pytest.fixture
def fake_gateway(memory_payments: MemoryPayments) -> FakeGateway:
    return FakeGateway(memory_payments)


@pytest.fixture
def fake_sender(memory_payments: MemoryPayments) -> FakeWebhookSender:
    return FakeWebhookSender(memory_payments)
