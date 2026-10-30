from collections.abc import Awaitable
from typing import Protocol

from payment_processor.db.models import Payment, PaymentStatus


class PaymentGateway(Protocol):
    def process(self) -> Awaitable[PaymentStatus]: ...


class WebhookSender(Protocol):
    def send(self, payment: Payment) -> Awaitable[None]: ...
