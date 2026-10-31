from uuid import UUID


class IdempotencyKeyViolationError(Exception):
    pass


class PersistenceUnavailableError(Exception):
    pass


class PaymentNotFoundError(LookupError):
    def __init__(self, payment_id: UUID) -> None:
        self.payment_id = payment_id
        super().__init__(f"Payment {payment_id} not found")
