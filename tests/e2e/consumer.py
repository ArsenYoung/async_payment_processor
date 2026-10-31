import os
from functools import partial
from unittest.mock import patch

from payment_processor.adapters.payment_gateway import PaymentGatewaySimulator
from payment_processor.bootstrap import create_payment_service
from payment_processor.rabbitmq_to_payment_worker import main as consumer


def main() -> None:
    status = os.environ["E2E_PAYMENT_STATUS"]
    if status not in {"succeeded", "failed"}:
        raise ValueError("E2E_PAYMENT_STATUS must be succeeded or failed")
    gateway = PaymentGatewaySimulator(
        min_delay=2.0,
        max_delay=2.0,
        success_rate=1.0 if status == "succeeded" else 0.0,
        random_value=lambda: 0.5,
    )
    with patch.object(
        consumer,
        "create_payment_service",
        partial(create_payment_service, gateway=gateway),
    ):
        consumer.main()


if __name__ == "__main__":
    main()
