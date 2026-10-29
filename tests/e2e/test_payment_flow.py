import time
from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
import pytest


@pytest.mark.parametrize("status", ["succeeded", "failed"])
def test_payment_flow(payment_stack, status: str) -> None:
    key = f"e2e-{uuid4()}"
    headers = {"X-API-Key": "e2e-api-key", "Idempotency-Key": key}
    payload = {
        "amount": "10.50",
        "currency": "RUB",
        "description": f"E2E {status} payment",
        "metadata": {"order_id": key},
        "webhook_url": "http://webhook-receiver:8000/webhooks",
    }
    with (
        httpx.Client(
            base_url=payment_stack.api_url, headers=headers, timeout=10, trust_env=False
        ) as api,
        httpx.Client(
            base_url=payment_stack.receiver_url, timeout=10, trust_env=False
        ) as receiver,
    ):
        first = api.post("/api/v1/payments", json=payload)
        assert first.status_code == 202, first.text
        created = first.json()
        payment_id = created["payment_id"]
        UUID(payment_id)
        assert created["status"] == "pending"
        pending = api.get(f"/api/v1/payments/{payment_id}")
        assert pending.status_code == 200, pending.text
        assert pending.json()["status"] == "pending"
        assert pending.json()["processed_at"] is None

        repeated = api.post("/api/v1/payments", json=payload)
        assert repeated.status_code == 202, repeated.text
        assert repeated.json() == created

        payment_stack.start_consumer(status)
        deadline = time.monotonic() + 45
        while True:
            response = receiver.get(f"/deliveries/{payment_id}")
            assert response.status_code == 200, response.text
            deliveries = response.json()
            if deliveries:
                break
            assert time.monotonic() < deadline, "Timed out waiting for payment webhook"
            time.sleep(0.1)

        assert len(deliveries) == 1
        webhook = deliveries[0]
        result = api.get(f"/api/v1/payments/{payment_id}")
        assert result.status_code == 200, result.text
        payment = result.json()
        assert payment["payment_id"] == payment_id
        assert payment["status"] == status
        assert payment["created_at"] == created["created_at"]
        assert payment["idempotency_key"] == key
        assert payment["webhook_url"] == payload["webhook_url"]
        assert Decimal(payment["amount"]) == Decimal(payload["amount"])
        for name in ("currency", "description", "metadata"):
            assert payment[name] == payload[name]
        for name in (
            "payment_id",
            "status",
            "amount",
            "currency",
            "description",
            "metadata",
            "created_at",
            "processed_at",
        ):
            assert webhook[name] == payment[name]
        assert datetime.fromisoformat(
            payment["processed_at"]
        ) >= datetime.fromisoformat(payment["created_at"])

        repeated = api.post("/api/v1/payments", json=payload)
        assert repeated.status_code == 202, repeated.text
        assert repeated.json() == {
            "payment_id": payment_id,
            "status": status,
            "created_at": created["created_at"],
        }
        final = api.get(f"/api/v1/payments/{payment_id}")
        assert final.status_code == 200, final.text
        assert final.json() == payment
