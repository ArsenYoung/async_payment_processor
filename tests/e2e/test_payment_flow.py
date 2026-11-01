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


def worker_metrics(stack, worker):
    return stack.compose(
        "exec",
        "--no-TTY",
        worker,
        "python",
        "-c",
        "import urllib.request; "
        "request = urllib.request.Request('http://127.0.0.1:9100/metrics', "
        "headers={'X-API-Key': 'e2e-api-key'}); "
        "print(urllib.request.urlopen(request, timeout=5).read().decode())",
    ).stdout


def metric_value(body, name, **labels):
    from prometheus_client.parser import text_string_to_metric_families

    return next(
        sample.value
        for family in text_string_to_metric_families(body)
        for sample in family.samples
        if sample.name == name and sample.labels == labels
    )


def test_runtime_metrics_and_dependency_readiness(docker_stack):
    stack = docker_stack
    stack.start_consumer("succeeded")
    publisher = "outbox-to-rabbitmq-worker"
    consumer = "rabbitmq-to-payment-worker"
    published = worker_metrics(stack, publisher)
    consumed = worker_metrics(stack, consumer)
    assert metric_value(published, "payment_collector_up", source="outbox") == 1
    assert (
        metric_value(
            published,
            "payment_operations_total",
            operation="publication",
            outcome="success",
        )
        >= 2
    )
    assert metric_value(consumed, "payment_collector_up", source="rabbitmq") == 1
    assert metric_value(consumed, "payment_dlq_messages") == 0
    assert "e2e-api-key" not in published + consumed

    def ready(worker):
        return (
            stack.compose(
                "exec",
                "--no-TTY",
                worker,
                "python",
                "-c",
                "from payment_processor.core.health import healthy; print(healthy())",
            ).stdout.strip()
            == "True"
        )

    def wait_workers(expected):
        deadline = time.monotonic() + 45
        while not all(ready(worker) == expected for worker in (publisher, consumer)):
            assert time.monotonic() < deadline, (
                f"Workers did not reach readiness {expected}"
            )
            time.sleep(0.5)

    with httpx.Client(
        base_url=stack.api_url,
        headers={"X-API-Key": "e2e-api-key"},
        trust_env=False,
        timeout=5,
    ) as api:
        wait_workers(True)
        assert api.get("/ready").status_code == 200
        stack.compose("stop", "postgres")
        try:
            wait_workers(False)
            assert api.get("/ready").status_code == 503
            assert api.get("/health").status_code == 200
        finally:
            stack.compose("up", "--detach", "--no-deps", "--wait", "postgres")
        wait_workers(True)
        assert api.get("/ready").status_code == 200
        stack.compose("stop", "rabbitmq")
        try:
            wait_workers(False)
            assert api.get("/ready").status_code == 200
        finally:
            stack.compose("up", "--detach", "--no-deps", "--wait", "rabbitmq")
        wait_workers(True)
        assert (
            metric_value(
                worker_metrics(stack, consumer),
                "payment_collector_up",
                source="rabbitmq",
            )
            == 1
        )
