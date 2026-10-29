import json
import os
import subprocess
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[2]
PUBLISHER = "outbox-to-rabbitmq-worker"
CONSUMER = "rabbitmq-to-payment-worker"
API_KEY = "persistence-api-key"


@dataclass
class PersistenceStack:
    project: str
    compose_file: Path
    environment: dict[str, str] = field(repr=False)

    def docker(
        self, *arguments: str, timeout: float = 180, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            ["docker", *arguments],
            cwd=ROOT,
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if check and result.returncode:
            raise RuntimeError(
                f"Docker {' '.join(arguments)} failed:\n"
                f"{result.stdout}\n{result.stderr}"
            )
        return result

    def compose(
        self, *arguments: str, timeout: float = 180
    ) -> subprocess.CompletedProcess[str]:
        return self.docker(
            "compose",
            "--file",
            str(self.compose_file),
            "--env-file",
            os.devnull,
            "--project-name",
            self.project,
            *arguments,
            timeout=timeout,
        )

    def start(self, *services: str) -> None:
        self.compose("up", "--detach", "--wait", "--wait-timeout", "120", *services)
        if "rabbitmq" in services:
            self.compose(
                "exec",
                "--no-TTY",
                "rabbitmq",
                "rabbitmqctl",
                "await_startup",
                "--timeout",
                "120",
            )

    def sql(self, query: str) -> str:
        return self.compose(
            "exec",
            "--no-TTY",
            "postgres",
            "psql",
            "--username=persistence",
            "--dbname=payments_persistence",
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "--set=ON_ERROR_STOP=1",
            "--command",
            query,
        ).stdout.strip()

    def url(self, service: str) -> str:
        address = self.compose("port", service, "8000").stdout.strip()
        return f"http://{address}"

    def payment_state(self, payment_id: UUID) -> dict[str, Any]:
        return json.loads(
            self.sql(
                "SELECT json_build_object("
                "'status', p.status, 'processed_at', p.processed_at, "
                "'delivered_at', p.webhook_delivered_at, 'event_id', o.id, "
                "'published_at', o.published_at, 'payload', o.payload) "
                "FROM payments p JOIN outbox o ON o.payment_id = p.id "
                f"WHERE p.id = '{payment_id}'"
            )
        )

    def queue(self) -> dict[str, Any]:
        queues = json.loads(
            self.compose(
                "exec",
                "--no-TTY",
                "rabbitmq",
                "rabbitmqctl",
                "list_queues",
                "--formatter=json",
                "name",
                "messages_ready",
                "messages_unacknowledged",
                "consumers",
                "durable",
            ).stdout
        )
        return next(queue for queue in queues if queue["name"] == "payments.new")


def wait_for(check: Callable[[], bool], description: str) -> None:
    deadline = time.monotonic() + 45
    while not check():
        assert time.monotonic() < deadline, f"Timed out waiting for {description}"
        time.sleep(0.2)


@pytest.fixture
def persistence_stack(tmp_path: Path) -> Iterator[PersistenceStack]:
    if os.environ.get("RUN_DOCKER_SMOKE") != "1":
        pytest.skip("Set RUN_DOCKER_SMOKE=1 to run isolated Docker persistence tests")

    project = f"payment-persistence-{uuid4().hex[:12]}"
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("COMPOSE_")
    }
    environment.update(
        POSTGRES_DB="payments_persistence",
        POSTGRES_USER="persistence",
        POSTGRES_PASSWORD="persistence-password",
        RABBITMQ_USER="persistence",
        RABBITMQ_PASSWORD="persistence-password",
        API_KEY=API_KEY,
        OUTBOX_BATCH_SIZE="100",
        OUTBOX_POLL_INTERVAL="0.1",
        OUTBOX_PUBLISH_TIMEOUT="5",
        WEBHOOK_TIMEOUT="5",
    )
    stack = PersistenceStack(project, ROOT / "docker-compose.yml", environment)
    config = json.loads(stack.compose("config", "--format=json").stdout)
    for service in config["services"].values():
        if "ports" in service:
            service["ports"] = [
                f"127.0.0.1::{port['target']}" for port in service["ports"]
            ]
        if "build" in service:
            service["image"] = f"{project}-app:local"
    config["services"]["webhook-receiver"] = {
        "image": f"{project}-app:local",
        "command": [
            "uvicorn",
            "webhook_receiver:app",
            "--app-dir",
            "/app/smoke",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
        ],
        "volumes": [f"{ROOT / 'tests/smoke'}:/app/smoke:ro"],
        "ports": ["127.0.0.1::8000"],
        "healthcheck": {
            "test": [
                "CMD",
                "python",
                "-c",
                "import urllib.request; "
                "urllib.request.urlopen('http://localhost:8000/health', timeout=3)"
                ".close()",
            ],
            "interval": "1s",
            "timeout": "5s",
            "retries": 30,
        },
    }
    stack.compose_file = tmp_path / "compose.json"
    stack.compose_file.write_text(json.dumps(config), encoding="utf-8")
    try:
        stack.compose("build", "api", timeout=600)
        stack.start("postgres", "rabbitmq")
        stack.compose(
            "run",
            "--rm",
            "--no-deps",
            "--volume",
            f"{ROOT / 'alembic.ini'}:/app/alembic.ini:ro",
            "--volume",
            f"{ROOT / 'alembic'}:/app/alembic:ro",
            "api",
            "alembic",
            "upgrade",
            "head",
        )
        stack.start("api", "webhook-receiver")
        yield stack
    finally:
        try:
            print(stack.compose("logs", "--no-color", "--tail", "80").stdout)
        finally:
            stack.compose("down", "--volumes", "--remove-orphans")
            image = f"{project}-app:local"
            if stack.docker("image", "inspect", image, check=False).returncode == 0:
                stack.docker("image", "rm", image)


@pytest.mark.parametrize("published", [True, False], ids=["queued", "outbox"])
def test_payment_recovers_after_down_up(
    persistence_stack: PersistenceStack, published: bool
) -> None:
    stack = persistence_stack
    with httpx.Client(
        base_url=stack.url("api"),
        headers={"X-API-Key": API_KEY},
        trust_env=False,
        timeout=5,
    ) as api:
        response = api.post(
            "/api/v1/payments",
            headers={"Idempotency-Key": f"persistence-{uuid4()}"},
            json={
                "amount": "10.50",
                "currency": "RUB",
                "description": "Payment surviving container recreation",
                "metadata": {"case": "queued" if published else "outbox"},
                "webhook_url": "http://webhook-receiver:8000/webhook",
            },
        )
        assert response.status_code == 202, response.text
        payment_id = UUID(response.json()["payment_id"])
        response = api.get(f"/api/v1/payments/{payment_id}")
        assert response.status_code == 200, response.text
        original_payment = response.json()
        assert original_payment["status"] == "pending"

    if published:
        stack.start(PUBLISHER)
        wait_for(
            lambda: stack.payment_state(payment_id)["published_at"] is not None,
            "publication before down",
        )
        queue = stack.queue()
        assert queue["durable"] is True
        assert queue["messages_ready"] == 1
        assert queue["messages_unacknowledged"] == queue["consumers"] == 0

    original_state = stack.payment_state(payment_id)
    assert original_state["status"] == "pending"
    assert original_state["delivered_at"] is None
    assert (original_state["published_at"] is not None) == published
    container_ids = {
        service: stack.compose("ps", "--quiet", service).stdout.strip()
        for service in ("postgres", "rabbitmq")
    }

    stack.compose("down")
    stack.start("postgres", "rabbitmq")
    for service, previous_id in container_ids.items():
        assert stack.compose("ps", "--quiet", service).stdout.strip() != previous_id
    assert stack.payment_state(payment_id) == original_state
    if published:
        queue = stack.queue()
        assert queue["durable"] is True
        assert queue["messages_ready"] == 1
        assert queue["messages_unacknowledged"] == queue["consumers"] == 0

    stack.start("api", "webhook-receiver")
    with httpx.Client(
        base_url=stack.url("api"),
        headers={"X-API-Key": API_KEY},
        trust_env=False,
        timeout=5,
    ) as api:
        response = api.get(f"/api/v1/payments/{payment_id}")
        assert response.status_code == 200, response.text
        assert response.json() == original_payment

        stack.start(PUBLISHER)
        wait_for(
            lambda: stack.payment_state(payment_id)["published_at"] is not None,
            "publication after up",
        )
        stack.start(CONSUMER)
        wait_for(
            lambda: stack.payment_state(payment_id)["delivered_at"] is not None,
            "payment completion and webhook delivery",
        )
        state = stack.payment_state(payment_id)
        assert state["event_id"] == original_state["event_id"]
        assert state["payload"] == original_state["payload"]
        assert state["status"] in {"succeeded", "failed"}
        assert state["processed_at"] is not None
        response = api.get(f"/api/v1/payments/{payment_id}")
        assert response.status_code == 200, response.text
        payment = response.json()

    response = httpx.get(
        f"{stack.url('webhook-receiver')}/notifications", trust_env=False, timeout=5
    )
    assert response.status_code == 200, response.text
    notifications = response.json()
    assert len(notifications) == 1
    notification = notifications[0]
    for key in (
        "payment_id",
        "status",
        "amount",
        "currency",
        "description",
        "metadata",
        "created_at",
        "processed_at",
    ):
        assert notification[key] == payment[key]

    def queue_drained() -> bool:
        queue = stack.queue()
        return queue["messages_ready"] == queue["messages_unacknowledged"] == 0

    wait_for(queue_drained, "queue draining and acknowledgement")
