import os
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class DockerStack:
    project: str
    environment: dict[str, str]
    api_url: str = ""
    receiver_url: str = ""

    def compose(
        self, *arguments: str, timeout: float = 180
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "--file",
                str(ROOT / "docker-compose.test.yml"),
                "--project-name",
                self.project,
                *arguments,
            ],
            cwd=ROOT,
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(
                f"Compose {' '.join(arguments)} failed:\n"
                f"{result.stdout}\n{result.stderr}"
            )
        return result

    def url(self, service: str) -> str:
        address = self.compose("port", service, "8000").stdout.strip()
        return f"http://{address}"

    def start_consumer(self, status: str) -> None:
        self.environment["E2E_PAYMENT_STATUS"] = status
        self.compose(
            "up",
            "--detach",
            "--no-deps",
            "--force-recreate",
            "--wait",
            "rabbitmq-to-payment-worker",
        )


@pytest.fixture(scope="module")
def docker_stack() -> Iterator[DockerStack]:
    if os.environ.get("RUN_E2E") != "1":
        pytest.skip("Set RUN_E2E=1 to start the isolated Docker payment stack")
    stack = DockerStack(f"payment-e2e-{uuid4().hex[:12]}", dict(os.environ))
    stack.environment["COMPOSE_PROJECT_NAME"] = stack.project
    try:
        stack.compose(
            "up",
            "--detach",
            "--build",
            "--wait",
            "--wait-timeout",
            "120",
            "postgres",
            "rabbitmq",
            "migrate",
            "api",
            "outbox-to-rabbitmq-worker",
            "webhook-receiver",
            timeout=600,
        )
        stack.api_url = stack.url("api")
        stack.receiver_url = stack.url("webhook-receiver")
        yield stack
    except BaseException:
        print(stack.compose("logs", "--no-color", "--tail", "100").stdout)
        raise
    finally:
        stack.compose("down", "--volumes", "--remove-orphans", "--rmi", "local")


@pytest.fixture
def payment_stack(docker_stack: DockerStack) -> Iterator[DockerStack]:
    try:
        yield docker_stack
    finally:
        print(docker_stack.compose("logs", "--no-color", "--tail", "100").stdout)
        docker_stack.compose("stop", "rabbitmq-to-payment-worker")
