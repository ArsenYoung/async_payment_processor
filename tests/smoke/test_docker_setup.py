import json
import os
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKERS = {
    "outbox-to-rabbitmq-worker": "outbox-to-rabbitmq-worker",
    "rabbitmq-to-payment-worker-smoke": "rabbitmq-to-payment-worker",
}


@dataclass
class SmokeStack:
    project: str
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
            str(ROOT / "docker-compose.test.yml"),
            "--env-file",
            os.devnull,
            "--project-name",
            self.project,
            "--profile",
            "smoke",
            *arguments,
            timeout=timeout,
        )

    def sql(self, query: str) -> str:
        return self.compose(
            "exec",
            "--no-TTY",
            "postgres",
            "psql",
            "--username=e2e",
            "--dbname=payments_e2e",
            "--no-psqlrc",
            "--tuples-only",
            "--no-align",
            "--set=ON_ERROR_STOP=1",
            "--command",
            query,
        ).stdout.strip()

    def schema(self) -> dict[str, list[str]]:
        return json.loads(
            self.sql(
                "SELECT COALESCE(json_object_agg(table_name, columns), '{}'::json) "
                "FROM (SELECT table_name, "
                "array_agg(column_name ORDER BY ordinal_position) AS columns "
                "FROM information_schema.columns WHERE table_schema = 'public' "
                "GROUP BY table_name) AS tables"
            )
        )

    def migrate(self, *arguments: str) -> str:
        return self.compose(
            "run", "--rm", "--no-deps", "migrate", "alembic", *arguments
        ).stdout.strip()


@pytest.fixture
def smoke_stack() -> Iterator[SmokeStack]:
    if os.environ.get("RUN_DOCKER_SMOKE") != "1":
        pytest.skip("Set RUN_DOCKER_SMOKE=1 to start the isolated Docker smoke stack")
    project = f"payment-smoke-{uuid4().hex[:12]}"
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("COMPOSE_")
    }
    environment["COMPOSE_PROJECT_NAME"] = project
    stack = SmokeStack(project, environment)
    try:
        yield stack
    finally:
        try:
            print(stack.compose("logs", "--no-color", "--tail", "100").stdout)
        finally:
            stack.compose("down", "--volumes", "--remove-orphans")
            image = f"{project}-app:local"
            if stack.docker("image", "inspect", image, check=False).returncode == 0:
                stack.docker("image", "rm", image)


def test_clean_startup_and_migrations(smoke_stack: SmokeStack) -> None:
    stack = smoke_stack
    config = json.loads(stack.compose("config", "--format", "json").stdout)
    assert config["name"] == stack.project
    consumer = config["services"]["rabbitmq-to-payment-worker-smoke"]
    assert consumer["command"] == [
        "python",
        "-m",
        "payment_processor.rabbitmq_to_payment_worker.main",
    ]
    assert not consumer.get("volumes")
    assert not config["services"]["migrate"].get("volumes")
    for service in ("api", *WORKERS):
        assert (
            config["services"][service]["depends_on"]["migrate"]["condition"]
            == "service_completed_successfully"
        )

    stack.compose("build", "migrate", timeout=600)
    stack.compose(
        "run",
        "--rm",
        "--no-deps",
        "migrate",
        "python",
        "-c",
        "from pathlib import Path; "
        "assert Path('/app/alembic.ini').is_file(); "
        "assert Path('/app/alembic/versions').is_dir(); "
        "import fastapi, uvicorn, pydantic, pydantic_settings, asyncpg, sqlalchemy; "
        "import greenlet, faststream.rabbit, alembic, httpx; "
        "import payment_processor.api.main; "
        "import payment_processor.outbox_to_rabbitmq_worker.main; "
        "import payment_processor.rabbitmq_to_payment_worker.main",
    )
    stack.compose(
        "up", "--detach", "--wait", "--wait-timeout", "120", "postgres", "rabbitmq"
    )
    assert stack.schema() == {}
    head = stack.migrate("heads").split()[0]

    stack.migrate("upgrade", "head")
    upgraded_schema = stack.schema()
    assert set(upgraded_schema) == {"payments", "outbox", "alembic_version"}
    assert "webhook_delivered_at" in upgraded_schema["payments"]
    assert stack.sql("SELECT version_num FROM alembic_version") == head
    assert stack.sql("SELECT count(*) FROM payments") == "0"
    assert stack.sql("SELECT count(*) FROM outbox") == "0"

    stack.migrate("downgrade", "base")
    assert stack.schema() == {"alembic_version": ["version_num"]}
    assert stack.sql("SELECT count(*) FROM alembic_version") == "0"

    stack.migrate("upgrade", "head")
    assert stack.schema() == upgraded_schema
    assert stack.sql("SELECT version_num FROM alembic_version") == head
    stack.migrate("check")

    stack.compose(
        "up",
        "--detach",
        "--wait",
        "--wait-timeout",
        "120",
        "api",
        *WORKERS,
    )
    deadline = time.monotonic() + 30
    for worker, component in WORKERS.items():
        while True:
            logs = stack.compose("logs", "--no-color", "--no-log-prefix", worker).stdout
            records = [json.loads(line) for line in logs.splitlines() if line.strip()]
            if any(
                record.get("component") == component
                and record.get("outcome") == "ready"
                for record in records
            ):
                break
            assert time.monotonic() < deadline, (
                f"{worker} did not become ready:\n{logs}"
            )
            time.sleep(0.2)

    services = {
        service["Service"]: service
        for service in map(
            json.loads,
            stack.compose("ps", "--all", "--format", "json").stdout.splitlines(),
        )
    }
    assert set(services) == {"postgres", "rabbitmq", "migrate", "api", *WORKERS}
    for service in ("postgres", "rabbitmq", "api"):
        assert services[service]["State"] == "running", services[service]
        assert services[service]["Health"] == "healthy", services[service]
    for worker in WORKERS:
        assert services[worker]["State"] == "running", services[worker]
    assert services["migrate"]["State"] == "exited", services["migrate"]
    assert services["migrate"]["ExitCode"] == 0, services["migrate"]

    address = stack.compose("port", "api", "8000").stdout.strip()
    with httpx.Client(base_url=f"http://{address}", trust_env=False, timeout=5) as api:
        assert api.get("/health").status_code == 401
        response = api.get("/health", headers={"X-API-Key": "e2e-api-key"})
        assert response.status_code == 200, response.text
        assert response.json() == {"status": "ok"}
