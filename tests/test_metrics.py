import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from prometheus_client import REGISTRY
from sqlalchemy import update

from payment_processor.adapters import metrics
from payment_processor.core.metrics import measure
from payment_processor.db.models import Outbox


@pytest.mark.asyncio
async def test_operation_metrics_cover_success_failure_and_duration():
    labels = {"operation": "processing", "outcome": "success"}
    before = REGISTRY.get_sample_value("payment_operations_total", labels) or 0
    failures = (
        REGISTRY.get_sample_value(
            "payment_operations_total", {**labels, "outcome": "failed"}
        )
        or 0
    )

    @measure("processing")
    async def work(fail=False):
        if fail:
            raise RuntimeError("private-payment-id")
        return 42

    assert await work() == 42
    with pytest.raises(RuntimeError):
        await work(True)
    assert REGISTRY.get_sample_value("payment_operations_total", labels) == before + 1
    assert (
        REGISTRY.get_sample_value(
            "payment_operations_total", {**labels, "outcome": "failed"}
        )
        == failures + 1
    )
    assert (
        REGISTRY.get_sample_value(
            "payment_operation_duration_seconds_count", {"operation": "processing"}
        )
        >= 2
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("excluded", ["published", "quarantined", "leased", "delayed"])
async def test_outbox_metrics_read_available_backlog_and_exclusions(
    outbox_database, retry_event, excluded
):
    await metrics.collect_outbox(outbox_database)
    assert REGISTRY.get_sample_value("payment_outbox_available") == 1
    assert REGISTRY.get_sample_value("payment_outbox_oldest_age_seconds") >= 0
    now = datetime.now(UTC)
    changes = {
        "published": {"published_at": now},
        "quarantined": {"quarantined_at": now},
        "leased": {"publication_until": now + timedelta(minutes=1)},
        "delayed": {"next_attempt_at": now + timedelta(minutes=1)},
    }
    async with outbox_database.session_scope() as session:
        await session.execute(update(Outbox).values(**changes[excluded]))
    await metrics.collect_outbox(outbox_database)
    assert REGISTRY.get_sample_value("payment_outbox_available") == 0
    assert REGISTRY.get_sample_value("payment_outbox_oldest_age_seconds") == 0
    assert REGISTRY.get_sample_value("payment_collector_up", {"source": "outbox"}) == 1


@pytest.mark.asyncio
async def test_dlq_gauge_comes_from_rabbitmq_and_marks_collection_failure(monkeypatch):
    response = httpx.Response(
        200,
        json={"messages": 7},
        request=httpx.Request("GET", "http://rabbit/api/queues"),
    )
    client = AsyncMock()
    client.get.return_value = response
    client.__aenter__.return_value = client
    monkeypatch.setattr(metrics.httpx, "AsyncClient", Mock(return_value=client))
    await metrics.collect_dlq()
    assert REGISTRY.get_sample_value("payment_dlq_messages") == 7
    assert (
        REGISTRY.get_sample_value("payment_collector_up", {"source": "rabbitmq"}) == 1
    )
    client.get.side_effect = RuntimeError("secret")
    await metrics.collect_dlq()
    assert (
        REGISTRY.get_sample_value("payment_collector_up", {"source": "rabbitmq"}) == 0
    )
    assert REGISTRY.get_sample_value("payment_dlq_messages") == 7


@pytest.mark.asyncio
async def test_worker_metrics_export_requires_key_and_collects_on_scrape(
    monkeypatch, unused_tcp_port
):
    monkeypatch.setattr(
        metrics,
        "settings",
        SimpleNamespace(metrics_port=unused_tcp_port, api_key="metrics-secret"),
    )
    collect = AsyncMock()
    async with metrics.metrics_server(collect):
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{unused_tcp_port}", trust_env=False
        ) as client:
            assert (await client.get("/metrics")).status_code == 401
            assert (
                await client.get("/metrics", headers={"X-API-Key": "wrong"})
            ).status_code == 401
            response = await client.get(
                "/metrics", headers={"X-API-Key": "metrics-secret"}
            )
            assert response.status_code == 200
            assert "payment_operations_total" in response.text
            assert "metrics-secret" not in response.text
            assert "private-payment-id" not in response.text
    collect.assert_awaited_once()
    with pytest.raises(OSError):
        await asyncio.open_connection("127.0.0.1", unused_tcp_port)


@pytest.mark.asyncio
async def test_publication_metrics_count_failed_commit_and_recovery_once(
    outbox_database, retry_event
):
    from contextlib import asynccontextmanager

    from payment_processor.bootstrap import create_outbox_service

    calls = 0

    @asynccontextmanager
    async def failing_commit():
        nonlocal calls
        calls += 1
        async with outbox_database.session_scope() as session:
            yield session
            if calls == 2:
                raise RuntimeError("Commit failed after external success")

    labels = {"operation": "publication", "outcome": "success"}
    success = REGISTRY.get_sample_value("payment_operations_total", labels) or 0
    failed = (
        REGISTRY.get_sample_value(
            "payment_operations_total", {**labels, "outcome": "failed"}
        )
        or 0
    )
    with pytest.raises(RuntimeError, match="Commit failed"):
        await create_outbox_service(failing_commit, AsyncMock()).publish_batch(1)
    assert (
        REGISTRY.get_sample_value(
            "payment_operations_total", {**labels, "outcome": "failed"}
        )
        == failed + 1
    )
    assert (
        REGISTRY.get_sample_value("payment_operations_total", labels) or 0
    ) == success
    await create_outbox_service(
        outbox_database.session_scope, AsyncMock()
    ).publish_batch(1)
    assert REGISTRY.get_sample_value("payment_operations_total", labels) == success + 1
