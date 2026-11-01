from collections.abc import Awaitable, Callable
from functools import wraps
from time import perf_counter
from typing import ParamSpec, TypeVar

from prometheus_client import Counter, Gauge, Histogram

P = ParamSpec("P")
T = TypeVar("T")
OPERATIONS = Counter(
    "payment_operations_total",
    "Completed application operations",
    ["operation", "outcome"],
)
DURATION = Histogram(
    "payment_operation_duration_seconds",
    "Operation latency including persistence",
    ["operation"],
    buckets=(0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60),
)
OUTBOX_AVAILABLE = Gauge(
    "payment_outbox_available", "Currently available unpublished events"
)
OUTBOX_OLDEST = Gauge(
    "payment_outbox_oldest_age_seconds", "Age of oldest available event"
)
DLQ_SIZE = Gauge(
    "payment_dlq_messages", "Ready and unacknowledged RabbitMQ DLQ messages"
)
COLLECTOR_UP = Gauge(
    "payment_collector_up", "Dependency metrics collection succeeded", ["source"]
)


def record_operation(operation: str, started: float, *, failed: bool = False) -> None:
    if operation not in {
        "processing",
        "publication",
        "webhook_delivery",
        "webhook_http",
    }:
        raise ValueError("Unknown metric operation")
    OPERATIONS.labels(operation, "failed" if failed else "success").inc()
    DURATION.labels(operation).observe(max(0, perf_counter() - started))


def measure(
    operation: str,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    def decorate(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @wraps(function)
        async def measured(*args: P.args, **kwargs: P.kwargs) -> T:
            started = perf_counter()
            failed = False
            try:
                return await function(*args, **kwargs)
            except BaseException:
                failed = True
                raise
            finally:
                record_operation(operation, started, failed=failed)

        return measured

    return decorate
