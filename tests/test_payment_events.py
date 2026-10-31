import json
import subprocess
import sys
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from httpx import AsyncClient
from pydantic import ValidationError
from sqlalchemy import select

from payment_processor.db.models import Outbox, OutboxEventType, PaymentCurrency
from payment_processor.db.session import Database
from payment_processor.messaging.schemas import (
    PaymentCreatedEvent,
    PaymentCreatedPayload,
)


@pytest.fixture
def event_payload(payment_payload: dict) -> dict:
    return {"payment_id": str(uuid4()), **payment_payload}


@pytest.mark.parametrize("currency", ["RUB", "USD", "EUR"])
def test_event_json_round_trip_preserves_payment_data(
    event_payload: dict, currency: str
) -> None:
    metadata = {
        "заказ": [{"товар": "Книга", "скидка": 0.5}, True, False, None, {}, []],
        "number": 9007199254740993,
    }
    event_payload.update(
        amount="9999999999999999.99",
        currency=currency,
        description="Оплата книги",
        metadata=metadata,
    )
    event_id = uuid4()
    event = PaymentCreatedEvent.model_validate(
        {
            "event_id": str(event_id),
            "event_type": "payment.created",
            "payload": event_payload,
        }
    )

    serialized = event.model_dump_json()
    restored = PaymentCreatedEvent.model_validate_json(serialized)

    assert restored == event
    assert restored.event_id == event_id
    assert restored.event_type == OutboxEventType.PAYMENT_CREATED
    assert restored.payload.payment_id == UUID(event_payload["payment_id"])
    assert restored.payload.amount == Decimal("9999999999999999.99")
    assert restored.payload.currency == PaymentCurrency(currency)
    assert restored.payload.description == "Оплата книги"
    assert restored.payload.metadata == metadata
    assert str(restored.payload.webhook_url) == event_payload["webhook_url"]
    assert json.loads(serialized)["payload"] == event_payload


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("payment_id", "invalid-uuid"),
        ("amount", "0"),
        ("amount", "-1.00"),
        ("amount", "0.001"),
        ("amount", "10000000000000000.00"),
        ("amount", "NaN"),
        ("amount", "Infinity"),
        ("currency", "GBP"),
        ("description", ""),
        ("description", "a" * 501),
        ("metadata", []),
        ("metadata", {"nested": [float("nan")]}),
        ("metadata", {"nested": {"value": float("inf")}}),
        ("webhook_url", "ftp://example.com/webhook"),
        ("webhook_url", "https://example.com/" + "a" * 2048),
        ("unexpected", True),
    ],
)
def test_invalid_event_payload_is_rejected(
    event_payload: dict, field: str, value: object
) -> None:
    event_payload[field] = value

    with pytest.raises(ValidationError) as caught:
        PaymentCreatedPayload.model_validate(event_payload)

    assert caught.value.errors()[0]["loc"][0] == field


@pytest.mark.parametrize(
    "field",
    ["payment_id", "amount", "currency", "description", "metadata", "webhook_url"],
)
def test_incomplete_event_payload_is_rejected(event_payload: dict, field: str) -> None:
    del event_payload[field]

    with pytest.raises(ValidationError) as caught:
        PaymentCreatedPayload.model_validate(event_payload)

    assert caught.value.errors()[0]["loc"] == (field,)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", None),
        ("event_id", "invalid-uuid"),
        ("event_type", "payment.updated"),
        ("unexpected", True),
    ],
)
def test_invalid_event_envelope_is_rejected(
    event_payload: dict, field: str, value: object
) -> None:
    message = {
        "event_id": str(uuid4()),
        "event_type": "payment.created",
        "payload": event_payload,
        field: value,
    }

    with pytest.raises(ValidationError) as caught:
        PaymentCreatedEvent.model_validate(message)

    assert caught.value.errors()[0]["loc"] == (field,)


def test_legacy_outbox_payload_uses_persisted_event_identity(
    event_payload: dict,
) -> None:
    outbox = Outbox(
        id=uuid4(),
        payment_id=UUID(event_payload["payment_id"]),
        event_type=OutboxEventType.PAYMENT_CREATED,
        payload=event_payload.copy(),
    )

    first = PaymentCreatedEvent.from_outbox(outbox)
    repeated = PaymentCreatedEvent.from_outbox(outbox)

    assert first.event_id == outbox.id
    assert repeated == first
    assert first.event_type == outbox.event_type
    assert first.payload.payment_id == outbox.payment_id
    assert first.payload.model_dump(mode="json") == event_payload
    assert outbox.payload == event_payload


def test_outbox_with_mismatched_payment_id_is_rejected(event_payload: dict) -> None:
    outbox = Outbox(
        id=uuid4(),
        payment_id=uuid4(),
        event_type=OutboxEventType.PAYMENT_CREATED,
        payload=event_payload,
    )

    with pytest.raises(ValueError, match="payment_id does not match"):
        PaymentCreatedEvent.from_outbox(outbox)


@pytest.mark.asyncio
async def test_saved_event_preserves_identity_across_idempotent_requests(
    database: Database,
    client: AsyncClient,
    payment_payload: dict,
    payment_headers: dict[str, str],
) -> None:
    response = await client.post(
        "/api/v1/payments", headers=payment_headers, json=payment_payload
    )
    assert response.status_code == 202
    payment_id = UUID(response.json()["payment_id"])

    async with database.session_factory() as session:
        outbox = (
            await session.scalars(select(Outbox).where(Outbox.payment_id == payment_id))
        ).one()
        event = PaymentCreatedEvent.from_outbox(outbox)
        assert event.event_id == outbox.id
        assert outbox.payload == {"payment_id": str(payment_id), **payment_payload}
        assert event.payload.model_dump(mode="json") == outbox.payload

    repeated = await client.post(
        "/api/v1/payments", headers=payment_headers, json=payment_payload
    )
    assert repeated.status_code == 202
    assert repeated.json() == response.json()

    async with database.session_factory() as session:
        outbox = (
            await session.scalars(select(Outbox).where(Outbox.payment_id == payment_id))
        ).one()
        restored = PaymentCreatedEvent.from_outbox(outbox)

    assert restored == event
    assert PaymentCreatedEvent.model_validate_json(restored.model_dump_json()) == event


def test_event_can_be_built_from_a_record_without_orm(event_payload: dict) -> None:
    record = SimpleNamespace(
        id=uuid4(),
        payment_id=UUID(event_payload["payment_id"]),
        event_type="payment.created",
        payload=event_payload.copy(),
    )

    event = PaymentCreatedEvent.from_outbox(record)

    assert event.model_dump(mode="json") == {
        "event_id": str(record.id),
        "event_type": "payment.created",
        "payload": event_payload,
    }


def test_messaging_schemas_import_without_database_dependencies() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib.abc
import sys

class BlockDatabaseImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "sqlalchemy" or fullname.startswith("payment_processor.db"):
            raise ImportError(f"Unexpected infrastructure import: {fullname}")

sys.meta_path.insert(0, BlockDatabaseImports())
import payment_processor.messaging.schemas
import payment_processor.application.commands
import payment_processor.application.results
""",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
