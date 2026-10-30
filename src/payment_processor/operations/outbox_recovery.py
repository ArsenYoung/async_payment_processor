import argparse
import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import UUID

from payment_processor.db.models import Outbox
from payment_processor.db.repositories.outbox import OutboxRepository
from payment_processor.db.session import Database
from payment_processor.messaging.schemas import PaymentCreatedEvent


async def recover_event(
    database: Database, event_id: UUID, *, payload: dict[str, Any] | None = None
) -> None:
    async with database.session_scope() as session:
        repository = OutboxRepository(session)
        record = await repository.get_for_recovery(event_id)
        if record is None:
            raise LookupError("Outbox event does not exist")
        if record.published_at is not None or record.quarantined_at is None:
            raise ValueError("Only unpublished quarantined events can be recovered")
        candidate = Outbox(
            id=record.id,
            payment_id=record.payment_id,
            event_type=record.event_type,
            payload=record.payload if payload is None else payload,
        )
        PaymentCreatedEvent.from_outbox(candidate)
        if payload is not None:
            record.payload = payload
        await repository.restore_quarantined(record)


async def _run(event_id: UUID, payload: dict[str, Any] | None) -> None:
    from payment_processor.core.config import settings

    database = Database(settings.database_url)
    try:
        await recover_event(database, event_id, payload=payload)
    finally:
        await database.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Recover one quarantined Outbox event")
    parser.add_argument("--event-id", type=UUID, required=True)
    parser.add_argument("--payload-file", type=Path)
    args = parser.parse_args()
    try:
        payload = None
        if args.payload_file is not None:
            payload = json.loads(args.payload_file.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("Payload must be a JSON object")
        asyncio.run(_run(args.event_id, payload))
    except (ValueError, LookupError, OSError) as error:
        parser.error(str(error))
    print(f"Outbox event {args.event_id} is available for publication")


if __name__ == "__main__":
    main()
