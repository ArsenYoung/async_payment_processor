import argparse
import asyncio
import logging
from datetime import UTC, datetime, timedelta
from math import isfinite

from sqlalchemy import text

from payment_processor.core.config import settings
from payment_processor.core.logging import configure_logging
from payment_processor.core.worker import run_worker
from payment_processor.db.repositories.outbox import SqlAlchemyOutboxRepository
from payment_processor.db.session import Database

logger = logging.getLogger(__name__)


async def cleanup_batch(
    database: Database,
    *,
    cutoff: datetime,
    limit: int,
    dry_run: bool = False,
) -> int:
    async with database.session_scope() as session:
        await session.execute(text("SET LOCAL statement_timeout = '5s'"))
        return await SqlAlchemyOutboxRepository(session).cleanup_published(
            cutoff=cutoff,
            limit=limit,
            dry_run=dry_run,
        )


async def run_cleanup(arguments: argparse.Namespace) -> None:
    database = Database(settings.database_url)

    async def work() -> None:
        while True:
            count = await cleanup_batch(
                database,
                cutoff=datetime.now(UTC) - timedelta(days=arguments.retention_days),
                limit=arguments.batch_size,
                dry_run=arguments.dry_run,
            )
            logger.info(
                "Outbox cleanup batch",
                extra={"count": count, "dry_run": arguments.dry_run},
            )
            if not arguments.repeat:
                return
            await asyncio.sleep(arguments.interval)

    try:
        if arguments.repeat:
            await run_worker(work, name="outbox-cleanup")
        else:
            await work()
    finally:
        await database.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Remove old published Outbox events in bounded batches"
    )
    parser.add_argument(
        "--retention-days", type=float, default=settings.outbox_retention_days
    )
    parser.add_argument(
        "--batch-size", type=int, default=settings.outbox_cleanup_batch_size
    )
    parser.add_argument(
        "--interval", type=float, default=settings.outbox_cleanup_interval
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--repeat", action="store_true")
    arguments = parser.parse_args()
    if (
        not isfinite(arguments.retention_days)
        or arguments.retention_days <= 0
        or arguments.batch_size <= 0
        or not isfinite(arguments.interval)
        or arguments.interval <= 0
    ):
        parser.error("Retention, batch size and interval must be positive and finite")
    configure_logging("outbox-cleanup", settings)
    asyncio.run(run_cleanup(arguments))


if __name__ == "__main__":
    main()
