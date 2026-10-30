from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c2d7a19e4b60"
down_revision: str | Sequence[str] | None = "84b9c4a7d621"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "outbox",
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "outbox", sa.Column("quarantined_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_index(
        "ix_outbox_available_created",
        "outbox",
        ["created_at", "id"],
        postgresql_where=sa.text("published_at IS NULL AND quarantined_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_available_created", table_name="outbox")
    op.drop_column("outbox", "quarantined_at")
    op.drop_column("outbox", "next_attempt_at")
