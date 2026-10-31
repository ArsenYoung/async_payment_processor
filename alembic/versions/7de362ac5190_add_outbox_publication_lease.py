from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "7de362ac5190"
down_revision: str | Sequence[str] | None = "62b9d51e8a40"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("outbox", sa.Column("publication_token", sa.UUID(), nullable=True))
    op.add_column(
        "outbox",
        sa.Column("publication_until", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("outbox", "publication_until")
    op.drop_column("outbox", "publication_token")
