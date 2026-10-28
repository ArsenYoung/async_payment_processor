from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "84b9c4a7d621"
down_revision: str | Sequence[str] | None = "fd381d9e8e93"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "payments",
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("payments", "webhook_delivered_at")
