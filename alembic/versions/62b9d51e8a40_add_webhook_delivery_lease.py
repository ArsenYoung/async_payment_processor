from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "62b9d51e8a40"
down_revision: str | Sequence[str] | None = "513f824a7c90"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("payments", sa.Column("webhook_token", sa.UUID(), nullable=True))
    op.add_column(
        "payments",
        sa.Column("webhook_until", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("payments", "webhook_until")
    op.drop_column("payments", "webhook_token")
