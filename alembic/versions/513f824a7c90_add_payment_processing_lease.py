from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "513f824a7c90"
down_revision: str | Sequence[str] | None = "c2d7a19e4b60"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("payments", sa.Column("processing_token", sa.UUID(), nullable=True))
    op.add_column(
        "payments",
        sa.Column("processing_until", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("payments", "processing_until")
    op.drop_column("payments", "processing_token")
