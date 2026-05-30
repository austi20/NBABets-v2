"""Initial baseline schema for Alembic-managed databases."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260526_01"
down_revision = None
branch_labels = None
depends_on = None


def _has_existing_schema() -> bool:
    inspector = sa.inspect(op.get_bind())
    existing = set(inspector.get_table_names())
    return bool(existing - {"alembic_version"})


def _metadata():
    # Importing these modules registers every ORM table on Base.metadata, so the
    # baseline always builds the complete current schema (parity with the models).
    import app.models.all  # noqa: F401
    from app.db.base import Base
    from app.db.models import trading  # noqa: F401

    return Base.metadata


def upgrade() -> None:
    if _has_existing_schema():
        op.execute("SELECT 1")
        return

    _metadata().create_all(bind=op.get_bind())


def downgrade() -> None:
    _metadata().drop_all(bind=op.get_bind())
