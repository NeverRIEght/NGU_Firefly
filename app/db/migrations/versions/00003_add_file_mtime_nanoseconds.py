"""add_file_mtime_nanoseconds

Revision ID: 00003
Revises: 00002
Create Date: 2026-09-17 12:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "00003"
down_revision: Union[str, None] = "00002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("file", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("file_mtime_nanoseconds", sa.Integer(), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("file", schema=None) as batch_op:
        batch_op.drop_column("file_mtime_nanoseconds")
