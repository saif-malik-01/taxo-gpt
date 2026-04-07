"""add_metadata_snapshot_to_chat_sessions

Revision ID: 45ab7e21a2c3
Revises: aa386e2f3ad8
Create Date: 2026-04-06 20:50:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '45ab7e21a2c3'
down_revision: Union[str, Sequence[str], None] = 'aa386e2f3ad8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('chat_sessions', sa.Column('metadata_snapshot', sa.JSON(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('chat_sessions', 'metadata_snapshot')
