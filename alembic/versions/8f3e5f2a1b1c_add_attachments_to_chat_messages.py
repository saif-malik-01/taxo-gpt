"""add_attachments_to_chat_messages

Revision ID: 8f3e5f2a1b1c
Revises: 45ab7e21a2c3
Create Date: 2026-04-06 21:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8f3e5f2a1b1c'
down_revision: Union[str, Sequence[str], None] = '45ab7e21a2c3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('chat_messages', sa.Column('attachments', sa.JSON(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('chat_messages', 'attachments')
