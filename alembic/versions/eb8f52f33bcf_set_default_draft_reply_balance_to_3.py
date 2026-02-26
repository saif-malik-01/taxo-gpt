"""set default draft reply balance to 3

Revision ID: eb8f52f33bcf
Revises: 7b49e094ec2a
Create Date: 2026-02-25 21:27:27.011684

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'eb8f52f33bcf'
down_revision: Union[str, Sequence[str], None] = '7b49e094ec2a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column('user_usage', 'draft_reply_balance', server_default='3')


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('user_usage', 'draft_reply_balance', server_default='0')
