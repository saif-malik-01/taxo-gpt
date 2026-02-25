"""add max_sessions to users

Revision ID: a122d3875fdc
Revises: 38ebcfe018f3
Create Date: 2026-02-25 11:01:36.699382

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a122d3875fdc'
down_revision: Union[str, Sequence[str], None] = '38ebcfe018f3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('users', sa.Column('max_sessions', sa.Integer(), server_default='1', nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('users', 'max_sessions')
