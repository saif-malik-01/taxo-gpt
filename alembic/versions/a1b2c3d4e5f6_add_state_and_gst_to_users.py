"""add state and gst_number to users

Revision ID: a1b2c3d4e5f6
Revises: f9f0e1d2c3b4
Create Date: 2026-04-04 19:15:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 'f9f0e1d2c3b4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add state and gst_number columns to users table
    op.add_column('users', sa.Column('state', sa.String(), nullable=True))
    op.add_column('users', sa.Column('gst_number', sa.String(), nullable=True))


def downgrade() -> None:
    # Remove columns from users table
    op.drop_column('users', 'gst_number')
    op.drop_column('users', 'state')
