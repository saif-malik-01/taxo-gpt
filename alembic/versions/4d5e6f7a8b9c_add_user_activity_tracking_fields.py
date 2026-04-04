"""add user activity tracking fields

Revision ID: 4d5e6f7a8b9c
Revises: 3b4c5d6e7f8a
Create Date: 2026-04-04 15:50:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4d5e6f7a8b9c'
down_revision: Union[str, None] = '3b4c5d6e7f8a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add last_login_at and reengagement_email_sent_at to users table
    op.add_column('users', sa.Column('last_login_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('users', sa.Column('reengagement_email_sent_at', sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    # Remove columns
    op.drop_column('users', 'reengagement_email_sent_at')
    op.drop_column('users', 'last_login_at')
