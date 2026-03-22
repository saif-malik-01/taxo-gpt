"""Add verification columns to users

Revision ID: 5b3dcdf5c5aa
Revises: 0e5ed8202fc9
Create Date: 2026-03-08 11:43:00.974354

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '5b3dcdf5c5aa'
down_revision: Union[str, Sequence[str], None] = '0e5ed8202fc9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add columns to users table
    op.add_column('users', sa.Column('is_verified', sa.Boolean(), server_default='false', nullable=True))
    op.add_column('users', sa.Column('verification_token', sa.String(), nullable=True))
    op.create_index(op.f('ix_users_verification_token'), 'users', ['verification_token'], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_users_verification_token'), table_name='users')
    op.drop_column('users', 'verification_token')
    op.drop_column('users', 'is_verified')
