"""add_auth_security_fields_to_users

Revision ID: 1c9d2b4f6a7e
Revises: 7a8b9c0d1e2f
Create Date: 2026-04-15 17:25:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1c9d2b4f6a7e'
down_revision: Union[str, Sequence[str], None] = '7a8b9c0d1e2f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('users', sa.Column('failed_login_attempts', sa.Integer(), nullable=True))
    op.add_column('users', sa.Column('locked_until', sa.DateTime(timezone=True), nullable=True))
    op.add_column('users', sa.Column('is_locked', sa.Boolean(), nullable=True))
    op.add_column('users', sa.Column('csrf_nonce', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('users', 'csrf_nonce')
    op.drop_column('users', 'is_locked')
    op.drop_column('users', 'locked_until')
    op.drop_column('users', 'failed_login_attempts')
