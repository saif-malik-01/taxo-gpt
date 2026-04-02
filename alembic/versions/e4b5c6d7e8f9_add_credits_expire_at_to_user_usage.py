"""add credits_expire_at to user_usage

Revision ID: e4b5c6d7e8f9
Revises: dcc73e5f2a1b
Create Date: 2026-04-02 18:20:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text

# revision identifiers, used by Alembic.
revision: str = 'e4b5c6d7e8f9'
down_revision: Union[str, Sequence[str], None] = 'dcc73e5f2a1b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add credits_expire_at column to user_usage table
    op.add_column('user_usage', sa.Column('credits_expire_at', sa.DateTime(timezone=True), nullable=True))
    
    # 2. Set initial expiry to March 1st, 2027 for all CURRENTLY registered users
    # This migration only runs once on existing users.
    op.execute(
        text("UPDATE user_usage SET credits_expire_at = '2027-03-01 00:00:00+00'")
    )


def downgrade() -> None:
    op.drop_column('user_usage', 'credits_expire_at')
