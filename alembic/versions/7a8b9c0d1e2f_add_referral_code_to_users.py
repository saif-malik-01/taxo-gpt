"""add referral_code to users

Revision ID: 7a8b9c0d1e2f
Revises: 6e7f8a9b0c1d
Create Date: 2026-04-14 11:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7a8b9c0d1e2f'
down_revision: Union[str, Sequence[str], None] = '6e7f8a9b0c1d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('users', sa.Column('referral_code', sa.String(), nullable=True))
    op.add_column('payment_transactions', sa.Column('user_gst_number', sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column('payment_transactions', 'user_gst_number')
    op.drop_column('users', 'referral_code')
