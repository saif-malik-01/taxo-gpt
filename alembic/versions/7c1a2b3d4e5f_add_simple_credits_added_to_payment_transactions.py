"""add simple_credits_added to payment_transactions

Revision ID: 7c1a2b3d4e5f
Revises: 1bd5efad8a39
Create Date: 2026-04-12 11:55:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '7c1a2b3d4e5f'
down_revision: Union[str, Sequence[str], None] = '1bd5efad8a39'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Use server_default='0' to handle existing rows
    op.add_column('payment_transactions', sa.Column('simple_credits_added', sa.Integer(), server_default='0', nullable=True))

def downgrade() -> None:
    op.drop_column('payment_transactions', 'simple_credits_added')
