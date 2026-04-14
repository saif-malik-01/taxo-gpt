"""add onboarding_step and reset usage balances

Revision ID: 6e7f8a9b0c1d
Revises: 3e2f1a0b9c8d
Create Date: 2026-04-14 10:35:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6e7f8a9b0c1d'
down_revision: Union[str, Sequence[str], None] = '3e2f1a0b9c8d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add onboarding_step to users table
    op.add_column('users', sa.Column('onboarding_step', sa.Integer(), server_default='1', nullable=True))
    
    # Update existing users to step 2 if they have credits or active package
    # We assume existing users are already "in" the app
    op.execute("UPDATE users SET onboarding_step = 2")
    
    # Reset UserUsage defaults for future records (handled by model changes already)
    # But for existing users, we keep their credits.

def downgrade() -> None:
    op.drop_column('users', 'onboarding_step')
