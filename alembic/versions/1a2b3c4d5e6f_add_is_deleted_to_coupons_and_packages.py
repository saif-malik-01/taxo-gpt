"""Add is_deleted column to coupons and credit packages

Revision ID: 1a2b3c4d5e6f
Revises: f9e36fffb429
Create Date: 2026-04-01 15:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1a2b3c4d5e6f'
down_revision: Union[str, Sequence[str], None] = 'f9e36fffb429'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('coupons', sa.Column('is_deleted', sa.Boolean(), server_default=sa.text('false'), nullable=True))
    op.add_column('credit_packages', sa.Column('is_deleted', sa.Boolean(), server_default=sa.text('false'), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('credit_packages', 'is_deleted')
    op.drop_column('coupons', 'is_deleted')
