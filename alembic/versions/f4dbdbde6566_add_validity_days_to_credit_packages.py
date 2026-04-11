"""add validity_days to credit_packages

Revision ID: f4dbdbde6566
Revises: e7b8c9d0e1f2
Create Date: 2026-04-11 18:19:59.484442

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f4dbdbde6566'
down_revision: Union[str, Sequence[str], None] = 'e7b8c9d0e1f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('credit_packages', sa.Column('validity_days', sa.Integer(), server_default='365', nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('credit_packages', 'validity_days')
