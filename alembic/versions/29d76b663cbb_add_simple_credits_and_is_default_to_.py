"""add simple_credits and is_default to credit_packages

Revision ID: 29d76b663cbb
Revises: f4dbdbde6566
Create Date: 2026-04-11 18:59:29.679361

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '29d76b663cbb'
down_revision: Union[str, Sequence[str], None] = 'f4dbdbde6566'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('credit_packages', sa.Column('simple_credits', sa.Integer(), server_default='0', nullable=True))
    op.add_column('credit_packages', sa.Column('is_default', sa.Boolean(), server_default=sa.text('false'), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('credit_packages', 'is_default')
    op.drop_column('credit_packages', 'simple_credits')
