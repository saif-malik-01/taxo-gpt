"""rename credits_added to draft_credits

Revision ID: 1bd5efad8a39
Revises: 29d76b663cbb
Create Date: 2026-04-11 19:04:27.865227

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1bd5efad8a39'
down_revision: Union[str, Sequence[str], None] = '29d76b663cbb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column('credit_packages', 'credits_added', new_column_name='draft_credits')
    op.alter_column('payment_transactions', 'credits_added', new_column_name='draft_credits_added')


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('payment_transactions', 'draft_credits_added', new_column_name='credits_added')
    op.alter_column('credit_packages', 'draft_credits', new_column_name='credits_added')
