"""Add token tracking columns to usage and messages

Revision ID: 0e5ed8202fc9
Revises: 3cd273a076d3
Create Date: 2026-03-08 11:35:18.741808

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0e5ed8202fc9'
down_revision: Union[str, Sequence[str], None] = '3cd273a076d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('chat_messages', sa.Column('prompt_tokens', sa.Integer(), nullable=True))
    op.add_column('chat_messages', sa.Column('response_tokens', sa.Integer(), nullable=True))
    op.add_column('user_usage', sa.Column('input_tokens_used', sa.BigInteger(), nullable=True))
    op.add_column('user_usage', sa.Column('output_tokens_used', sa.BigInteger(), nullable=True))
    op.add_column('user_usage', sa.Column('total_tokens_used', sa.BigInteger(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('user_usage', 'total_tokens_used')
    op.drop_column('user_usage', 'output_tokens_used')
    op.drop_column('user_usage', 'input_tokens_used')
    op.drop_column('chat_messages', 'response_tokens')
    op.drop_column('chat_messages', 'prompt_tokens')
