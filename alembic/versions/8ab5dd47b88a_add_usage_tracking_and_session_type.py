"""add usage tracking and session type

Revision ID: 8ab5dd47b88a
Revises: a122d3875fdc
Create Date: 2026-02-25 11:37:43.933264

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8ab5dd47b88a'
down_revision: Union[str, Sequence[str], None] = 'a122d3875fdc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create user_usage table
    op.create_table('user_usage',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('simple_query_count', sa.Integer(), server_default='0', nullable=True),
        sa.Column('draft_reply_count', sa.Integer(), server_default='0', nullable=True),
        sa.Column('last_updated', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('user_id')
    )
    op.create_index(op.f('ix_user_usage_id'), 'user_usage', ['id'], unique=False)

    # Add session_type to chat_sessions
    op.add_column('chat_sessions', sa.Column('session_type', sa.String(), server_default='simple', nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('chat_sessions', 'session_type')
    op.drop_index(op.f('ix_user_usage_id'), table_name='user_usage')
    op.drop_table('user_usage')
