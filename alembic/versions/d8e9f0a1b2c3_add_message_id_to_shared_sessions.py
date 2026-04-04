"""add message_id to shared_sessions

Revision ID: d8e9f0a1b2c3
Revises: a1b2c3d4e5f6
Create Date: 2026-04-04 20:30:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'd8e9f0a1b2c3'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add message_id column to shared_sessions
    op.add_column('shared_sessions', sa.Column('message_id', sa.BigInteger(), nullable=True))
    
    # 2. Make session_id nullable (so message-only shares can exist, 
    # though we currently store both for message shares)
    op.alter_column('shared_sessions', 'session_id', existing_type=sa.String(), nullable=True)
    
    # 3. Create foreign key for message_id
    op.create_foreign_key(
        'shared_sessions_message_id_fkey',
        'shared_sessions', 'chat_messages',
        ['message_id'], ['id'],
        ondelete='CASCADE'
    )


def downgrade() -> None:
    # 1. Drop foreign key
    op.drop_constraint('shared_sessions_message_id_fkey', 'shared_sessions', type_='foreignkey')
    
    # 2. Revert session_id to non-nullable (careful if data exists with null session_id)
    # Since we store session_id even for message shares, this is mostly safe as long as no orphaned links exist.
    op.alter_column('shared_sessions', 'session_id', existing_type=sa.String(), nullable=False)
    
    # 3. Drop message_id column
    op.drop_column('shared_sessions', 'message_id')
