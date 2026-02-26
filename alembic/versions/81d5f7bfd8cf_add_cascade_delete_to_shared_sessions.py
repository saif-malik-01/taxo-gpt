"""add cascade delete to shared sessions

Revision ID: 81d5f7bfd8cf
Revises: eb8f52f33bcf
Create Date: 2026-02-25 21:49:01.478571

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '81d5f7bfd8cf'
down_revision: Union[str, Sequence[str], None] = 'eb8f52f33bcf'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint('shared_sessions_session_id_fkey', 'shared_sessions', type_='foreignkey')
    op.create_foreign_key(
        'shared_sessions_session_id_fkey',
        'shared_sessions', 'chat_sessions',
        ['session_id'], ['id'], ondelete='CASCADE'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint('shared_sessions_session_id_fkey', 'shared_sessions', type_='foreignkey')
    op.create_foreign_key(
        'shared_sessions_session_id_fkey',
        'shared_sessions', 'chat_sessions',
        ['session_id'], ['id']
    )
