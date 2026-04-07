"""merge heads final

Revision ID: 99f1a2b3c4d5
Revises: d8e9f0a1b2c3, 8f3e5f2a1b1c
Create Date: 2026-04-07 14:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '99f1a2b3c4d5'
down_revision: Union[str, Sequence[str], None] = ('d8e9f0a1b2c3', '8f3e5f2a1b1c')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
