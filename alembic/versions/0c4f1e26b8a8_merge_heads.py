"""merge heads

Revision ID: 0c4f1e26b8a8
Revises: 1a2b3c4d5e6f, 2a9e23e4f9fe
Create Date: 2026-04-01 21:58:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0c4f1e26b8a8'
down_revision: Union[str, Sequence[str], None] = ('1a2b3c4d5e6f', '2a9e23e4f9fe')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
