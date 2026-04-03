"""rename soft deleted slugs

Revision ID: 3b4c5d6e7f8a
Revises: e4b5c6d7e8f9
Create Date: 2026-04-03 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import text
import time

# revision identifiers, used by Alembic.
revision: str = '3b4c5d6e7f8a'
down_revision: Union[str, None] = 'e4b5c6d7e8f9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    timestamp = int(time.time())

    # Update credit_packages
    packages = conn.execute(text("SELECT id, name FROM credit_packages WHERE is_deleted = true AND name NOT LIKE '%_deleted_%'")).fetchall()
    for pkg in packages:
        new_name = f"{pkg.name}_deleted_{timestamp}"
        conn.execute(
            text("UPDATE credit_packages SET name = :new_name WHERE id = :id"),
            {"new_name": new_name, "id": pkg.id}
        )

    # Update coupons
    coupons = conn.execute(text("SELECT id, code FROM coupons WHERE is_deleted = true AND code NOT LIKE '%_deleted_%'")).fetchall()
    for coupon in coupons:
        new_code = f"{coupon.code}_deleted_{timestamp}"
        conn.execute(
            text("UPDATE coupons SET code = :new_code WHERE id = :id"),
            {"new_code": new_code, "id": coupon.id}
        )


def downgrade() -> None:
    # Not easily reversible without parsing strings; 
    # data migrations like this generally don't need a downgrade logic.
    pass
