"""add_package_id_to_coupons

Revision ID: 3e2f1a0b9c8d
Revises: 7c1a2b3d4e5f
Create Date: 2026-04-12 12:20:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3e2f1a0b9c8d'
down_revision: Union[str, Sequence[str], None] = '7c1a2b3d4e5f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add package_id column to coupons table
    op.add_column('coupons', sa.Column('package_id', sa.Integer(), nullable=True))
    
    # Create foreign key constraint
    op.create_foreign_key(
        'fk_coupons_package_id_credit_packages',
        'coupons', 'credit_packages',
        ['package_id'], ['id']
    )


def downgrade() -> None:
    # Drop foreign key constraint first
    op.drop_constraint('fk_coupons_package_id_credit_packages', 'coupons', type_='foreignkey')
    
    # Drop package_id column
    op.drop_column('coupons', 'package_id')
