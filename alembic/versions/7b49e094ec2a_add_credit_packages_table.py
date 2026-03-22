"""add credit packages table

Revision ID: 7b49e094ec2a
Revises: 6da4bfcb9737
Create Date: 2026-02-25 12:26:39.247161

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7b49e094ec2a'
down_revision: Union[str, Sequence[str], None] = '6da4bfcb9737'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Create credit_packages table
    op.create_table('credit_packages',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('title', sa.String(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('amount', sa.Integer(), nullable=True),
        sa.Column('currency', sa.String(), server_default='INR', nullable=True),
        sa.Column('credits_added', sa.Integer(), nullable=True),
        sa.Column('is_active', sa.Boolean(), server_default='true', nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_credit_packages_id'), 'credit_packages', ['id'], unique=False)
    op.create_index(op.f('ix_credit_packages_name'), 'credit_packages', ['name'], unique=True)

    # Modify payment_transactions
    op.add_column('payment_transactions', sa.Column('package_id', sa.Integer(), nullable=True))
    op.create_foreign_key('fk_payment_transactions_package_id', 'payment_transactions', 'credit_packages', ['package_id'], ['id'])
    op.drop_column('payment_transactions', 'package_name')


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column('payment_transactions', sa.Column('package_name', sa.VARCHAR(), autoincrement=False, nullable=True))
    op.drop_constraint('fk_payment_transactions_package_id', 'payment_transactions', type_='foreignkey')
    op.drop_column('payment_transactions', 'package_id')
    op.drop_index(op.f('ix_credit_packages_name'), table_name='credit_packages')
    op.drop_index(op.f('ix_credit_packages_id'), table_name='credit_packages')
    op.drop_table('credit_packages')
