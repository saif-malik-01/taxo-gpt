"""Add invoice_number to payment_transactions

Revision ID: f9f0e1d2c3b4
Revises: 4d5e6f7a8b9c
Create Date: 2026-04-04 18:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f9f0e1d2c3b4'
down_revision: Union[str, None] = '4d5e6f7a8b9c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add invoice_number column to payment_transactions
    op.add_column('payment_transactions', sa.Column('invoice_number', sa.String(), nullable=True))
    op.create_index(op.f('ix_payment_transactions_invoice_number'), 'payment_transactions', ['invoice_number'], unique=True)


def downgrade() -> None:
    # Remove invoice_number column and index
    op.drop_index(op.f('ix_payment_transactions_invoice_number'), table_name='payment_transactions')
    op.drop_column('payment_transactions', 'invoice_number')
