"""convert usage to wallet and add transactions

Revision ID: 6da4bfcb9737
Revises: 8ab5dd47b88a
Create Date: 2026-02-25 12:14:29.343255

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6da4bfcb9737'
down_revision: Union[str, Sequence[str], None] = '8ab5dd47b88a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Update user_usage table columns
    op.add_column('user_usage', sa.Column('simple_query_balance', sa.Integer(), server_default='1000000', nullable=True))
    op.add_column('user_usage', sa.Column('draft_reply_balance', sa.Integer(), server_default='0', nullable=True))
    op.add_column('user_usage', sa.Column('simple_query_used', sa.Integer(), server_default='0', nullable=True))
    op.add_column('user_usage', sa.Column('draft_reply_used', sa.Integer(), server_default='0', nullable=True))
    
    # Drop old columns
    op.drop_column('user_usage', 'simple_query_count')
    op.drop_column('user_usage', 'draft_reply_count')

    # Create payment_transactions table
    op.create_table('payment_transactions',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('order_id', sa.String(), nullable=True),
        sa.Column('payment_id', sa.String(), nullable=True),
        sa.Column('amount', sa.Integer(), nullable=True),
        sa.Column('currency', sa.String(), server_default='INR', nullable=True),
        sa.Column('package_name', sa.String(), nullable=True),
        sa.Column('credits_added', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(), server_default='pending', nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_payment_transactions_id'), 'payment_transactions', ['id'], unique=False)
    op.create_index(op.f('ix_payment_transactions_order_id'), 'payment_transactions', ['order_id'], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_payment_transactions_order_id'), table_name='payment_transactions')
    op.drop_index(op.f('ix_payment_transactions_id'), table_name='payment_transactions')
    op.drop_table('payment_transactions')
    
    op.add_column('user_usage', sa.Column('draft_reply_count', sa.INTEGER(), server_default=sa.text('0'), autoincrement=False, nullable=True))
    op.add_column('user_usage', sa.Column('simple_query_count', sa.INTEGER(), server_default=sa.text('0'), autoincrement=False, nullable=True))
    
    op.drop_column('user_usage', 'draft_reply_used')
    op.drop_column('user_usage', 'simple_query_used')
    op.drop_column('user_usage', 'draft_reply_balance')
    op.drop_column('user_usage', 'simple_query_balance')
