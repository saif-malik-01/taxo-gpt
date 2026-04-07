"""create_session_document_texts

Revision ID: e7b8c9d0e1f2
Revises: 99f1a2b3c4d5
Create Date: 2026-04-07 10:45:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'e7b8c9d0e1f2'
down_revision: Union[str, Sequence[str], None] = '99f1a2b3c4d5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # Create the table
    op.create_table(
        'session_document_texts',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('session_id', sa.String(), nullable=False),
        sa.Column('case_id', sa.Integer(), nullable=False),
        sa.Column('filename', sa.String(), nullable=False),
        sa.Column('doc_type', sa.String(), nullable=False),
        sa.Column('extracted_text', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    # Create indexes for performance
    op.create_index(op.f('ix_session_document_texts_id'), 'session_document_texts', ['id'], unique=False)
    op.create_index(op.f('ix_session_document_texts_session_id'), 'session_document_texts', ['session_id'], unique=False)

def downgrade() -> None:
    op.drop_index(op.f('ix_session_document_texts_session_id'), table_name='session_document_texts')
    op.drop_index(op.f('ix_session_document_texts_id'), table_name='session_document_texts')
    op.drop_table('session_document_texts')
