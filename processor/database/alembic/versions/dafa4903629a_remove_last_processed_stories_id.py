"""remove last_processed_stories_id

Revision ID: dafa4903629a
Revises: 008514cada9e
Create Date: 2023-12-22 15:13:43.586250

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dafa4903629a'
down_revision = '008514cada9e'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('projects', 'last_processed_id')
    pass


def downgrade():
    op.add_column('projects', sa.Column('last_processed_id', sa.BigInteger(), nullable=True))
    pass
