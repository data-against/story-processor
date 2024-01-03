"""remove stories_id

Revision ID: cbebdf62cadd
Revises: edcd2083026b
Create Date: 2023-12-31 13:08:47.727198

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cbebdf62cadd'
down_revision = 'edcd2083026b'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('stories', 'stories_id')


def downgrade():
    op.add_column('stories', sa.Column('stories_id', sa.Integer))
