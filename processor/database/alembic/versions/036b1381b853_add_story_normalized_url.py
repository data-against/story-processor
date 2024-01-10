"""add story normalized url

Revision ID: 036b1381b853
Revises: cbebdf62cadd
Create Date: 2024-01-09 20:37:02.068429

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '036b1381b853'
down_revision = 'cbebdf62cadd'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('stories', sa.Column('normalized_url', sa.String))
    op.execute("UPDATE stories SET normalized_url=url")
    op.create_unique_constraint('uq_stories_project_id_normalized_url', 'stories', ['project_id', 'normalized_url'])


def downgrade():
    op.drop_column('stories', 'normalized_url')
    op.drop_constraint('uq_stories_project_id_normalized_url', 'stories')

