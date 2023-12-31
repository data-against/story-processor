"""unique project story constraint

Revision ID: edcd2083026b
Revises: 99efbbdab4ba
Create Date: 2023-12-30 18:38:23.162319

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'edcd2083026b'
down_revision = '99efbbdab4ba'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("UPDATE stories SET url=CONCAT(url, '?fspid=', id)")
    op.create_unique_constraint('uq_stories_project_id_url', 'stories', ['project_id', 'url'])


def downgrade():
    op.drop_constraint('uq_stories_project_id_url', 'stories')
