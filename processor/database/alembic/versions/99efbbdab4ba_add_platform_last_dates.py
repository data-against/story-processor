"""add platform last dates

Revision ID: 99efbbdab4ba
Revises: dafa4903629a
Create Date: 2023-12-22 16:37:00.073910

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '99efbbdab4ba'
down_revision = 'dafa4903629a'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('projects', 'last_publish_date')
    op.drop_column('projects', 'last_url')
    op.add_column('projects', sa.Column('latest_date_mc', sa.DateTime(), nullable=True))
    op.add_column('projects', sa.Column('latest_date_nc', sa.DateTime(), nullable=True))
    op.add_column('projects', sa.Column('latest_date_wm', sa.DateTime(), nullable=True))


def downgrade():
    op.drop_column('projects', 'latest_date_mc')
    op.drop_column('projects', 'latest_date_nc')
    op.drop_column('projects', 'latest_date_wm')
    op.add_column('projects', sa.Column('last_publish_date', sa.DateTime(), nullable=True))
    op.add_column('projects', sa.Column('last_url', sa.String(), nullable=True))
