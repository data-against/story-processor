"""upgrade_composite_index

Revision ID: 008514cada9e
Revises: 8d4cf7d3c9cb
Create Date: 2023-10-29 23:57:13.095265

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '008514cada9e'
down_revision = '8d4cf7d3c9cb'
branch_labels = None
depends_on = None

def upgrade():
    # Create new indexes
    op.create_index('stories_processed_above_threshold_true', 'stories', ['processed_date', 'above_threshold'], unique=False, postgresql_where='(above_threshold = true)')
    op.create_index('stories_project_model_score', 'stories', ['project_id', 'model_score'], unique=False)
    op.create_index('stories_project_posted_date', 'stories', ['project_id', 'posted_date'], unique=False)
    op.create_index('stories_source_processed_date', 'stories', ['processed_date', 'source'], unique=False)
    op.create_index('stories_source_published_date', 'stories', ['published_date', 'source'], unique=False)

def downgrade():
    # Drop newly created indexes in reverse order
    op.drop_index('stories_source_published_date', table_name='stories')
    op.drop_index('stories_source_processed_date', table_name='stories')
    op.drop_index('stories_project_posted_date', table_name='stories')
    op.drop_index('stories_project_model_score', table_name='stories')
    op.drop_index('stories_processed_above_threshold_true', table_name='stories', postgresql_where='(above_threshold = true)')
