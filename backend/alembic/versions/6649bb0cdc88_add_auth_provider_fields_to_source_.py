"""add_auth_provider_fields_to_source_connection

Revision ID: 6649bb0cdc88
Revises: 7d3eeb781f97
Create Date: 2025-07-12 12:09:46.067093

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON


# revision identifiers, used by Alembic.
revision = '6649bb0cdc88'
down_revision = '7d3eeb781f97'
branch_labels = None
depends_on = None


def upgrade():
    # Add auth_provider_short_name column to track which auth provider was used
    op.add_column('source_connection',
        sa.Column('auth_provider_short_name', sa.String(), nullable=True)
    )

    # Add auth_provider_config column to store the config used with the auth provider
    op.add_column('source_connection',
        sa.Column('auth_provider_config', JSON, nullable=True)
    )


def downgrade():
    # Remove the columns in reverse order
    op.drop_column('source_connection', 'auth_provider_config')
    op.drop_column('source_connection', 'auth_provider_short_name')
