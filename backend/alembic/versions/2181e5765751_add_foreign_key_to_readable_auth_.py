"""add_foreign_key_to_readable_auth_provider_id

Revision ID: 2181e5765751
Revises: 1d71c1892c63
Create Date: 2025-07-18 20:30:11.830353

Note: This migration was superseded by 520c9ab65c5f which properly handles
orphaned values before creating the foreign key constraint.

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2181e5765751'
down_revision = '1d71c1892c63'
branch_labels = None
depends_on = None


def upgrade():
    # First, clean up any orphaned readable_auth_provider_id values
    # Set to NULL any readable_auth_provider_id that doesn't exist in connection.readable_id
    op.execute("""
        UPDATE source_connection sc
        SET readable_auth_provider_id = NULL
        WHERE readable_auth_provider_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM connection c
            WHERE c.readable_id = sc.readable_auth_provider_id
        )
    """)

    # Add foreign key constraint from source_connection.readable_auth_provider_id to connection.readable_id
    op.create_foreign_key(
        "fk_source_connection_readable_auth_provider_id_connection",
        "source_connection",
        "connection",
        ["readable_auth_provider_id"],
        ["readable_id"],
        ondelete="CASCADE"
    )


def downgrade():
    # Remove the foreign key constraint
    op.drop_constraint(
        "fk_source_connection_readable_auth_provider_id_connection",
        "source_connection",
        type_="foreignkey"
    )
