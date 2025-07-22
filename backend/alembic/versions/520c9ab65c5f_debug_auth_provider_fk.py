"""add foreign key constraint for auth provider cascade deletion

Revision ID: 520c9ab65c5f
Revises: 2181e5765751
Create Date: 2025-07-18 20:42:40.454490

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '520c9ab65c5f'
down_revision = '2181e5765751'
branch_labels = None
depends_on = None


def upgrade():
    # Use raw SQL to clean up orphaned values and create constraint
    # This is idempotent - can be run multiple times safely
    op.execute("""
        -- Clean up orphaned values
        UPDATE source_connection
        SET readable_auth_provider_id = NULL
        WHERE readable_auth_provider_id IS NOT NULL
        AND readable_auth_provider_id NOT IN (
            SELECT readable_id FROM connection WHERE readable_id IS NOT NULL
        );

        -- Create foreign key constraint if it doesn't exist
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_source_connection_readable_auth_provider_id_connection'
            ) THEN
                ALTER TABLE source_connection
                ADD CONSTRAINT fk_source_connection_readable_auth_provider_id_connection
                FOREIGN KEY (readable_auth_provider_id)
                REFERENCES connection(readable_id)
                ON DELETE CASCADE;
            END IF;
        END $$;
    """)


def downgrade():
    op.execute("""
        ALTER TABLE source_connection
        DROP CONSTRAINT IF EXISTS fk_source_connection_readable_auth_provider_id_connection;
    """)
