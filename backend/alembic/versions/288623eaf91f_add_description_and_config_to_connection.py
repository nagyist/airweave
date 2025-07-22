"""add description and config to connection

Revision ID: 288623eaf91f
Revises: a1b2c3d4e5f6
Create Date: 2024-12-20 17:14:25.754432

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '288623eaf91f'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add description column to connection table if it doesn't exist
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE connection ADD COLUMN description TEXT;
        EXCEPTION
            WHEN duplicate_column THEN null;
        END $$;
    """)

    # Add config_fields column to connection table if it doesn't exist
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE connection ADD COLUMN config_fields JSON;
        EXCEPTION
            WHEN duplicate_column THEN null;
        END $$;
    """)


def downgrade() -> None:
    # Remove config_fields column from connection table if it exists
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE connection DROP COLUMN config_fields;
        EXCEPTION
            WHEN undefined_column THEN null;
        END $$;
    """)

    # Remove description column from connection table if it exists
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE connection DROP COLUMN description;
        EXCEPTION
            WHEN undefined_column THEN null;
        END $$;
    """)
