"""make source config_class required

Revision ID: 09b554b1809c
Revises: 965a6588169d
Create Date: 2025-05-18 21:06:42.959439

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '09b554b1809c'
down_revision = '965a6588169d'
branch_labels = None
depends_on = None


def upgrade():
    # First add the column as nullable
    op.add_column('source', sa.Column('config_class', sa.String(), nullable=True))

    # Then set default values for existing rows
    op.execute("UPDATE source SET config_class = 'BaseConfig'")

    # Finally make it NOT NULL
    op.alter_column('source', 'config_class',
                   existing_type=sa.String(),
                   nullable=False)


def downgrade():
    # Simply drop the column on downgrade
    op.drop_column('source', 'config_class')
