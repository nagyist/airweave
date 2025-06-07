"""merge multiple heads

Revision ID: b1170e606aae
Revises: 09b554b1809c, 2d0c02a35fdb
Create Date: 2025-05-20 15:00:25.933401

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b1170e606aae'
down_revision = ('09b554b1809c', '2d0c02a35fdb')
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
