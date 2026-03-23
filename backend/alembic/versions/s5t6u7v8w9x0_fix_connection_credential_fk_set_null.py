"""fix connection integration_credential_id FK to SET NULL on delete

Revision ID: s5t6u7v8w9x0
Revises: 7f3a9c2b1d4e
Create Date: 2026-03-21 00:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "s5t6u7v8w9x0"
down_revision = "7f3a9c2b1d4e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Change integration_credential_id FK to SET NULL on delete."""
    op.drop_constraint(
        "connection_integration_credential_id_fkey",
        "connection",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "connection_integration_credential_id_fkey",
        "connection",
        "integration_credential",
        ["integration_credential_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    """Revert FK to no action on delete."""
    op.drop_constraint(
        "connection_integration_credential_id_fkey",
        "connection",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "connection_integration_credential_id_fkey",
        "connection",
        "integration_credential",
        ["integration_credential_id"],
        ["id"],
    )
