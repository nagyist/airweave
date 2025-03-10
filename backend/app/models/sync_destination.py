"""Sync destination model."""

from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models._base import OrganizationBase, UserMixin


class SyncDestination(OrganizationBase, UserMixin):
    """Sync destination model for tracking multiple destinations per sync."""

    __tablename__ = "sync_destination"

    sync_id: Mapped[UUID] = mapped_column(
        ForeignKey("sync.id", ondelete="CASCADE"), nullable=False, index=True
    )
    connection_id: Mapped[UUID] = mapped_column(
        ForeignKey("connection.id", ondelete="CASCADE"), nullable=True, index=True
    )
    is_native: Mapped[bool] = mapped_column(default=False)
    destination_type: Mapped[str] = mapped_column(
        nullable=False
    )  # 'weaviate_native', 'neo4j_native', etc.

    # Relationships
    sync = relationship(
        "Sync",
        back_populates="destinations",
    )
    connection = relationship("Connection", lazy="noload")

    __table_args__ = (UniqueConstraint("sync_id", "connection_id", name="uq_sync_connection"),)
