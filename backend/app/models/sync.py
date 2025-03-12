"""Sync model."""

from typing import TYPE_CHECKING, Optional
from uuid import UUID

from sqlalchemy import JSON, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.shared_models import SyncStatus
from app.models._base import OrganizationBase, UserMixin

if TYPE_CHECKING:
    from app.models.connection import Connection
    from app.models.destination import Destination
    from app.models.entity import Entity
    from app.models.source import Source
    from app.models.sync_connection import SyncConnection
    from app.models.sync_job import SyncJob


class Sync(OrganizationBase, UserMixin):
    """Sync model."""

    __tablename__ = "sync"

    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    status: Mapped[SyncStatus] = mapped_column(default=SyncStatus.ACTIVE)
    cron_schedule: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    white_label_user_identifier: Mapped[str] = mapped_column(String(256), nullable=True)
    sync_metadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # Foreign keys
    white_label_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("white_label.id"), nullable=True
    )

    # Relationships
    jobs: Mapped[list["SyncJob"]] = relationship(
        "SyncJob",
        back_populates="sync",
        lazy="noload",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    entities: Mapped[list["Entity"]] = relationship(
        "Entity",
        back_populates="sync",
        lazy="noload",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    # Generic relationship to all sync connections
    sync_connections: Mapped[list["SyncConnection"]] = relationship(
        "SyncConnection",
        back_populates="sync",
        lazy="noload",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    # Source connection (through SyncConnection)
    source_connection: Mapped[Optional["Connection"]] = relationship(
        "Connection",
        secondary="sync_connection",
        primaryjoin="and_(Sync.id==SyncConnection.sync_id, "
        "SyncConnection.connection_type=='SOURCE')",
        secondaryjoin="SyncConnection.connection_id==Connection.id",
        viewonly=True,
        uselist=False,  # Only one source connection
        lazy="joined",  # Eager loading for this important relationship
    )

    # Source relationship (through Connection and Source)
    source: Mapped[Optional["Source"]] = relationship(
        "Source",
        secondary="connection",
        primaryjoin="and_(Sync.id==SyncConnection.sync_id, "
        "SyncConnection.connection_type=='SOURCE')",
        secondaryjoin="and_(SyncConnection.connection_id==Connection.id, "
        "Connection.short_name==Source.short_name)",
        viewonly=True,
        uselist=False,  # Only one source
        lazy="joined",  # Eager loading
    )

    # Destination connections (through SyncConnection)
    destination_connections: Mapped[list["Connection"]] = relationship(
        "Connection",
        secondary="sync_connection",
        primaryjoin="and_(Sync.id==SyncConnection.sync_id, "
        "SyncConnection.connection_type=='DESTINATION')",
        secondaryjoin="SyncConnection.connection_id==Connection.id",
        viewonly=True,
        lazy="joined",  # Eager loading
    )

    # Destinations relationship (through Connection and Destination)
    destinations: Mapped[list["Destination"]] = relationship(
        "Destination",
        secondary=["sync_connection", "connection"],
        primaryjoin="and_(Sync.id==SyncConnection.sync_id, "
        "SyncConnection.connection_type=='DESTINATION')",
        secondaryjoin="and_(SyncConnection.connection_id==Connection.id, "
        "Connection.short_name==Destination.short_name)",
        viewonly=True,
        lazy="joined",  # Eager loading
    )

    __table_args__ = (
        UniqueConstraint(
            "white_label_id",
            "white_label_user_identifier",
            name="uq_white_label_user",
        ),
    )
