"""Connection model."""

from typing import TYPE_CHECKING, Optional
from uuid import UUID

from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.shared_models import ConnectionStatus, IntegrationType
from app.models._base import OrganizationBase, UserMixin

if TYPE_CHECKING:
    from app.models.destination import Destination
    from app.models.embedding_model import EmbeddingModel
    from app.models.integration_credential import IntegrationCredential
    from app.models.source import Source
    from app.models.sync import Sync
    from app.models.sync_connection import SyncConnection


class Connection(OrganizationBase, UserMixin):
    """Connection model to manage relationships between integrations and their credentials."""

    __tablename__ = "connection"

    name: Mapped[str] = mapped_column(String, nullable=False)
    integration_type: Mapped[IntegrationType] = mapped_column(
        SQLAlchemyEnum(IntegrationType), nullable=False
    )
    status: Mapped[ConnectionStatus] = mapped_column(
        SQLAlchemyEnum(ConnectionStatus), default=ConnectionStatus.ACTIVE
    )

    # Foreign keys
    integration_credential_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("integration_credential.id"), nullable=True
    )
    short_name: Mapped[str] = mapped_column(String, nullable=False)

    # Relationships
    integration_credential: Mapped[Optional["IntegrationCredential"]] = relationship(
        "IntegrationCredential", back_populates="connections"
    )
    source: Mapped[Optional["Source"]] = relationship(
        "Source",
        primaryjoin="and_(foreign(Connection.short_name)==remote(Source.short_name), "
        "Connection.integration_type=='SOURCE')",
        foreign_keys=[short_name],
        viewonly=True,
        lazy="noload",
    )
    destination: Mapped[Optional["Destination"]] = relationship(
        "Destination",
        primaryjoin="and_(foreign(Connection.short_name)==remote(Destination.short_name), "
        "Connection.integration_type=='DESTINATION')",
        foreign_keys=[short_name],
        viewonly=True,
        lazy="noload",
    )
    embedding_model: Mapped[Optional["EmbeddingModel"]] = relationship(
        "EmbeddingModel",
        primaryjoin="and_(foreign(Connection.short_name)==remote(EmbeddingModel.short_name), "
        "Connection.integration_type=='EMBEDDING_MODEL')",
        foreign_keys=[short_name],
        viewonly=True,
        lazy="noload",
    )

    # Generic relationship to all sync connections
    sync_connections: Mapped[list["SyncConnection"]] = relationship(
        "SyncConnection",
        back_populates="connection",
        lazy="noload",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    # Specialized relationships based on connection type
    # For source connections - syncs where this is the source
    source_syncs: Mapped[list["Sync"]] = relationship(
        "Sync",
        secondary="sync_connection",
        primaryjoin="and_(Connection.id==SyncConnection.connection_id, "
        "Connection.integration_type=='SOURCE', "
        "SyncConnection.connection_type=='SOURCE')",
        secondaryjoin="SyncConnection.sync_id==Sync.id",
        viewonly=True,
        lazy="noload",
    )

    # For destination connections - syncs where this is a destination
    destination_syncs: Mapped[list["Sync"]] = relationship(
        "Sync",
        secondary="sync_connection",
        primaryjoin="and_(Connection.id==SyncConnection.connection_id, "
        "Connection.integration_type=='DESTINATION', "
        "SyncConnection.connection_type=='DESTINATION')",
        secondaryjoin="SyncConnection.sync_id==Sync.id",
        viewonly=True,
        lazy="noload",
    )
