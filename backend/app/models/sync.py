"""Sync model."""

from typing import TYPE_CHECKING, Optional
from uuid import UUID

from sqlalchemy import JSON, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.shared_models import SyncStatus
from app.models._base import OrganizationBase, UserMixin

if TYPE_CHECKING:
    from app.models.entity import Entity
    from app.models.sync_destination import SyncDestination
    from app.models.sync_job import SyncJob


class Sync(OrganizationBase, UserMixin):
    """Sync model."""

    __tablename__ = "sync"

    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    source_connection_id: Mapped[UUID] = mapped_column(ForeignKey("connection.id"), nullable=False)
    destination_connection_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("connection.id"), nullable=True
    )
    status: Mapped[SyncStatus] = mapped_column(default=SyncStatus.ACTIVE)
    embedding_model_connection_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("connection.id"), nullable=True
    )
    cron_schedule: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    white_label_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("white_label.id"), nullable=True
    )
    white_label_user_identifier: Mapped[str] = mapped_column(String(256), nullable=True)
    sync_metadata: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

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

    destinations: Mapped[list["SyncDestination"]] = relationship(
        "SyncDestination",
        back_populates="sync",
        lazy="joined",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "white_label_id",
            "white_label_user_identifier",
            name="uq_white_label_user",
        ),
    )
