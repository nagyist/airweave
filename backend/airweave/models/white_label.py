"""White label model."""

from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from airweave.models._base import OrganizationBase, UserMixin

if TYPE_CHECKING:
    from airweave.models.source_connection import SourceConnection


class WhiteLabel(OrganizationBase, UserMixin):
    """White label model."""

    __tablename__ = "white_label"

    name: Mapped[str] = mapped_column(String, nullable=False)
    source_short_name: Mapped[str] = mapped_column(
        String, ForeignKey("source.short_name"), nullable=False
    )
    redirect_url: Mapped[str] = mapped_column(String, nullable=False)
    client_id: Mapped[str] = mapped_column(String, nullable=False)
    client_secret: Mapped[str] = mapped_column(String, nullable=False)
    allowed_origins: Mapped[str] = mapped_column(String, nullable=False)

    source_connections: Mapped[list["SourceConnection"]] = relationship(
        "SourceConnection",
        back_populates="white_label",
        lazy="noload",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
