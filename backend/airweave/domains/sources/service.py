"""Source service."""

from airweave import schemas
from airweave.core.protocols.sources import SourceServiceProtocol
from airweave.db.session_factory import DBSessionFactory


class SourceService(SourceServiceProtocol):
    """Service for managing sources."""

    def __init__(self, db_session_factory: DBSessionFactory):
        """Initialize the source service."""
        self.db_session_factory = db_session_factory

    async def get(self, short_name: str) -> schemas.Source:
        """Get a source by short name."""
        async with self.db_session_factory.get_db_session() as db:
            sourerepo.get_by_short_name(db, short_name)

        raise NotImplementedError

    async def list(self) -> list[schemas.Source]:
        """List all sources."""
        raise NotImplementedError
