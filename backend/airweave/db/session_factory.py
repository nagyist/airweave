# db/session_factory.py
"""DB session factory for dependency injection.

Wraps the raw session creation from session.py into a class that can
be constructed by the container factory and injected into services.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession

from airweave.db.session import AsyncSessionLocal


class DBSessionFactory:
    """Creates async database sessions."""

    @asynccontextmanager
    async def get_db_session(self) -> AsyncIterator[AsyncSession]:
        """Get a database session as a context manager."""
        async with AsyncSessionLocal() as db:
            try:
                yield db
            finally:
                try:
                    await db.close()
                except Exception:
                    pass
