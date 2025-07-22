"""CRUD operations for auth providers."""

from airweave.crud._base_public import CRUDPublic
from airweave.models.auth_provider import AuthProvider
from airweave.schemas.auth_provider import AuthProviderCreate, AuthProviderUpdate


class CRUDAuthProvider(CRUDPublic[AuthProvider, AuthProviderCreate, AuthProviderUpdate]):
    """CRUD operations for auth providers."""

    pass


# Create instance for use in endpoints
auth_provider = CRUDAuthProvider(AuthProvider)
