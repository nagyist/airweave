# flake8: noqa: F401
"""Schemas for the application."""

from airweave.platform.auth.schemas import OAuth2AuthUrl, OAuth2TokenResponse

from .api_key import (
    APIKey,
    APIKeyCreate,
    APIKeyInDBBase,
    APIKeyUpdate,
)
from .chat import Chat, ChatCreate, ChatMessage, ChatMessageCreate, ChatUpdate
from .collection import (
    Collection,
    CollectionCreate,
    CollectionInDBBase,
    CollectionSearchQuery,
    CollectionUpdate,
)
from .connection import Connection, ConnectionCreate, ConnectionInDBBase, ConnectionUpdate
from .dag import (
    DagEdge,
    DagEdgeCreate,
    DagNode,
    DagNodeCreate,
    SyncDag,
    SyncDagCreate,
    SyncDagUpdate,
)
from .destination import (
    Destination,
    DestinationCreate,
    DestinationInDBBase,
    DestinationUpdate,
    DestinationWithAuthenticationFields,
)
from .embedding_model import (
    EmbeddingModel,
    EmbeddingModelCreate,
    EmbeddingModelInDBBase,
    EmbeddingModelUpdate,
    EmbeddingModelWithAuthenticationFields,
)
from .entity import Entity, EntityCount, EntityCreate, EntityInDBBase, EntityUpdate
from .entity_definition import (
    EntityDefinition,
    EntityDefinitionCreate,
    EntityDefinitionUpdate,
    EntityType,
)
from .entity_relation import (
    EntityRelation,
    EntityRelationCreate,
    EntityRelationUpdate,
)
from .integration_credential import (
    IntegrationCredential,
    IntegrationCredentialCreate,
    IntegrationCredentialCreateEncrypted,
    IntegrationCredentialInDB,
    IntegrationCredentialRawCreate,
    IntegrationCredentialUpdate,
)
from .organization import (
    Organization,
    OrganizationBase,
    OrganizationCreate,
    OrganizationInDBBase,
    OrganizationUpdate,
)
from .search import SearchResponse
from .source import (
    Source,
    SourceCreate,
    SourceInDBBase,
    SourceUpdate,
)
from .source_connection import (
    SourceConnection,
    SourceConnectionCreate,
    SourceConnectionInDBBase,
    SourceConnectionListItem,
    SourceConnectionUpdate,
)
from .sync import (
    Sync,
    SyncBase,
    SyncCreate,
    SyncInDBBase,
    SyncUpdate,
    SyncWithoutConnections,
    SyncWithSourceConnection,
)
from .sync_job import (
    SourceConnectionJob,
    SyncJob,
    SyncJobCreate,
    SyncJobInDBBase,
    SyncJobUpdate,
)
from .transformer import Transformer, TransformerCreate, TransformerUpdate
from .user import (
    User,
    UserCreate,
    UserInDB,
    UserInDBBase,
    UserUpdate,
    UserWithOrganizations,
)
from .white_label import WhiteLabel, WhiteLabelCreate, WhiteLabelInDBBase, WhiteLabelUpdate
