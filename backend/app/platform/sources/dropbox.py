"""Dropbox source implementation."""

from typing import AsyncGenerator, Dict, List, Optional

import httpx

from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import Breadcrumb, ChunkEntity
from app.platform.entities.dropbox import (
    DropboxAccountEntity,
    DropboxFileEntity,
    DropboxFolderEntity,
)
from app.platform.sources._base import BaseSource


@source("Dropbox", "dropbox", AuthType.oauth2_with_refresh)
class DropboxSource(BaseSource):
    """Dropbox source implementation."""

    @classmethod
    async def create(cls, access_token: str) -> "DropboxSource":
        """Create a new Dropbox source.

        Args:
            access_token: The OAuth2 access token for Dropbox API access.

        Returns:
            A configured DropboxSource instance.
        """
        instance = cls()
        instance.access_token = access_token
        return instance

    async def _get_with_auth(
        self,
        client: httpx.AsyncClient,
        url: str,
        method: str = "GET",
        json_body: Optional[Dict] = None,
    ) -> Dict:
        """Make an authenticated request to the Dropbox API.

        Args:
            client: The HTTPX client instance.
            url: The API endpoint URL.
            method: HTTP method to use (default: "GET").
            json_body: Optional JSON payload for POST requests.

        Returns:
            The JSON response from the API.

        Raises:
            HTTPError: If the request fails.
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        if method.upper() == "GET":
            response = await client.get(url, headers=headers)
        else:
            response = await client.post(url, headers=headers, json=json_body or {})
        response.raise_for_status()
        return response.json()

    async def _generate_account_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate Dropbox account-level entities.

        Args:
            client: The HTTPX client instance.

        Yields:
            Account-level entities containing user/team information.
        """
        yield DropboxAccountEntity(
            entity_id="dummy_account_entity",
            breadcrumbs=[],
            display_name="My Dropbox Account",
            account_id="account-123",
            email="me@example.com",
            profile_photo_url=None,
            is_team=False,
        )

    async def _generate_folder_entities(
        self,
        client: httpx.AsyncClient,
        account_breadcrumb: Breadcrumb,
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate folder entities for a given Dropbox account.

        Args:
            client: The HTTPX client instance.
            account_breadcrumb: Breadcrumb for the parent account.

        Yields:
            Folder entities containing metadata about Dropbox folders.
        """
        # Dummy placeholder for MVP:
        yield DropboxFolderEntity(
            entity_id="folder-entity-id",
            breadcrumbs=[account_breadcrumb],
            folder_id="folder-123",
            name="Example Folder",
            path_lower="/example_folder",
            path_display="/Example Folder",
            shared_folder_id=None,
            is_team_folder=False,
        )

    async def _generate_file_entities(
        self,
        client: httpx.AsyncClient,
        folder_breadcrumbs: List[Breadcrumb],
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate file entities within a given folder.

        Args:
            client: The HTTPX client instance.
            folder_breadcrumbs: List of breadcrumbs for the parent folders.

        Yields:
            File entities containing metadata about Dropbox files.
        """
        yield DropboxFileEntity(
            entity_id="file-entity-id",
            breadcrumbs=folder_breadcrumbs,
            file_id="file-123",
            name="Example File.pdf",
            path_lower="/example_folder/example_file.pdf",
            path_display="/Example Folder/Example File.pdf",
            rev="0123456789abcdef",
            client_modified=None,
            server_modified=None,
            size=1024,
            is_downloadable=True,
            sharing_info={},
        )

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate all entities from Dropbox.

        Yields:
            A sequence of entities in the following order:
            1. Account-level entities
            2. Folder entities for each account
            3. File entities for each folder
        """
        async with httpx.AsyncClient() as client:
            # 1. Account(s)
            async for account_entity in self._generate_account_entities(client):
                yield account_entity

                account_breadcrumb = Breadcrumb(
                    entity_id=account_entity.account_id,
                    name=account_entity.display_name,
                    type="account",
                )

                # 2. Folders
                async for folder_entity in self._generate_folder_entities(
                    client, account_breadcrumb
                ):
                    yield folder_entity

                    folder_breadcrumb = Breadcrumb(
                        entity_id=folder_entity.folder_id,
                        name=folder_entity.name,
                        type="folder",
                    )
                    folder_breadcrumbs = [account_breadcrumb, folder_breadcrumb]

                    # 3. Files
                    async for file_entity in self._generate_file_entities(
                        client, folder_breadcrumbs
                    ):
                        yield file_entity
