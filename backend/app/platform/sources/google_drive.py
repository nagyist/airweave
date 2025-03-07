"""Google Drive source implementation.

Retrieves data from a user's Google Drive (read-only mode):
  - Shared drives (Drive objects)
  - Files within each shared drive
  - Files in the user's "My Drive" (non-shared, corpora=user)

Follows the same structure and pattern as other connector implementations
(e.g., Gmail, Asana, Todoist, HubSpot). The entity schemas are defined in
entities/google_drive.py.

References:
    https://developers.google.com/drive/api/v3/reference/drives (Shared drives)
    https://developers.google.com/drive/api/v3/reference/files  (Files)
"""

from typing import AsyncGenerator, Dict, Optional

import httpx

from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import ChunkEntity
from app.platform.entities.google_drive import GoogleDriveDriveEntity, GoogleDriveFileEntity
from app.platform.sources._base import BaseSource


@source("Google Drive", "google_drive", AuthType.oauth2_with_refresh)
class GoogleDriveSource(BaseSource):
    """Google Drive source implementation (read-only).

    Retrieves and yields:
      - GoogleDriveDriveEntity objects, representing shared drives
      - GoogleDriveFileEntity objects, representing files in each shared drive
      - GoogleDriveFileEntity objects, representing files in the user's My Drive
    """

    @classmethod
    async def create(cls, access_token: str) -> "GoogleDriveSource":
        """Create a new Google Drive source instance with the provided OAuth access token."""
        instance = cls()
        instance.access_token = access_token
        return instance

    async def _get_with_auth(
        self, client: httpx.AsyncClient, url: str, params: Optional[Dict] = None
    ) -> Dict:
        """Make an authenticated GET request to the Google Drive API."""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _list_drives(self, client: httpx.AsyncClient) -> AsyncGenerator[Dict, None]:
        """List all shared drives (Drive objects) using pagination.

        GET https://www.googleapis.com/drive/v3/drives
        """
        url = "https://www.googleapis.com/drive/v3/drives"
        params = {"pageSize": 100}
        while url:
            data = await self._get_with_auth(client, url, params=params)
            drives = data.get("drives", [])
            for drive_obj in drives:
                yield drive_obj

            # Handle pagination
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break  # no more results
            params["pageToken"] = next_page_token
            url = "https://www.googleapis.com/drive/v3/drives"  # keep the same base URL

    async def _generate_drive_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate GoogleDriveDriveEntity objects for each shared drive."""
        async for drive_obj in self._list_drives(client):
            yield GoogleDriveDriveEntity(
                entity_id=drive_obj["id"],
                # No breadcrumbs for top-level drives in this connector
                breadcrumbs=[],
                drive_id=drive_obj["id"],
                name=drive_obj.get("name"),
                kind=drive_obj.get("kind"),
                color_rgb=drive_obj.get("colorRgb"),
                created_time=drive_obj.get("createdTime"),
                hidden=drive_obj.get("hidden", False),
                org_unit_id=drive_obj.get("orgUnitId"),
            )

    async def _list_files_in_drive(
        self, client: httpx.AsyncClient, drive_id: str
    ) -> AsyncGenerator[Dict, None]:
        """List files within a shared drive using pagination.

        GET https://www.googleapis.com/drive/v3/files
        ?driveId=<drive_id>&includeItemsFromAllDrives=true&supportsAllDrives=true&corpora=drive
        """
        url = "https://www.googleapis.com/drive/v3/files"
        params = {
            "pageSize": 100,
            "driveId": drive_id,
            "corpora": "drive",
            "includeItemsFromAllDrives": "true",
            "supportsAllDrives": "true",
            "fields": "nextPageToken, files",
        }
        while url:
            data = await self._get_with_auth(client, url, params=params)
            for file_obj in data.get("files", []):
                yield file_obj

            # Handle pagination
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params["pageToken"] = next_page_token
            url = "https://www.googleapis.com/drive/v3/files"

    async def _list_files_in_my_drive(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[Dict, None]:
        """List files in the user's My Drive (corpora=user) using pagination.

        GET https://www.googleapis.com/drive/v3/files
        ?corpora=user&includeItemsFromAllDrives=false&supportsAllDrives=true
        """
        url = "https://www.googleapis.com/drive/v3/files"
        params = {
            "pageSize": 100,
            "corpora": "user",
            "includeItemsFromAllDrives": "false",
            "supportsAllDrives": "true",
            "fields": "nextPageToken, files",
        }
        while url:
            data = await self._get_with_auth(client, url, params=params)
            for file_obj in data.get("files", []):
                yield file_obj

            # Handle pagination
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params["pageToken"] = next_page_token
            url = "https://www.googleapis.com/drive/v3/files"

    def _build_file_entity(self, file_obj: Dict) -> GoogleDriveFileEntity:
        """Helper to build a GoogleDriveFileEntity from a file API response object."""
        return GoogleDriveFileEntity(
            entity_id=file_obj["id"],
            breadcrumbs=[],
            file_id=file_obj["id"],
            name=file_obj.get("name"),
            mime_type=file_obj.get("mimeType"),
            description=file_obj.get("description"),
            starred=file_obj.get("starred", False),
            trashed=file_obj.get("trashed", False),
            explicitly_trashed=file_obj.get("explicitlyTrashed", False),
            parents=file_obj.get("parents", []),
            owners=file_obj.get("owners", []),
            shared=file_obj.get("shared", False),
            web_view_link=file_obj.get("webViewLink"),
            icon_link=file_obj.get("iconLink"),
            created_time=file_obj.get("createdTime"),
            modified_time=file_obj.get("modifiedTime"),
            size=int(file_obj["size"]) if file_obj.get("size") else None,
            md5_checksum=file_obj.get("md5Checksum"),
        )

    async def _generate_file_entities_in_drive(
        self, client: httpx.AsyncClient, drive_id: str
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate GoogleDriveFileEntity objects for each file in a shared drive."""
        async for file_obj in self._list_files_in_drive(client, drive_id):
            yield self._build_file_entity(file_obj)

    async def _generate_file_entities_in_my_drive(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate GoogleDriveFileEntity objects for each file in the user's My Drive."""
        async for file_obj in self._list_files_in_my_drive(client):
            yield self._build_file_entity(file_obj)

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate all Google Drive entities.

        Yields entities in the following order:
          - Shared drives (Drive objects)
          - Files in each shared drive
          - Files in My Drive (corpora=user)
        """
        async with httpx.AsyncClient() as client:
            # 1) Generate entities for shared drives
            async for drive_entity in self._generate_drive_entities(client):
                yield drive_entity

            # 2) For each shared drive, yield file entities
            #    We'll re-list drives in memory so we don't have to fetch them again
            drive_ids = []
            async for drive_obj in self._list_drives(client):
                drive_ids.append(drive_obj["id"])

            for drive_id in drive_ids:
                async for file_entity in self._generate_file_entities_in_drive(client, drive_id):
                    yield file_entity

            # 3) Finally, yield file entities for My Drive (corpora=user)
            async for mydrive_file_entity in self._generate_file_entities_in_my_drive(client):
                yield mydrive_file_entity
