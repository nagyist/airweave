"""OneDrive source implementation (read-only).

Retrieves data from a user's OneDrive or SharePoint document library, including:
 - Drives (OneDriveDriveEntity objects)
 - DriveItems (OneDriveDriveItemEntity objects) for each drive

This follows a hierarchical pattern (similar to Todoist or Asana):
    Drive
      └── DriveItem (folder)
          └── DriveItem (file/folder)
              └── ...

We fetch and yield these as entity schemas defined in entities/onedrive.py.

Reference (Graph API):
  https://learn.microsoft.com/en-us/graph/api/drive-list?view=graph-rest-1.0
  https://learn.microsoft.com/en-us/graph/api/driveitem-list-children?view=graph-rest-1.0
"""

from collections import deque
from typing import AsyncGenerator, Dict, Optional

import httpx

from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import Breadcrumb
from app.platform.entities.onedrive import OneDriveDriveEntity, OneDriveDriveItemEntity
from app.platform.sources._base import BaseSource


@source("OneDrive", "onedrive", AuthType.oauth2_with_refresh)
class OneDriveSource(BaseSource):
    """OneDrive source implementation (read-only)."""

    @classmethod
    async def create(cls, access_token: str) -> "OneDriveSource":
        """Instantiate a new OneDrive source object with the provided OAuth access token."""
        instance = cls()
        instance.access_token = access_token
        return instance

    async def _get_with_auth(
        self, client: httpx.AsyncClient, url: str, params: Optional[Dict] = None
    ) -> Dict:
        """Make an authenticated GET request to the Microsoft Graph for OneDrive."""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = await client.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    async def _list_drives(self, client: httpx.AsyncClient) -> AsyncGenerator[Dict, None]:
        """List all drives associated with the authorized user or organizational context.

        Endpoint: GET https://graph.microsoft.com/v1.0/me/drives
        (Can also return SharePoint drives under /sites if the account has them.)
        Uses @odata.nextLink for pagination.
        """
        url = "https://graph.microsoft.com/v1.0/me/drives"
        while url:
            data = await self._get_with_auth(client, url)
            for drive in data.get("value", []):
                yield drive
            url = data.get("@odata.nextLink")

    async def _generate_drive_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[OneDriveDriveEntity, None]:
        """Generate OneDriveDriveEntity objects for each drive."""
        async for drive_obj in self._list_drives(client):
            yield OneDriveDriveEntity(
                entity_id=drive_obj["id"],
                breadcrumbs=[],  # top-level entity
                drive_type=drive_obj.get("driveType"),
                owner=drive_obj.get("owner"),
                quota=drive_obj.get("quota"),
                created_at=drive_obj.get("createdDateTime"),
                updated_at=drive_obj.get("lastModifiedDateTime"),
            )

    async def _list_drive_items(
        self,
        client: httpx.AsyncClient,
        drive_id: str,
        initial_url: Optional[str] = None,
    ) -> AsyncGenerator[Dict, None]:
        """List (recursively) all items within a given drive using a BFS approach.

        If initial_url is None, we start from the root children:
          GET /drives/{drive_id}/root/children

        For each folder, we enqueue its children URL to explore deeper.

        Yields each DriveItem dict as we discover it.
        Uses @odata.nextLink for paging within each folder.
        """
        if not initial_url:
            initial_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"

        # Queue of folder-listing URLs to visit (BFS). Start with the root children:
        queue = deque([initial_url])

        while queue:
            url = queue.popleft()
            data = await self._get_with_auth(client, url)
            for item in data.get("value", []):
                # Yield the current item
                yield item
                # If it's a folder, push its children URL to queue
                if "folder" in item:
                    folder_children_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item['id']}/children"
                    queue.append(folder_children_url)

            # If there's a nextLink for paginated results in the current folder
            next_link = data.get("@odata.nextLink")
            if next_link:
                queue.append(next_link)

    async def _generate_drive_item_entities(
        self, client: httpx.AsyncClient, drive_id: str, drive_name: str
    ) -> AsyncGenerator[OneDriveDriveItemEntity, None]:
        """For the specified drive, yield a OneDriveDriveItemEntity for each item.

        We recursively enumerate folders and files.
        """
        # Create a drive breadcrumb for top-level (similar to Google Drive's approach)
        drive_breadcrumb = Breadcrumb(entity_id=drive_id, name=drive_name, type="drive")

        async for item in self._list_drive_items(client, drive_id):
            # Build a entity for each item
            # (Breadcrumbs can optionally be extended if you want each folder in path,
            #  though here we only store drive-level breadcrumb for simplicity.)
            yield OneDriveDriveItemEntity(
                entity_id=item["id"],
                breadcrumbs=[drive_breadcrumb],  # Minimal: just the drive-level
                name=item.get("name"),
                description=item.get("description"),  # Might be missing in many items
                file=item.get("file"),
                folder=item.get("folder"),
                parent_reference=item.get("parentReference"),
                etag=item.get("eTag"),
                ctag=item.get("cTag"),
                created_at=item.get("createdDateTime"),
                updated_at=item.get("lastModifiedDateTime"),
                size=item.get("size"),
                web_url=item.get("webUrl"),
            )

    async def generate_entities(self) -> AsyncGenerator[object, None]:
        """Generate all OneDrive entities.

        Yields entities in the following order:
          - OneDriveDriveEntity for each drive
          - OneDriveDriveItemEntity for each item in each drive (folders/files).
        """
        async with httpx.AsyncClient() as client:
            # 1) Yield drive entities
            #    Note: We'll also collect them in memory to enumerate items from each drive
            drives = []
            async for drive_entity in self._generate_drive_entities(client):
                yield drive_entity
                drives.append(drive_entity)

            # 2) For each drive, yield item entities
            for drive_entity in drives:
                drive_id = drive_entity.entity_id
                drive_name = drive_entity.drive_type or drive_entity.entity_id  # fallback
                async for item_entity in self._generate_drive_item_entities(
                    client, drive_id, drive_name
                ):
                    yield item_entity
