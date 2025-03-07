"""Jira source implementation.

Retrieves data (read-only) from a user's Jira Cloud instance:
 - Projects
 - Issues (within each project)
 - Comments (within each issue)

References:
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/
    https://developer.atlassian.com/cloud/jira/platform/rest/v3/overview
"""

from typing import Any, AsyncGenerator

import httpx

from app.core.logging import logger
from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import BaseEntity, Breadcrumb
from app.platform.entities.jira import (
    JiraCommentEntity,
    JiraIssueEntity,
    JiraProjectEntity,
)
from app.platform.sources._base import BaseSource


@source("Jira", "jira", AuthType.oauth2_with_refresh)
class JiraSource(BaseSource):
    """Jira source implementation (read-only).

    This connector retrieves hierarchical data from Jira's REST API:
      - Projects
      - Issues (within each project)
      - Comments (within each issue)

    The Jira entity schemas are defined in entities/jira.py.
    """

    @staticmethod
    async def _get_accessible_resources(access_token: str) -> list[dict]:
        """Get the list of accessible Atlassian resources for this token.

        Args:
            access_token: The OAuth access token

        Returns:
            list[dict]: List of accessible resources, each containing 'id' and 'url' keys
        """
        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
            try:
                response = await client.get(
                    "https://api.atlassian.com/oauth/token/accessible-resources", headers=headers
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Error getting accessible resources: {str(e)}")
                return []

    @staticmethod
    async def _extract_cloud_id(access_token: str) -> tuple[str, str]:
        """Extract the Atlassian Cloud ID from OAuth 2.0 accessible-resources.

        Args:
            access_token: The OAuth access token

        Returns:
            cloud_id (str): The cloud instance ID
        """
        try:
            resources = await JiraSource._get_accessible_resources(access_token)

            if not resources:
                logger.warning("No accessible resources found")
                return ""

            # Use the first available resource
            # In most cases, there will only be one resource
            resource = resources[0]
            cloud_id = resource.get("id", "")

            if not cloud_id:
                logger.warning("Missing ID in accessible resources")
            return cloud_id

        except Exception as e:
            logger.error(f"Error extracting cloud ID: {str(e)}")
            return ""

    @classmethod
    async def create(cls, access_token: str) -> "JiraSource":
        """Create a new Jira source instance."""
        instance = cls()
        instance.access_token = access_token
        instance.cloud_id = await cls._extract_cloud_id(access_token)
        instance.base_url = f"https://api.atlassian.com/ex/jira/{instance.cloud_id}"
        logger.info(f"Initialized Jira source with base URL: {instance.base_url}")
        return instance

    async def _get_with_auth(self, client: httpx.AsyncClient, url: str) -> Any:
        """Make an authenticated GET request to the Jira REST API using the provided URL.

        By default, we're using OAuth 2.0 with refresh tokens for authentication.
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "X-Atlassian-Token": "no-check",  # Required for CSRF protection
        }

        # Add cloud instance ID if available
        if self.cloud_id:
            headers["X-Cloud-ID"] = self.cloud_id

        logger.debug(f"Making request to {url} with headers: {headers}")
        response = await client.get(url, headers=headers)

        if not response.is_success:
            logger.error(f"Request failed with status {response.status_code}")
            logger.error(f"Response headers: {dict(response.headers)}")
            logger.error(f"Response body: {response.text}")

        response.raise_for_status()
        return response.json()

    async def _generate_project_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[JiraProjectEntity, None]:
        """Generate JiraProjectEntity objects.

        Endpoint:
            GET /rest/api/3/project/search

        Source: https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-projects/#api-rest-api-3-project-search-get

        Args:
        -----
            client: The httpx.AsyncClient instance

        Returns:
        --------
            AsyncGenerator[JiraProjectEntity, None]: An asynchronous generator of JiraProjectEntity
                objects
        """
        search_api_path = "/rest/api/3/project/search"
        max_results = 50
        project_search_url = f"{self.base_url}{search_api_path}?maxResults={max_results}"

        while True:
            data = await self._get_with_auth(client, project_search_url)

            # This endpoint returns a list of projects.
            projects = data.get("values", [])
            for project in projects:
                yield JiraProjectEntity(
                    entity_id=project["id"],
                    breadcrumbs=[],  # top-level object, no parent
                    project_key=project["key"],
                    name=project.get("name"),
                    project_type=project.get("projectTypeKey"),
                    lead=project.get("lead"),
                    description=project.get("description"),
                    archived=project.get("archived", False),
                )

            # Handle pagination
            if data.get("isLast", True):
                break

            start_at = data.get("startAt", 0)
            next_start = start_at + max_results
            project_search_url = (
                f"{self.base_url}{search_api_path}?startAt={next_start}&maxResults={max_results}"
            )

    async def _generate_comment_entities(
        self, client: httpx.AsyncClient, issue: JiraIssueEntity
    ) -> AsyncGenerator[JiraCommentEntity, None]:
        """Generate JiraCommentEntity for each comment on a given issue.

        Endpoint:
            GET /rest/api/3/issue/{issueKey}/comment

        Args:
        -----
            client: The httpx.AsyncClient instance
            issue: The JiraIssueEntity instance

        Returns:
        --------
            AsyncGenerator[JiraCommentEntity, None]: An asynchronous generator of JiraCommentEntity
                objects
        """
        comment_url = f"{self.base_url}/rest/api/3/issue/{issue.issue_key}/comment"
        while True:
            data = await self._get_with_auth(client, comment_url)

            # Comments are nested under data["comments"] or data["values"] depending on the version.
            # For Jira cloud (v3), the field is typically "comments" in the "comments" resource.
            comments = data.get("comments", [])
            for comment in comments:
                yield JiraCommentEntity(
                    entity_id=comment["id"],
                    breadcrumbs=[
                        # Provide a breadcrumb back to the Issue this comment is on
                        Breadcrumb(entity_id=issue.entity_id, name=issue.issue_key, type="issue")
                    ],
                    issue_key=issue.issue_key,
                    body=(
                        comment.get("body", {}).get("content")
                        if isinstance(comment.get("body"), dict)
                        else comment.get("body")
                    ),
                    author=comment.get("author"),
                    created_at=comment.get("created"),
                    updated_at=comment.get("updated"),
                )

            # Handle pagination, if any. Some Jira comment endpoints use "startAt" and "maxResults".
            # Check if we have more in data["total"] vs. data["maxResults"], etc.
            total = data.get("total", 0)
            if not total:
                break

            start_at = data.get("startAt", 0)
            max_results = data.get("maxResults", 50)
            next_start = start_at + max_results

            # If we've retrieved everything or there's no next page, break.
            if next_start >= total:
                break

            # Else, retrieve the next page:
            comment_url = (
                f"{self.base_url}/rest/api/3/issue/{issue.issue_key}/"
                f"comment?startAt={next_start}&maxResults={max_results}"
            )

    async def _generate_issue_entities(
        self, client: httpx.AsyncClient, project: JiraProjectEntity
    ) -> AsyncGenerator[JiraIssueEntity, None]:
        """Generate JiraIssueEntity for each issue in the given project.

        We use the JQL Search endpoint to get issues belonging to the project.

        Endpoint:
            GET /rest/api/3/search/jql?jql=project=<PROJECT_KEY>

        Args:
        -----
            client: The httpx.AsyncClient instance
            project: The JiraProjectEntity instance

        Returns:
        --------
            AsyncGenerator[JiraIssueEntity, None]: An asynchronous generator of JiraIssueEntity
                objects
        """
        project_key = project.project_key
        next_page_token = None

        while True:
            # Construct the search URL with JQL query
            search_url = (
                f"{self.base_url}/rest/api/3/search/jql?jql=project={project_key}"
                "&expand=names,watcher"
            )

            # Add pagination token if we have one
            if next_page_token:
                search_url += f"&nextPageToken={next_page_token}"

            data = await self._get_with_auth(client, search_url)

            issues = data.get("issues", [])

            for issue_data in issues:
                fields = issue_data.get("fields", {})

                # Safely get nested values
                resolution = fields.get("resolution")
                resolution_name = resolution.get("name") if resolution else None

                status = fields.get("status", {})
                status_name = status.get("name") if status else None

                priority = fields.get("priority", {})
                priority_name = priority.get("name") if priority else None

                issue_type = fields.get("issuetype", {})
                issue_type_name = issue_type.get("name") if issue_type else None

                # Handle watchers safely
                watches = fields.get("watches", {})
                watchers_info = watches.get("watchers", []) if watches else []

                # Handle votes safely
                votes = fields.get("votes", {})
                votes_count = votes.get("votes", 0) if votes else 0

                yield JiraIssueEntity(
                    entity_id=issue_data["id"],
                    breadcrumbs=[
                        Breadcrumb(
                            entity_id=project.entity_id, name=project.name or "", type="project"
                        )
                    ],
                    issue_key=issue_data["key"],
                    summary=fields.get("summary"),
                    description=fields.get("description"),
                    status=status_name,
                    priority=priority_name,
                    issue_type=issue_type_name,
                    assignee=fields.get("assignee"),
                    reporter=fields.get("reporter"),
                    resolution=resolution_name,
                    created_at=fields.get("created"),
                    updated_at=fields.get("updated"),
                    resolved_at=fields.get("resolutiondate"),
                    labels=fields.get("labels", []),
                    watchers=watchers_info,
                    votes=votes_count,
                    archived=False,
                )

            # Check if there are more pages
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break

    async def generate_entities(self) -> AsyncGenerator[BaseEntity, None]:
        """Generate all entities from Jira in hierarchical order.

        1. Projects (top-level)
        2. Issues (belonging to each project)
        3. Comments (belonging to each issue)

        This method is called by the sync service to generate entities for the source.

        Yields:
        -------
            AsyncGenerator[BaseEntity, None]: An asynchronous generator of BaseEntity objects
        """
        async with httpx.AsyncClient() as client:
            # 1) Generate (and yield) all Projects
            async for project_entity in self._generate_project_entities(client):
                yield project_entity

                # 2) Generate (and yield) all Issues for each Project
                async for issue_entity in self._generate_issue_entities(client, project_entity):
                    yield issue_entity

                    # 3) Generate (and yield) Comments for each Issue
                    async for comment_entity in self._generate_comment_entities(
                        client, issue_entity
                    ):
                        yield comment_entity
