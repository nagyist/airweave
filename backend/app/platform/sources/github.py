"""GitHub source implementation (read-only).

Retrieves data from a user's GitHub account, focusing on:
 - Repositories
 - Repository Contents (files, directories, etc.)

and yields them as entities using the corresponding GitHub entity schemas
(GithubRepoEntity and GithubContentEntity).

References:
  https://docs.github.com/en/rest/repos/repos
  https://docs.github.com/en/rest/repos/contents

Notes:
  - This connector uses a read-only scope.
  - For each repository, we gather basic repo metadata, then traverse the
    repository contents recursively (default branch only).
"""

from typing import AsyncGenerator, Optional

import httpx

from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import Breadcrumb, ChunkEntity
from app.platform.entities.github import GithubContentEntity, GithubRepoEntity
from app.platform.sources._base import BaseSource


@source("GitHub", "github", AuthType.oauth2_with_refresh)
class GithubSource(BaseSource):
    """GitHub source implementation (read-only)."""

    @classmethod
    async def create(cls, access_token: str) -> "GithubSource":
        """Create a new GitHub source instance."""
        instance = cls()
        instance.access_token = access_token
        return instance

    async def _get_with_auth(
        self, client: httpx.AsyncClient, url: str, params: Optional[dict] = None
    ) -> httpx.Response:
        """Make an authenticated GET request to the GitHub API."""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = await client.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response

    async def _generate_repo_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[GithubRepoEntity, None]:
        """Generate entities for all user-accessible repositories.

        GET /user/repos
        We'll follow pagination by looking for a 'Link' header with 'rel="next"'.
        """
        url = "https://api.github.com/user/repos"
        params = {"per_page": 100}  # We can page in 100-repo increments
        while url:
            resp = await self._get_with_auth(client, url, params=params)
            repos_data = resp.json()  # This should be a list of repo objects
            for repo in repos_data:
                yield GithubRepoEntity(
                    entity_id=str(repo["id"]),
                    breadcrumbs=[],
                    name=repo.get("name"),
                    full_name=repo.get("full_name"),
                    owner_login=repo["owner"].get("login") if repo.get("owner") else None,
                    private=repo.get("private", False),
                    description=repo.get("description"),
                    fork=repo.get("fork", False),
                    created_at=repo.get("created_at"),
                    updated_at=repo.get("updated_at"),
                    pushed_at=repo.get("pushed_at"),
                    homepage=repo.get("homepage"),
                    size=repo.get("size"),
                    stargazers_count=repo.get("stargazers_count", 0),
                    watchers_count=repo.get("watchers_count", 0),
                    language=repo.get("language"),
                    forks_count=repo.get("forks_count", 0),
                    open_issues_count=repo.get("open_issues_count", 0),
                    topics=repo.get("topics", []),
                    default_branch=repo.get("default_branch"),
                    archived=repo.get("archived", False),
                    disabled=repo.get("disabled", False),
                )

            # Attempt to parse pagination links
            link_header = resp.headers.get("Link", "")
            next_url = None
            if link_header:
                # Example: <https://api.github.com/user/repos?page=2>; rel="next", ...
                parts = link_header.split(",")
                for part in parts:
                    segment = part.strip()
                    if 'rel="next"' in segment:
                        next_url = segment.split(";")[0].strip("<>")
                        break
            url = next_url
            params = None  # after first request, rely on next_url for all pagination

    async def _walk_repository_contents(
        self,
        client: httpx.AsyncClient,
        repo_full_name: str,
        branch: str,
        path: str,
        breadcrumbs: Optional[list] = None,
    ) -> AsyncGenerator[GithubContentEntity, None]:
        """Recursively walk the contents of a given repository path on the specified branch.

        Yields GithubContentEntity objects.

        GET /repos/{owner}/{repo}/contents/{path}?ref={branch}
        """
        if breadcrumbs is None:
            breadcrumbs = []

        url = f"https://api.github.com/repos/{repo_full_name}/contents/{path}"
        params = {"ref": branch}
        resp = await self._get_with_auth(client, url, params)
        data = resp.json()

        # data can be a list (directory contents) or a dict (single file)
        if isinstance(data, dict) and data.get("type") == "file":
            # Single file item
            yield GithubContentEntity(
                entity_id=data["sha"],
                breadcrumbs=breadcrumbs,
                repo_full_name=repo_full_name,
                path=data.get("path"),
                sha=data.get("sha"),
                item_type=data.get("type"),
                size=data.get("size"),
                html_url=data.get("html_url"),
                download_url=data.get("download_url"),
                content=data.get("content"),  # base64-encoded if present
                encoding=data.get("encoding"),
            )
        elif isinstance(data, list):
            # Directory listing
            for item in data:
                yield GithubContentEntity(
                    entity_id=item["sha"],
                    breadcrumbs=breadcrumbs,
                    repo_full_name=repo_full_name,
                    path=item.get("path"),
                    sha=item.get("sha"),
                    item_type=item.get("type"),
                    size=item.get("size"),
                    html_url=item.get("html_url"),
                    download_url=item.get("download_url"),
                    # For directories, GitHub won't include content in this listing
                    content=None,
                    encoding=None,
                )
                # If it's a directory, recurse
                if item.get("type") == "dir":
                    dir_breadcrumbs = breadcrumbs + [
                        Breadcrumb(
                            entity_id=item["sha"],
                            name=item.get("name", item.get("path", "")),
                            type="directory",
                        )
                    ]
                    async for sub_item in self._walk_repository_contents(
                        client, repo_full_name, branch, item["path"], dir_breadcrumbs
                    ):
                        yield sub_item

    async def _generate_content_entities(
        self, client: httpx.AsyncClient, repo: GithubRepoEntity
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate content entities for a given repository.

        Walks through the hierarchy of directories/files on the default branch.
        """
        if not repo.default_branch or not repo.full_name:
            return
        # Create a breadcrumb for the repository root
        repo_breadcrumb = Breadcrumb(
            entity_id=repo.entity_id,
            name=repo.name or repo.full_name,
            type="repository",
        )
        async for content_entity in self._walk_repository_contents(
            client,
            repo.full_name,
            repo.default_branch,
            path="",  # start at the repo root
            breadcrumbs=[repo_breadcrumb],
        ):
            yield content_entity

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate and yield entities for GitHub objects.

        Yields entities in the following order:
          - Repositories
            - Contents (files/directories) for each repository
        """
        async with httpx.AsyncClient() as client:
            # 1) Yield repo entities
            async for repo_entity in self._generate_repo_entities(client):
                yield repo_entity

                # 2) For each repo, yield content entities
                async for content_entity in self._generate_content_entities(client, repo_entity):
                    yield content_entity
