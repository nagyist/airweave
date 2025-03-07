"""Trello source implementation."""

from typing import AsyncGenerator, Dict, List, Optional

import httpx

from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import Breadcrumb, ChunkEntity
from app.platform.entities.trello import (
    TrelloActionEntity,
    TrelloBoardEntity,
    TrelloCardEntity,
    TrelloListEntity,
    TrelloMemberEntity,
    TrelloOrganizationEntity,
)
from app.platform.sources._base import BaseSource


@source("Trello", "trello", AuthType.trello_auth)
class TrelloSource(BaseSource):
    """Trello source implementation.

    This connector retrieves data from Trello objects such as Organizations,
    Boards, Lists, Cards, Members, and Actions, then yields them as entities using
    their respective Trello entity schemas. This version also utilizes the
    'breadcrumb' pattern to reflect the hierarchy:
      Organization → Board → List → Card
    """

    TRELLO_API_BASE = "https://api.trello.com/1"

    @classmethod
    async def create(cls, access_token: str) -> "TrelloSource":
        """Create a new Trello source instance."""
        instance = cls()
        instance.access_token = access_token
        return instance

    async def _get_with_auth(
        self, client: httpx.AsyncClient, endpoint: str, params: Optional[Dict] = None
    ) -> Dict:
        """Make an authenticated GET request to the Trello API.

        The 'key' is provided by dev.integrations.yaml, while 'token' is our access_token.
        For example, to retrieve organizations belonging to the current user:
          GET /members/me/organizations
        """
        params = params or {}
        # The 'key' is provided by dev.integrations.yaml, while 'token' is our access_token.
        # If your code loads the key from a secure location, edit here as needed.
        params["key"] = "2d051c43173fdbd0e89ac5b71333d310"
        params["token"] = self.access_token

        url = f"{self.TRELLO_API_BASE}{endpoint}"
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    async def _generate_organization_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate TrelloOrganizationEntity objects for each organization (workspace).

        GET /members/me/organizations
        """
        data = await self._get_with_auth(client, "/members/me/organizations")
        for org in data:
            yield TrelloOrganizationEntity(
                entity_id=org["id"],
                breadcrumbs=[],  # Top-level, no parent
                display_name=org.get("displayName"),
                name=org.get("name"),
                desc=org.get("desc"),
                website=org.get("website"),
            )

    async def _generate_board_entities(
        self,
        client: httpx.AsyncClient,
        org_id: str,
        parent_breadcrumbs: Optional[List[Breadcrumb]] = None,
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate TrelloBoardEntity objects for each board under a given organization.

        GET /organizations/{id}/boards
        """
        if parent_breadcrumbs is None:
            parent_breadcrumbs = []

        data = await self._get_with_auth(client, f"/organizations/{org_id}/boards")
        for board in data:
            # Create board breadcrumb that includes the organization path
            board_breadcrumbs = parent_breadcrumbs[:]
            board_breadcrumbs.append(
                Breadcrumb(entity_id=board["id"], name=board.get("name"), type="board")
            )

            yield TrelloBoardEntity(
                entity_id=board["id"],
                breadcrumbs=board_breadcrumbs,
                name=board.get("name"),
                desc=board.get("desc"),
                closed=board.get("closed", False),
                pinned=board.get("pinned", False),
                starred=board.get("starred", False),
                url=board.get("url"),
                short_url=board.get("shortUrl"),
                date_last_activity=board.get("dateLastActivity"),
            )

    async def _generate_list_entities(
        self,
        client: httpx.AsyncClient,
        board_id: str,
        parent_breadcrumbs: Optional[List[Breadcrumb]] = None,
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate TrelloListEntity objects for each list in a board.

        GET /boards/{id}/lists
        """
        if parent_breadcrumbs is None:
            parent_breadcrumbs = []

        data = await self._get_with_auth(client, f"/boards/{board_id}/lists")
        for lst in data:
            # Extend the breadcrumb to include the list
            list_breadcrumbs = parent_breadcrumbs[:]
            list_breadcrumbs.append(
                Breadcrumb(entity_id=lst["id"], name=lst.get("name"), type="list")
            )

            yield TrelloListEntity(
                entity_id=lst["id"],
                breadcrumbs=list_breadcrumbs,
                name=lst.get("name"),
                board_id=lst.get("idBoard"),
                closed=lst.get("closed", False),
                subscribed=lst.get("subscribed", False),
            )

    async def _generate_card_entities(
        self,
        client: httpx.AsyncClient,
        board_id: str,
        parent_breadcrumbs: Optional[List[Breadcrumb]] = None,
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate TrelloCardEntity objects for each card in a board.

        GET /boards/{id}/cards
        """
        if parent_breadcrumbs is None:
            parent_breadcrumbs = []

        data = await self._get_with_auth(client, f"/boards/{board_id}/cards")
        for card in data:
            # If needed, you can fetch the list to extend breadcrumbs further.
            # But typically, board-level is enough. Or you can do an extra lookup.
            card_breadcrumbs = parent_breadcrumbs[:]
            # Optionally, if you want to show "list" in the breadcrumb, do a quick GET /lists/{id}
            # to fetch the name
            # For now, we'll just use the board path.
            card_breadcrumbs.append(
                Breadcrumb(entity_id=card["id"], name=card.get("name"), type="card")
            )

            yield TrelloCardEntity(
                entity_id=card["id"],
                breadcrumbs=card_breadcrumbs,
                name=card.get("name"),
                desc=card.get("desc"),
                closed=card.get("closed", False),
                list_id=card.get("idList"),
                board_id=card.get("idBoard"),
                due=card.get("due"),
                due_complete=card.get("dueComplete", False),
                last_activity_at=card.get("dateLastActivity"),
                url=card.get("url"),
            )

    async def _generate_member_entities(
        self,
        client: httpx.AsyncClient,
        board_id: str,
        parent_breadcrumbs: Optional[List[Breadcrumb]] = None,
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate TrelloMemberEntity objects for each member of a board.

        GET /boards/{id}/members
        """
        if parent_breadcrumbs is None:
            parent_breadcrumbs = []

        data = await self._get_with_auth(client, f"/boards/{board_id}/members")
        for member in data:
            # Extend the breadcrumb to identify the member
            member_breadcrumbs = parent_breadcrumbs[:]
            member_breadcrumbs.append(
                Breadcrumb(entity_id=member["id"], name=member.get("fullName"), type="member")
            )

            yield TrelloMemberEntity(
                entity_id=member["id"],
                breadcrumbs=member_breadcrumbs,
                avatar_url=member.get("avatarUrl"),
                initials=member.get("initials"),
                full_name=member.get("fullName"),
                username=member.get("username"),
                confirmed=member.get("confirmed", False),
                organizations=member.get("idOrganizations", []),
                boards=member.get("idBoards", []),
            )

    async def _generate_action_entities(
        self,
        client: httpx.AsyncClient,
        board_id: str,
        parent_breadcrumbs: Optional[List[Breadcrumb]] = None,
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate TrelloActionEntity objects for a board.

        GET /boards/{id}/actions
        """
        if parent_breadcrumbs is None:
            parent_breadcrumbs = []

        # For performance, you might want to filter or paginate,
        # but here we show an example of retrieving all actions.
        data = await self._get_with_auth(client, f"/boards/{board_id}/actions")
        for action in data:
            action_breadcrumbs = parent_breadcrumbs[:]
            action_breadcrumbs.append(
                Breadcrumb(entity_id=action["id"], name=action.get("type"), type="action")
            )

            yield TrelloActionEntity(
                entity_id=action["id"],
                breadcrumbs=action_breadcrumbs,
                action_type=action.get("type"),
                date=action.get("date"),
                member_creator_id=action.get("idMemberCreator"),
                data=action.get("data"),
            )

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate all entities from Trello.

        Organizations, Boards, Lists, Cards, Members, and Actions.
        This version includes a breadcrumb path to reflect the hierarchical
        structure: Organization → Board → List → Card, etc.
        """
        async with httpx.AsyncClient() as client:
            # Yield all organization (workspace) entities
            async for org_entity in self._generate_organization_entities(client):
                yield org_entity

                # Build the breadcrumb for the organization
                org_breadcrumb = Breadcrumb(
                    entity_id=org_entity.entity_id,
                    name=org_entity.display_name or org_entity.name,
                    type="organization",
                )

                # For each organization, yield boards
                async for board_entity in self._generate_board_entities(
                    client, org_entity.entity_id, [org_breadcrumb]
                ):
                    yield board_entity

                    # Now build board breadcrumbs
                    board_breadcrumbs = board_entity.breadcrumbs

                    # Yield lists for each board
                    async for list_entity in self._generate_list_entities(
                        client, board_entity.entity_id, board_breadcrumbs
                    ):
                        yield list_entity

                        # If you want to generate cards per-list, you could do it here.
                        # But as an example, we generate all board cards in one pass below.

                    # Yield cards for each board (not per-list, but per-board)
                    async for card_entity in self._generate_card_entities(
                        client, board_entity.entity_id, board_breadcrumbs
                    ):
                        yield card_entity

                    # Yield members for each board
                    async for member_entity in self._generate_member_entities(
                        client, board_entity.entity_id, board_breadcrumbs
                    ):
                        yield member_entity

                    # Yield actions for each board
                    async for action_entity in self._generate_action_entities(
                        client, board_entity.entity_id, board_breadcrumbs
                    ):
                        yield action_entity
