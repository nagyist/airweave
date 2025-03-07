"""Intercom source implementation."""

from typing import AsyncGenerator, Dict, Optional

import httpx

from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import ChunkEntity
from app.platform.entities.intercom import (
    IntercomCompanyEntity,
    IntercomContactEntity,
    IntercomConversationEntity,
    IntercomTicketEntity,
)
from app.platform.sources._base import BaseSource


@source("Intercom", "intercom", AuthType.oauth2)
class IntercomSource(BaseSource):
    """Intercom source implementation.

    This connector retrieves data from Intercom objects such as Contacts, Companies,
    Conversations, and Tickets, then yields them as entities using their respective
    Intercom entity schemas.
    """

    @classmethod
    async def create(cls, access_token: str) -> "IntercomSource":
        """Create a new Intercom source instance."""
        instance = cls()
        instance.access_token = access_token
        return instance

    async def _get_with_auth(
        self, client: httpx.AsyncClient, url: str, params: Optional[Dict] = None
    ) -> Dict:
        """Make authenticated GET request to the Intercom API.

        For example, to retrieve contacts:
          GET https://api.intercom.io/contacts
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }
        response = await client.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    async def _generate_contact_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate Contact entities from Intercom.

        Using the Contacts endpoint:
          GET https://api.intercom.io/contacts
        """
        url = "https://api.intercom.io/contacts"
        while url:
            data = await self._get_with_auth(client, url)
            for contact in data.get("data", []):
                yield IntercomContactEntity(
                    entity_id=contact.get("id"),
                    role=contact.get("role"),
                    external_id=contact.get("external_id"),
                    email=contact.get("email"),
                    phone=contact.get("phone"),
                    name=contact.get("name"),
                    avatar=contact.get("avatar", {}).get("image_url"),
                    created_at=contact.get("created_at"),
                    updated_at=contact.get("updated_at"),
                    archived=contact.get("archived", False),
                )

            # Handle pagination
            pages = data.get("pages", {})
            next_link = pages.get("next")
            url = next_link if next_link else None

    async def _generate_company_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate Company entities from Intercom.

        Using the Companies endpoint:
          GET https://api.intercom.io/companies
        """
        url = "https://api.intercom.io/companies"
        while url:
            data = await self._get_with_auth(client, url)
            for company in data.get("data", []):
                yield IntercomCompanyEntity(
                    entity_id=company.get("id"),
                    name=company.get("name"),
                    company_id=company.get("company_id"),
                    plan=company.get("plan", {}).get("name"),
                    monthly_spend=company.get("monthly_spend"),
                    session_count=company.get("session_count"),
                    user_count=company.get("user_count"),
                    website=company.get("website"),
                    created_at=company.get("created_at"),
                    updated_at=company.get("updated_at"),
                    archived=company.get("archived", False),
                )

            # Handle pagination
            pages = data.get("pages", {})
            next_link = pages.get("next")
            url = next_link if next_link else None

    async def _generate_conversation_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate Conversation entities from Intercom.

        Using the Conversations endpoint:
          GET https://api.intercom.io/conversations
        """
        url = "https://api.intercom.io/conversations"
        while url:
            data = await self._get_with_auth(client, url)
            for convo in data.get("data", []):
                yield IntercomConversationEntity(
                    entity_id=convo.get("id"),
                    conversation_id=convo.get("id", ""),
                    title=convo.get("title"),
                    state=convo.get("state"),
                    created_at=convo.get("created_at"),
                    updated_at=convo.get("updated_at"),
                    archived=convo.get("archived", False),
                )

            # Handle pagination
            pages = data.get("pages", {})
            next_link = pages.get("next")
            url = next_link if next_link else None

    async def _generate_ticket_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate Ticket entities from Intercom.

        Using a hypothetical Tickets endpoint:
          GET https://api.intercom.io/tickets
        (In Intercom, tickets may be handled within conversations or separate endpoints.
         Adjust the URL or approach accordingly based on your actual Intercom data model.)
        """
        url = "https://api.intercom.io/tickets"
        while url:
            data = await self._get_with_auth(client, url)
            for ticket in data.get("data", []):
                yield IntercomTicketEntity(
                    entity_id=ticket.get("id"),
                    subject=ticket.get("subject"),
                    description=ticket.get("description"),
                    state=ticket.get("state"),
                    contact_id=ticket.get("contact_id"),
                    company_id=ticket.get("company_id"),
                    created_at=ticket.get("created_at"),
                    updated_at=ticket.get("updated_at"),
                    archived=ticket.get("archived", False),
                )

            # Handle pagination
            pages = data.get("pages", {})
            next_link = pages.get("next")
            url = next_link if next_link else None

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate all entities from Intercom.

        Yields:
            Intercom entities: Contacts, Companies, Conversations, and Tickets.
        """
        async with httpx.AsyncClient() as client:
            # Yield contact entities
            async for contact_entity in self._generate_contact_entities(client):
                yield contact_entity

            # Yield company entities
            async for company_entity in self._generate_company_entities(client):
                yield company_entity

            # Yield conversation entities
            async for conversation_entity in self._generate_conversation_entities(client):
                yield conversation_entity

            # Yield ticket entities
            async for ticket_entity in self._generate_ticket_entities(client):
                yield ticket_entity
