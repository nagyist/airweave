"""HubSpot source implementation."""

from typing import AsyncGenerator, Dict

import httpx

from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import ChunkEntity
from app.platform.entities.hubspot import (
    HubspotCompanyEntity,
    HubspotContactEntity,
    HubspotDealEntity,
    HubspotTicketEntity,
)
from app.platform.sources._base import BaseSource


@source("HubSpot", "hubspot", AuthType.oauth2_with_refresh)
class HubspotSource(BaseSource):
    """HubSpot source implementation.

    This connector retrieves data from HubSpot CRM objects such as Contacts,
    Companies, Deals, and Tickets, then yields them as entities using
    their respective entity schemas.
    """

    @classmethod
    async def create(cls, access_token: str) -> "HubspotSource":
        """Create a new HubSpot source instance."""
        instance = cls()
        instance.access_token = access_token
        return instance

    async def _get_with_auth(self, client: httpx.AsyncClient, url: str) -> Dict:
        """Make authenticated GET request to HubSpot API.

        For example, to retrieve contacts:
          GET https://api.hubapi.com/crm/v3/objects/contacts
        """
        response = await client.get(
            url,
            headers={"Authorization": f"Bearer {self.access_token}"},
        )
        response.raise_for_status()
        return response.json()

    async def _generate_contact_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate Contact entities from HubSpot.

        This uses the REST CRM API endpoint for contacts:
          GET /crm/v3/objects/contacts
        """
        url = "https://api.hubapi.com/crm/v3/objects/contacts"
        while url:
            data = await self._get_with_auth(client, url)
            for contact in data.get("results", []):
                yield HubspotContactEntity(
                    entity_id=contact["id"],
                    first_name=contact["properties"].get("firstname"),
                    last_name=contact["properties"].get("lastname"),
                    email=contact["properties"].get("email"),
                    phone=contact["properties"].get("phone"),
                    lifecycle_stage=contact["properties"].get("lifecyclestage"),
                    created_at=contact["createdAt"],
                    updated_at=contact["updatedAt"],
                    archived=contact.get("archived", False),
                )

            # Handle pagination
            paging = data.get("paging", {})
            next_link = paging.get("next", {}).get("link")
            url = next_link if next_link else None

    async def _generate_company_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate Company entities from HubSpot.

        This uses the REST CRM API endpoint for companies:
          GET /crm/v3/objects/companies
        """
        url = "https://api.hubapi.com/crm/v3/objects/companies"
        while url:
            data = await self._get_with_auth(client, url)
            for company in data.get("results", []):
                yield HubspotCompanyEntity(
                    entity_id=company["id"],
                    name=company["properties"].get("name"),
                    domain=company["properties"].get("domain"),
                    industry=company["properties"].get("industry"),
                    phone=company["properties"].get("phone"),
                    website=company["properties"].get("website"),
                    city=company["properties"].get("city"),
                    state=company["properties"].get("state"),
                    zip=company["properties"].get("zip"),
                    created_at=company["createdAt"],
                    updated_at=company["updatedAt"],
                    archived=company.get("archived", False),
                )

            paging = data.get("paging", {})
            next_link = paging.get("next", {}).get("link")
            url = next_link if next_link else None

    async def _generate_deal_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate Deal entities from HubSpot.

        This uses the REST CRM API endpoint for deals:
          GET /crm/v3/objects/deals
        """
        url = "https://api.hubapi.com/crm/v3/objects/deals"
        while url:
            data = await self._get_with_auth(client, url)
            for deal in data.get("results", []):
                yield HubspotDealEntity(
                    entity_id=deal["id"],
                    deal_name=deal["properties"].get("dealname"),
                    amount=(
                        float(deal["properties"].get("amount", 0.0))
                        if deal["properties"].get("amount")
                        else None
                    ),
                    pipeline=deal["properties"].get("pipeline"),
                    deal_stage=deal["properties"].get("dealstage"),
                    close_date=deal["properties"].get("closedate"),
                    created_at=deal["createdAt"],
                    updated_at=deal["updatedAt"],
                    archived=deal.get("archived", False),
                )

            paging = data.get("paging", {})
            next_link = paging.get("next", {}).get("link")
            url = next_link if next_link else None

    async def _generate_ticket_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate Ticket entities from HubSpot.

        This uses the REST CRM API endpoint for tickets:
          GET /crm/v3/objects/tickets
        """
        url = "https://api.hubapi.com/crm/v3/objects/tickets"
        while url:
            data = await self._get_with_auth(client, url)
            for ticket in data.get("results", []):
                yield HubspotTicketEntity(
                    entity_id=ticket["id"],
                    subject=ticket["properties"].get("subject"),
                    content=ticket["properties"].get("content"),
                    status=ticket["properties"].get("hs_pipeline_stage"),
                    created_at=ticket["createdAt"],
                    updated_at=ticket["updatedAt"],
                    archived=ticket.get("archived", False),
                )

            paging = data.get("paging", {})
            next_link = paging.get("next", {}).get("link")
            url = next_link if next_link else None

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate all entities from HubSpot.

        Yields:
            HubSpot entities: Contacts, Companies, Deals, and Tickets.
        """
        async with httpx.AsyncClient() as client:
            # Yield contact entities
            async for contact_entity in self._generate_contact_entities(client):
                yield contact_entity

            # Yield company entities
            async for company_entity in self._generate_company_entities(client):
                yield company_entity

            # Yield deal entities
            async for deal_entity in self._generate_deal_entities(client):
                yield deal_entity

            # Yield ticket entities
            async for ticket_entity in self._generate_ticket_entities(client):
                yield ticket_entity
