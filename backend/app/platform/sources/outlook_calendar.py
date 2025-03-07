"""Outlook Calendar source implementation (read-only).

Retrieves data from a user's Outlook/Microsoft 365 Calendars via Microsoft Graph API:
  - Calendars (GET /me/calendars)
  - Events (GET /me/calendars/{calendar_id}/events)

The entity schemas are defined in entities/outlook_calendar.py.

Reference:
    https://learn.microsoft.com/en-us/graph/api/resources/calendar?view=graph-rest-1.0
    https://learn.microsoft.com/en-us/graph/api/resources/event?view=graph-rest-1.0
"""

from datetime import datetime
from typing import Any, AsyncGenerator, Dict, Optional

import httpx

from app.platform.auth.schemas import AuthType
from app.platform.decorators import source
from app.platform.entities._base import Breadcrumb, ChunkEntity
from app.platform.entities.outlook_calendar import (
    OutlookCalendarCalendarEntity,
    OutlookCalendarEventEntity,
)
from app.platform.sources._base import BaseSource


@source("Outlook Calendar", "outlook_calendar", AuthType.oauth2_with_refresh)
class OutlookCalendarSource(BaseSource):
    """Outlook Calendar source implementation (read-only)."""

    @classmethod
    async def create(cls, access_token: str) -> "OutlookCalendarSource":
        """Create a new Outlook Calendar source instance."""
        instance = cls()
        instance.access_token = access_token
        return instance

    async def _get_with_auth(
        self,
        client: httpx.AsyncClient,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make an authenticated GET request to the Microsoft Graph API."""
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }
        response = await client.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    async def _generate_calendar_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[OutlookCalendarCalendarEntity, None]:
        """Yield OutlookCalendarCalendarEntity objects for each calendar in the user's account.

        Endpoint:
          GET /me/calendars
        """
        base_url = "https://graph.microsoft.com/v1.0/me/calendars"
        # Microsoft Graph uses @odata.nextLink for pagination
        next_url = base_url

        while next_url:
            data = await self._get_with_auth(client, next_url)
            calendars = data.get("value", [])
            for cal in calendars:
                yield OutlookCalendarCalendarEntity(
                    entity_id=cal["id"],  # inherited field for unique ID
                    breadcrumbs=[],  # top-level entity, no parent
                    name=cal.get("name"),
                    color=cal.get("color"),
                    change_key=cal.get("changeKey"),
                    can_edit=cal.get("canEdit", False),
                    can_share=cal.get("canShare", False),
                    can_view_private_items=cal.get("canViewPrivateItems", False),
                    owner=cal.get("owner"),
                )

            # Handle pagination
            next_url = data.get("@odata.nextLink")

    async def _generate_event_entities(
        self,
        client: httpx.AsyncClient,
        calendar: OutlookCalendarCalendarEntity,
    ) -> AsyncGenerator[OutlookCalendarEventEntity, None]:
        """Yield OutlookCalendarEventEntities for each event in the given calendar.

        Endpoint:
          GET /me/calendars/{calendar_id}/events
        """
        calendar_id = calendar.entity_id
        base_url = f"https://graph.microsoft.com/v1.0/me/calendars/{calendar_id}/events"
        next_url = base_url

        # Create a breadcrumb for this calendar
        cal_breadcrumb = Breadcrumb(
            entity_id=calendar_id,
            name=(calendar.name or "")[:50],
            type="calendar",
        )

        while next_url:
            data = await self._get_with_auth(client, next_url)
            events = data.get("value", [])
            for ev in events:
                # Parse date/time stamps
                start_date_str = ev.get("start", {}).get("dateTime")
                end_date_str = ev.get("end", {}).get("dateTime")
                created_at_str = ev.get("createdDateTime")
                updated_at_str = ev.get("lastModifiedDateTime")

                def _parse_dt(dt_str: Optional[str]) -> Optional[datetime]:
                    return (
                        datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                        if dt_str and "T" in dt_str
                        else None
                    )

                yield OutlookCalendarEventEntity(
                    entity_id=ev["id"],
                    breadcrumbs=[cal_breadcrumb],
                    subject=ev.get("subject"),
                    body_preview=ev.get("bodyPreview"),
                    body_content=(ev.get("body", {}).get("content") if ev.get("body") else None),
                    start_datetime=_parse_dt(start_date_str),
                    start_timezone=ev.get("start", {}).get("timeZone"),
                    end_datetime=_parse_dt(end_date_str),
                    end_timezone=ev.get("end", {}).get("timeZone"),
                    is_all_day=ev.get("isAllDay", False),
                    is_cancelled=ev.get("isCancelled", False),
                    show_as=ev.get("showAs"),
                    importance=ev.get("importance"),
                    sensitivity=ev.get("sensitivity"),
                    organizer=ev.get("organizer", {}),
                    attendees=ev.get("attendees"),
                    location=ev.get("location"),
                    created_at=_parse_dt(created_at_str),
                    updated_at=_parse_dt(updated_at_str),
                    web_link=ev.get("webLink"),
                    online_meeting_url=ev.get("onlineMeetingUrl"),
                    series_master_id=ev.get("seriesMasterId"),
                    recurrence=ev.get("recurrence"),
                )

            # Handle pagination
            next_url = data.get("@odata.nextLink")

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate all entities from Outlook Calendar: Calendars and Events."""
        async with httpx.AsyncClient() as client:
            # 1) Get the user's calendars
            async for calendar_entity in self._generate_calendar_entities(client):
                yield calendar_entity

                # 2) For each calendar, get and yield its events
                async for event_entity in self._generate_event_entities(client, calendar_entity):
                    yield event_entity
