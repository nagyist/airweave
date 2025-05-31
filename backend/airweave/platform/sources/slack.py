"""Slack source implementation."""

from typing import Any, AsyncGenerator, Dict, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from airweave.platform.auth.schemas import AuthType
from airweave.platform.decorators import source
from airweave.platform.entities._base import ChunkEntity
from airweave.platform.entities.slack import (
    SlackChannelEntity,
    SlackMessageEntity,
    SlackUserEntity,
)
from airweave.platform.sources._base import BaseSource


@source(
    name="Slack",
    short_name="slack",
    auth_type=AuthType.oauth2,
    auth_config_class="SlackAuthConfig",
    config_class="SlackConfig",
    labels=["Communication", "Team Collaboration"],
)
class SlackSource(BaseSource):
    """Slack source implementation.

    This connector retrieves data from Slack such as Channels, Users, and Messages,
    then yields them as entities using their respective Slack entity schemas.
    """

    @classmethod
    async def create(cls, credentials, config: Optional[Dict[str, Any]] = None) -> "SlackSource":
        """Create a new Slack source instance."""
        instance = cls()
        instance.access_token = credentials.access_token
        return instance

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _get_with_auth(
        self, client: httpx.AsyncClient, url: str, params: Optional[Dict] = None
    ) -> Dict:
        """Make authenticated GET request to the Slack Web API.

        For example, to retrieve channels:
          GET https://slack.com/api/conversations.list
        """
        # Note: For Slack, we append the token in headers
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        response = await client.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Slack responses include "ok" to indicate success
        if not data.get("ok", False):
            # Special handling for not_in_channel errors
            if data.get("error") == "not_in_channel":
                print(
                    f"Warning: Cannot access resource - "
                    f"not in channel. URL: {url}, params: {params}"
                )
                # Return an empty response with ok=True to prevent retry/error propagation
                return {"ok": True, "channels": [], "messages": [], "members": []}
            # For other errors, raise as usual
            raise httpx.HTTPError(f"Slack API error: {data}")
        return data

    async def _generate_channel_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate SlackChannelEntity objects from Slack using conversations.list.

        Endpoint: https://slack.com/api/conversations.list
        Available scope(s) for reading channels, groups, etc.:
            channels:read, groups:read, im:read, mpim:read
        """
        url = "https://slack.com/api/conversations.list"
        params = {"limit": 200}  # Adjust page size as desired

        while True:
            data = await self._get_with_auth(client, url, params=params)

            for channel in data.get("channels", []):
                yield SlackChannelEntity(
                    entity_id=channel["id"],
                    channel_id=channel["id"],
                    name=channel.get("name"),
                    is_channel=channel.get("is_channel", False),
                    is_group=channel.get("is_group", False),
                    is_im=channel.get("is_im", False),
                    is_mpim=channel.get("is_mpim", False),
                    is_archived=channel.get("is_archived", False),
                    created=channel.get("created"),
                    creator=channel.get("creator"),
                    members=channel.get("members", []),
                    topic=channel.get("topic"),
                    purpose=channel.get("purpose"),
                )

            # Slack pagination uses next_cursor in response_metadata
            next_cursor = data.get("response_metadata", {}).get("next_cursor")
            if not next_cursor:
                break
            params["cursor"] = next_cursor

    async def _generate_user_entities(
        self, client: httpx.AsyncClient
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate SlackUserEntity objects from Slack using users.list.

        Endpoint: https://slack.com/api/users.list
        Scope(s): users:read
        """
        url = "https://slack.com/api/users.list"
        params = {"limit": 200}

        while True:
            data = await self._get_with_auth(client, url, params=params)

            for member in data.get("members", []):
                yield SlackUserEntity(
                    entity_id=member["id"],
                    user_id=member["id"],
                    team_id=member.get("team_id"),
                    name=member.get("name"),
                    real_name=member.get("real_name"),
                    display_name=member.get("profile", {}).get("display_name"),
                    is_bot=member.get("is_bot", False),
                    is_admin=member.get("is_admin", False),
                    is_owner=member.get("is_owner", False),
                    is_primary_owner=member.get("is_primary_owner", False),
                    is_restricted=member.get("is_restricted", False),
                    is_ultra_restricted=member.get("is_ultra_restricted", False),
                    updated=member.get("profile", {}).get("updated"),
                )

            next_cursor = data.get("response_metadata", {}).get("next_cursor")
            if not next_cursor:
                break
            params["cursor"] = next_cursor

    async def _generate_message_entities(
        self, client: httpx.AsyncClient, channel_id: str
    ) -> AsyncGenerator[ChunkEntity, None]:
        """Generate SlackMessageEntity objects for a given channel using conversations.history.

        Endpoint: https://slack.com/api/conversations.history
        Scope(s): channels:history, groups:history, im:history, mpim:history
        """
        url = "https://slack.com/api/conversations.history"
        params = {"channel": channel_id, "limit": 200}

        try:
            while True:
                try:
                    data = await self._get_with_auth(client, url, params=params)

                    for message in data.get("messages", []):
                        yield SlackMessageEntity(
                            entity_id=f"{channel_id}-{message.get('ts')}",
                            channel_id=channel_id,
                            user_id=message.get("user"),
                            text=message.get("text"),
                            ts=message.get("ts"),
                            thread_ts=message.get("thread_ts"),
                            team=message.get("team"),
                            attachments=message.get("attachments", []),
                            blocks=message.get("blocks", []),
                            files=message.get("files", []),
                            reactions=message.get("reactions", []),
                            is_bot=message.get("bot_id") is not None,
                            subtype=message.get("subtype"),
                            edited=message.get("edited"),
                        )

                    next_cursor = data.get("response_metadata", {}).get("next_cursor")
                    if not next_cursor:
                        break
                    params["cursor"] = next_cursor

                except httpx.HTTPError as e:
                    # Check if this is a rate limit error
                    if "429" in str(e):
                        # Let tenacity retry with backoff
                        raise
                    # Handle 'not_in_channel' error before re-raising other errors
                    elif "not_in_channel" in str(e):
                        print(
                            f"Warning: Cannot access messages in channel {channel_id}. "
                            f"User is not a member of this channel."
                        )
                        # Break out of the loop and yield nothing for this channel
                        return
                    else:
                        # Re-raise any other HTTP errors
                        raise
        except httpx.HTTPError as e:
            # This is the critical part - we need to catch and
            # handle the not_in_channel error completely
            if "not_in_channel" in str(e):
                print(
                    f"Warning: Cannot access messages in channel {channel_id}. "
                    f"Bot is not a member of this channel."
                )
                return  # Don't re-raise, just return with no entities
            else:
                # For other errors, we can still re-raise
                raise

    async def generate_entities(self) -> AsyncGenerator[ChunkEntity, None]:
        """Generate all entities from Slack.

        Channels, Users, and Messages.
        """
        async with httpx.AsyncClient() as client:
            # Yield channel entities
            async for channel_entity in self._generate_channel_entities(client):
                yield channel_entity

                # For each channel, also yield message entities
                async for message_entity in self._generate_message_entities(
                    client, channel_entity.channel_id
                ):
                    yield message_entity

            # Yield user entities
            async for user_entity in self._generate_user_entities(client):
                yield user_entity
