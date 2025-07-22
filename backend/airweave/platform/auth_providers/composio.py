"""Composio Test Auth Provider - provides authentication services for other integrations."""

from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException

from airweave.platform.auth.schemas import AuthType
from airweave.platform.auth_providers._base import BaseAuthProvider
from airweave.platform.decorators import auth_provider


@auth_provider(
    name="Composio",
    short_name="composio",
    auth_type=AuthType.api_key,
    auth_config_class="ComposioAuthConfig",
    config_class="ComposioConfig",
)
class ComposioAuthProvider(BaseAuthProvider):
    """Composio authentication provider."""

    # Mapping of Airweave field names to Composio field names
    # Key: Airweave field name, Value: Composio field name
    FIELD_NAME_MAPPING = {
        "api_key": "generic_api_key",  # Stripe and other API key sources
        # Add more mappings as needed
    }

    # Mapping of Airweave source short names to Composio toolkit slugs
    # Key: Airweave short name, Value: Composio slug
    SLUG_NAME_MAPPING = {
        "google_drive": "googledrive",
        "google_calendar": "googlecalendar",
        "outlook_mail": "outlook",
        "outlook_calendar": "outlook",
        "onedrive": "one_drive",
        # Add more mappings as needed
    }

    @classmethod
    async def create(
        cls, credentials: Optional[Any] = None, config: Optional[Dict[str, Any]] = None
    ) -> "ComposioAuthProvider":
        """Create a new Composio auth provider instance.

        Args:
            credentials: Auth credentials containing api_key
            config: Configuration parameters

        Returns:
            A Composio test auth provider instance
        """
        instance = cls()
        instance.api_key = credentials["api_key"]
        instance.integration_id = config["integration_id"]
        instance.account_id = config["account_id"]
        return instance

    def _get_composio_slug(self, airweave_short_name: str) -> str:
        """Get the Composio toolkit slug for an Airweave source short name.

        Args:
            airweave_short_name: The Airweave source short name

        Returns:
            The corresponding Composio toolkit slug
        """
        return self.SLUG_NAME_MAPPING.get(airweave_short_name, airweave_short_name)

    def _map_field_name(self, airweave_field: str) -> str:
        """Map an Airweave field name to the corresponding Composio field name.

        Args:
            airweave_field: The Airweave field name

        Returns:
            The corresponding Composio field name
        """
        return self.FIELD_NAME_MAPPING.get(airweave_field, airweave_field)

    async def _get_with_auth(
        self, client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make authenticated API request using Composio API key.

        Args:
            client: HTTP client
            url: API endpoint URL
            params: Optional query parameters

        Returns:
            JSON response

        Raises:
            httpx.HTTPStatusError: If the request fails
        """
        headers = {"x-api-key": self.api_key}

        try:
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self.logger.error(f"HTTP error from Composio API: {e.response.status_code} for {url}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error accessing Composio API: {url}, {str(e)}")
            raise

    async def get_creds_for_source(
        self, source_short_name: str, source_auth_config_fields: List[str]
    ) -> Dict[str, Any]:
        """Get credentials for a specific source integration.

        Args:
            source_short_name: The short name of the source to get credentials for
            source_auth_config_fields: The fields required for the source auth config

        Returns:
            Credentials dictionary for the source

        Raises:
            HTTPException: If no credentials found for the source
        """
        # Map Airweave source name to Composio slug if needed
        composio_slug = self._get_composio_slug(source_short_name)

        self.logger.info(
            f"🔍 [Composio] Starting credential retrieval for source '{source_short_name}'"
        )
        if composio_slug != source_short_name:
            self.logger.info(
                f"📝 [Composio] Mapped source name '{source_short_name}' "
                f"to Composio slug '{composio_slug}'"
            )

        self.logger.info(f"📋 [Composio] Required auth fields: {source_auth_config_fields}")
        self.logger.info(
            f"🔑 [Composio] Using integration_id='{self.integration_id}', "
            f"account_id='{self.account_id}'"
        )

        async with httpx.AsyncClient() as client:
            # Get accounts matching the source
            source_connected_accounts = await self._get_source_connected_accounts(
                client, composio_slug, source_short_name
            )

            # Find the matching connection
            source_creds_dict = self._find_matching_connection(
                source_connected_accounts, source_short_name
            )

            # Map and validate required fields
            found_credentials = self._map_and_validate_fields(
                source_creds_dict, source_auth_config_fields, source_short_name
            )

            # TODO: pagination

            self.logger.info(f"\n🔑 [Composio] Found credentials: {found_credentials}\n")
            return found_credentials

    async def _get_source_connected_accounts(
        self, client: httpx.AsyncClient, composio_slug: str, source_short_name: str
    ) -> List[Dict[str, Any]]:
        """Get connected accounts for a specific source from Composio.

        Args:
            client: HTTP client
            composio_slug: The Composio toolkit slug
            source_short_name: The original source short name

        Returns:
            List of connected accounts for the source

        Raises:
            HTTPException: If no accounts found for the source
        """
        self.logger.info("🌐 [Composio] Fetching connected accounts from Composio API...")

        connected_accounts_response = await self._get_with_auth(
            client,
            "https://backend.composio.dev/api/v3/connected_accounts",
        )

        total_accounts = len(connected_accounts_response.get("items", []))
        self.logger.info(f"\n📊 [Composio] Total connected accounts found: {total_accounts}\n")

        # Log all available toolkits/slugs for debugging
        all_toolkits = {
            acc.get("toolkit", {}).get("slug", "unknown")
            for acc in connected_accounts_response.get("items", [])
        }
        self.logger.info(f"\n🔧 [Composio] Available toolkit slugs: {sorted(all_toolkits)}\n")

        source_connected_accounts = [
            connected_account
            for connected_account in connected_accounts_response.get("items", [])
            if connected_account.get("toolkit", {}).get("slug") == composio_slug
        ]

        self.logger.info(
            f"\n🎯 [Composio] Found {len(source_connected_accounts)} accounts matching "
            f"slug '{composio_slug}'\n"
        )

        if not source_connected_accounts:
            self.logger.error(
                f"\n❌ [Composio] No connected accounts found for slug '{composio_slug}'. "
                f"Available slugs: {sorted(all_toolkits)}\n"
            )
            raise HTTPException(
                status_code=404,
                detail=f"No connected accounts found for source "
                f"'{source_short_name}' (Composio slug: '{composio_slug}') in Composio.",
            )

        # Log details of each matching account
        for i, account in enumerate(source_connected_accounts):
            acc_id = account.get("id")
            int_id = account.get("auth_config", {}).get("id")
            self.logger.info(
                f"\n  📌 Account {i + 1}: account_id='{acc_id}', integration_id='{int_id}'\n"
            )

        return source_connected_accounts

    def _find_matching_connection(
        self, source_connected_accounts: List[Dict[str, Any]], source_short_name: str
    ) -> Dict[str, Any]:
        """Find the matching connection in the list of connected accounts.

        Args:
            source_connected_accounts: List of connected accounts
            source_short_name: The source short name

        Returns:
            The credential dictionary for the matching connection

        Raises:
            HTTPException: If no matching connection found
        """
        source_creds_dict = None

        for connected_account in source_connected_accounts:
            account_id = connected_account.get("id")
            integration_id = connected_account.get("auth_config", {}).get("id")

            self.logger.debug(
                f"🔍 [Composio] Checking account: integration_id='{integration_id}' "
                f"(looking for '{self.integration_id}'), account_id='{account_id}' "
                f"(looking for '{self.account_id}')"
            )

            if integration_id == self.integration_id and account_id == self.account_id:
                self.logger.info(
                    f"\n✅ [Composio] Found matching connection! "
                    f"integration_id='{integration_id}', account_id='{account_id}'\n"
                )
                source_creds_dict = connected_account.get("state", {}).get("val")

                # Log available credential fields
                if source_creds_dict:
                    available_fields = list(source_creds_dict.keys())
                    self.logger.info(
                        f"\n🔓 [Composio] Available credential fields: {available_fields}\n"
                    )
                    for field, value in source_creds_dict.items():
                        if isinstance(value, str) and len(value) > 10:
                            preview = f"{value[:5]}...{value[-3:]}"
                        else:
                            preview = "<non-string or short value>"
                        self.logger.debug(f"\n  - {field}: {preview}\n")
                break

        if not source_creds_dict:
            self.logger.error(
                f"\n❌ [Composio] No matching connection found with "
                f"integration_id='{self.integration_id}' and account_id='{self.account_id}'\n"
            )
            raise HTTPException(
                status_code=404,
                detail=f"No matching connection in Composio with integration_id="
                f"'{self.integration_id}' and account_id='{self.account_id}' "
                f"for source '{source_short_name}'.",
            )

        return source_creds_dict

    def _map_and_validate_fields(
        self,
        source_creds_dict: Dict[str, Any],
        source_auth_config_fields: List[str],
        source_short_name: str,
    ) -> Dict[str, Any]:
        """Map Airweave field names to Composio fields and validate all required fields exist.

        Args:
            source_creds_dict: The credentials dictionary from Composio
            source_auth_config_fields: Required auth fields
            source_short_name: The source short name

        Returns:
            Dictionary with mapped credentials

        Raises:
            HTTPException: If required fields are missing
        """
        missing_fields = []
        found_credentials = {}

        self.logger.info("🔍 [Composio] Checking for required auth fields...")

        for airweave_field in source_auth_config_fields:
            # Map the field name if needed
            composio_field = self._map_field_name(airweave_field)

            if airweave_field != composio_field:
                self.logger.info(
                    f"\n  🔄 Mapped field '{airweave_field}' to Composio field '{composio_field}'\n"
                )

            if composio_field in source_creds_dict:
                # Store with the original Airweave field name
                found_credentials[airweave_field] = source_creds_dict[composio_field]
                self.logger.info(
                    f"\n  ✅ Found field: '{airweave_field}' (as '{composio_field}' in Composio)\n"
                )
            else:
                missing_fields.append(airweave_field)
                self.logger.warning(
                    f"\n  ❌ Missing field: '{airweave_field}' (looked for "
                    f"'{composio_field}' in Composio)\n"
                )

        if missing_fields:
            available_fields = list(source_creds_dict.keys())
            self.logger.error(
                f"\n❌ [Composio] Missing required fields! "
                f"Required: {source_auth_config_fields}, "
                f"Missing: {missing_fields}, "
                f"Available in Composio: {available_fields}\n"
            )
            raise HTTPException(
                status_code=422,
                detail=f"Missing required auth fields for source '{source_short_name}': "
                f"{missing_fields}. "
                f"Required fields: {source_auth_config_fields}. "
                f"Available fields in Composio credentials: {available_fields}",
            )

        self.logger.info(
            f"\n✅ [Composio] Successfully retrieved all {len(found_credentials)} required "
            f"credential fields for source '{source_short_name}'\n"
        )

        return found_credentials
