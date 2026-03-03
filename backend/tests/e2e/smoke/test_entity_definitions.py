"""
Smoke tests for Entity Definition endpoints.

Verifies the registry-backed /entities/definitions/by-source/ endpoint
returns correct data without any DB dependency.
"""

import pytest
import httpx


class TestEntityDefinitions:
    """Test suite for Entity Definition API endpoints."""

    @pytest.mark.asyncio
    async def test_get_definitions_for_known_source(self, api_client: httpx.AsyncClient):
        """Test fetching entity definitions for a known source."""
        response = await api_client.get(
            "/entities/definitions/by-source/",
            params={"source_short_name": "asana"},
        )

        assert response.status_code == 200, f"Failed: {response.text}"

        definitions = response.json()
        assert isinstance(definitions, list)
        assert len(definitions) > 0, "Asana should have at least one entity definition"

    @pytest.mark.asyncio
    async def test_definitions_have_required_fields(self, api_client: httpx.AsyncClient):
        """Test that entity definitions contain all expected fields."""
        response = await api_client.get(
            "/entities/definitions/by-source/",
            params={"source_short_name": "asana"},
        )
        assert response.status_code == 200

        required_fields = [
            "short_name",
            "name",
            "description",
            "class_name",
            "module_name",
            "entity_type",
            "entity_schema",
        ]

        for defn in response.json():
            for field in required_fields:
                assert field in defn, (
                    f"Entity definition {defn.get('short_name', '?')} missing field: {field}"
                )

    @pytest.mark.asyncio
    async def test_definitions_do_not_expose_internal_fields(self, api_client: httpx.AsyncClient):
        """Test that internal fields like entity_class_ref are not serialized."""
        response = await api_client.get(
            "/entities/definitions/by-source/",
            params={"source_short_name": "asana"},
        )
        assert response.status_code == 200

        for defn in response.json():
            assert "entity_class_ref" not in defn, "Internal field leaked to API response"

    @pytest.mark.asyncio
    async def test_unknown_source_returns_empty_list(self, api_client: httpx.AsyncClient):
        """Test that a non-existent source returns 200 with an empty list."""
        response = await api_client.get(
            "/entities/definitions/by-source/",
            params={"source_short_name": "this_source_does_not_exist_xyz"},
        )

        assert response.status_code == 200
        assert response.json() == []

    @pytest.mark.asyncio
    async def test_module_name_matches_queried_source(self, api_client: httpx.AsyncClient):
        """Test that all returned definitions belong to the queried source."""
        source = "google_calendar"
        response = await api_client.get(
            "/entities/definitions/by-source/",
            params={"source_short_name": source},
        )
        assert response.status_code == 200

        for defn in response.json():
            assert defn["module_name"] == source, (
                f"Definition {defn['short_name']} has module_name={defn['module_name']}, "
                f"expected {source}"
            )

    @pytest.mark.asyncio
    async def test_entity_schema_is_object(self, api_client: httpx.AsyncClient):
        """Test that entity_schema is a dict (JSON schema)."""
        response = await api_client.get(
            "/entities/definitions/by-source/",
            params={"source_short_name": "slack"},
        )
        assert response.status_code == 200

        for defn in response.json():
            assert isinstance(defn["entity_schema"], dict), (
                f"Entity {defn['short_name']} schema should be a dict"
            )

    @pytest.mark.asyncio
    async def test_multiple_sources_return_different_definitions(
        self, api_client: httpx.AsyncClient
    ):
        """Test that different sources return different entity definitions."""
        resp_asana = await api_client.get(
            "/entities/definitions/by-source/",
            params={"source_short_name": "asana"},
        )
        resp_slack = await api_client.get(
            "/entities/definitions/by-source/",
            params={"source_short_name": "slack"},
        )

        assert resp_asana.status_code == 200
        assert resp_slack.status_code == 200

        asana_names = {d["short_name"] for d in resp_asana.json()}
        slack_names = {d["short_name"] for d in resp_slack.json()}

        assert asana_names.isdisjoint(slack_names), (
            f"Asana and Slack share entity definitions: {asana_names & slack_names}"
        )
