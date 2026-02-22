"""Fake source validation service for testing."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import HTTPException
from pydantic import BaseModel


class FakeSourceValidationService:
    """Test implementation of SourceValidationServiceProtocol.

    Returns canned results. Configure via seed methods.
    """

    def __init__(self) -> None:
        self._config_results: dict[str, Dict[str, Any]] = {}
        self._auth_results: dict[str, Any] = {}
        self._should_raise: Optional[Exception] = None
        self._calls: list[tuple[Any, ...]] = []

    def seed_config_result(self, short_name: str, result: Dict[str, Any]) -> None:
        self._config_results[short_name] = result

    def seed_auth_result(self, short_name: str, result: Any) -> None:
        self._auth_results[short_name] = result

    def set_error(self, error: Exception) -> None:
        self._should_raise = error

    def validate_config(self, short_name: str, config_fields: Any, ctx: Any) -> Dict[str, Any]:
        self._calls.append(("validate_config", short_name, config_fields))
        if self._should_raise:
            raise self._should_raise
        if short_name not in self._config_results:
            raise HTTPException(status_code=404, detail=f"Source '{short_name}' not found")
        return self._config_results[short_name]

    def validate_auth(self, short_name: str, auth_fields: dict) -> Any:
        self._calls.append(("validate_auth", short_name, auth_fields))
        if self._should_raise:
            raise self._should_raise
        if short_name not in self._auth_results:
            raise HTTPException(status_code=404, detail=f"Source '{short_name}' not found")
        return self._auth_results[short_name]
