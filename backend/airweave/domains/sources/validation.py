"""Source validation service.

Validates config and auth fields using registry-resolved Pydantic schemas.
No DB calls -- uses SourceRegistryEntry.config_ref / auth_config_ref directly.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Dict

from fastapi import HTTPException

from airweave.api.context import ApiContext
from airweave.domains.sources.protocols import (
    SourceRegistryProtocol,
    SourceValidationServiceProtocol,
)


class SourceValidationService(SourceValidationServiceProtocol):
    """Validates config and auth fields against registry-resolved Pydantic schemas."""

    def __init__(self, source_registry: SourceRegistryProtocol) -> None:
        self._registry = source_registry

    def validate_config(  # noqa: C901
        self, short_name: str, config_fields: Any, ctx: ApiContext
    ) -> Dict[str, Any]:
        """Validate configuration fields against source schema, returning a plain dict.

        Also strips fields that have feature flags not enabled for the organization.
        """
        try:
            entry = self._registry.get(short_name)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Source '{short_name}' not found")

        if not config_fields:
            return {}

        config_class = entry.config_ref
        if config_class is None:
            try:
                return self._as_mapping(config_fields)
            except Exception:
                return {}

        try:
            payload = self._as_mapping(config_fields)

            enabled_features = ctx.organization.enabled_features or []

            for field_name, field_info in config_class.model_fields.items():
                json_schema_extra = field_info.json_schema_extra or {}
                feature_flag = json_schema_extra.get("feature_flag")

                if feature_flag and feature_flag not in enabled_features:
                    if field_name in payload and payload[field_name] is not None:
                        ctx.logger.warning(
                            f"Rejected config field '{field_name}' for {short_name}: "
                            f"feature flag '{feature_flag}' not enabled for organization"
                        )
                        field_title = field_info.title or field_name
                        raise HTTPException(
                            status_code=403,
                            detail=(
                                f"The '{field_title}' feature requires the '{feature_flag}' "
                                f"feature to be enabled for your organization. "
                                f"Please contact support to enable this feature."
                            ),
                        )

            if hasattr(config_class, "model_validate"):
                model = config_class.model_validate(payload)
            else:
                model = config_class(**payload)

            if hasattr(model, "model_dump"):
                return model.model_dump()
            if hasattr(model, "dict"):
                return model.dict()
            return dict(model) if isinstance(model, dict) else payload

        except Exception as e:
            from pydantic import ValidationError

            if isinstance(e, (HTTPException, ValidationError)):
                if isinstance(e, ValidationError):

                    def _loc(err: dict) -> str:
                        loc = err.get("loc", ())
                        return ".".join(str(x) for x in loc) if loc else "?"

                    errors = "; ".join([f"{_loc(err)}: {err.get('msg')}" for err in e.errors()])
                    raise HTTPException(
                        status_code=422, detail=f"Invalid config fields: {errors}"
                    ) from e
                raise
            raise HTTPException(status_code=422, detail=str(e)) from e

    def validate_auth(self, short_name: str, auth_fields: dict) -> Any:
        """Validate authentication fields against source's auth config schema."""
        try:
            entry = self._registry.get(short_name)
        except KeyError:
            raise HTTPException(status_code=404, detail=f"Source '{short_name}' not found")

        auth_config_class = entry.auth_config_ref
        if auth_config_class is None:
            raise HTTPException(
                status_code=422,
                detail=f"Source '{short_name}' does not support direct auth",
            )

        try:
            auth_config = auth_config_class(**auth_fields)
            return auth_config
        except Exception as e:
            from pydantic import ValidationError

            if isinstance(e, ValidationError):
                errors = "; ".join([f"{err['loc'][0]}: {err['msg']}" for err in e.errors()])
                raise HTTPException(status_code=422, detail=f"Invalid auth fields: {errors}") from e
            raise HTTPException(status_code=422, detail=str(e)) from e

    @staticmethod
    def _as_mapping(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, Mapping):
            return dict(value)
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if hasattr(value, "dict"):
            return value.dict()
        if hasattr(value, "values"):
            v = value.values
            if isinstance(v, Mapping):
                return dict(v)
            return v
        if isinstance(value, list) and all(
            isinstance(x, dict) and "key" in x and "value" in x for x in value
        ):
            return {x["key"]: x["value"] for x in value}
        raise TypeError(f"config_fields must be mapping-like; got {type(value).__name__}")
