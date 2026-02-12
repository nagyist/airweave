"""Source service."""

from fastapi import HTTPException

from airweave import schemas
from airweave.api.context import ApiContext
from airweave.core.protocols.repositories import SourceRepositoryProtocol
from airweave.core.protocols.sources import SourceServiceProtocol
from airweave.db.session_factory import DBSessionFactory
from airweave.platform.locator import ResourceLocator


class SourceService(SourceServiceProtocol):
    """Service for managing sources."""

    def __init__(
        self,
        source_repository: SourceRepositoryProtocol,
        resource_locator: ResourceLocator,
        db_session_factory: DBSessionFactory,
    ):
        """Initialize the source service."""
        self.source_repository = source_repository
        self.resource_locator = resource_locator
        self.db_session_factory = db_session_factory

    async def get(self, short_name: str) -> schemas.Source:
        """Get a source by short name."""
        async with self.db_session_factory.get_db_session() as db:
            pass

        raise NotImplementedError

    async def list(self, ctx: ApiContext) -> list[schemas.Source]:
        """List all sources."""
        ctx.logger.info("Starting read_sources endpoint")
        try:
            async with self.db_session_factory.get_db_session() as db_session:
                sources = await self.source_repository.get_multi(
                    db_session=db_session,
                )
            ctx.logger.info(f"Retrieved {len(sources)} sources from database")
        except Exception as e:
            ctx.logger.error(f"Failed to retrieve sources: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to retrieve sources") from e

        # Initialize auth_fields for each source
        result_sources = []
        invalid_sources = []
        enabled_features = ctx.organization.enabled_features or []

        for source in sources:
            try:
                # Filter sources by feature flag at source level
                if source.feature_flag:
                    from airweave.core.shared_models import FeatureFlag as FeatureFlagEnum

                    try:
                        required_flag = FeatureFlagEnum(source.feature_flag)
                        if required_flag not in enabled_features:
                            ctx.logger.debug(
                                f"🚫 Hidden source {source.short_name} "
                                f"(requires feature flag: {source.feature_flag})"
                            )
                            continue  # Skip this source
                    except ValueError:
                        ctx.logger.warning(
                            f"Source {source.short_name} has invalid flag {source.feature_flag}"
                        )
                        # Continue processing if flag is invalid (fail open)
                # Config class is always required
                if not source.config_class:
                    invalid_sources.append(f"{source.short_name} (missing config_class)")
                    continue

                # Auth config class is only required for sources with DIRECT auth
                # OAuth sources don't have auth_config_class
                auth_fields = None
                if source.auth_config_class:
                    # Get authentication configuration class if it exists
                    try:
                        auth_config_class = self.resource_locator.get_auth_config(
                            source.auth_config_class
                        )
                        auth_fields = Fields.from_config_class(auth_config_class)
                    except AttributeError as e:
                        invalid_sources.append(
                            f"{source.short_name} (invalid auth_config_class: {str(e)})"
                        )
                        continue
                else:
                    # For OAuth sources, auth_fields is None (handled by OAuth flow)
                    auth_fields = Fields(fields=[])

                # Get configuration class
                try:
                    config_class = self.resource_locator.get_config(source.config_class)
                    config_fields_unfiltered = Fields.from_config_class(config_class)

                    # Filter config fields based on organization's enabled features
                    config_fields = config_fields_unfiltered.filter_by_features(enabled_features)

                    # Log any fields that were filtered out due to missing feature flags
                    filtered_out = [
                        f.name
                        for f in config_fields_unfiltered.fields
                        if f.feature_flag and f.feature_flag not in enabled_features
                    ]
                    if filtered_out:
                        ctx.logger.debug(
                            f"🚫 Hidden config fields for {source.short_name} "
                            f"(feature flags not enabled): {filtered_out}"
                        )
                except AttributeError as e:
                    invalid_sources.append(f"{source.short_name} (invalid config_class: {str(e)})")
                    continue

                # Get supported auth providers
                supported_auth_providers = auth_provider_service.get_supported_providers_for_source(
                    source.short_name
                )

                # Create source model with all fields including auth_fields and config_fields
                source_dict = {
                    **{
                        key: getattr(source, key)
                        for key in source.__dict__
                        if not key.startswith("_")
                    },
                    "auth_fields": auth_fields,
                    "config_fields": config_fields,
                    "supported_auth_providers": supported_auth_providers,
                }

                # In self-hosted mode, force requires_byoc for OAuth sources
                if settings.ENVIRONMENT == "self-hosted" and source.auth_methods:
                    if (
                        "oauth_browser" in source.auth_methods
                        or "oauth_token" in source.auth_methods
                    ):
                        source_dict["requires_byoc"] = True

                source_model = schemas.Source.model_validate(source_dict)
                result_sources.append(source_model)

            except Exception as e:
                # Log the error but continue processing other sources
                ctx.logger.exception(f"Error processing source {source.short_name}: {str(e)}")
                invalid_sources.append(f"{source.short_name} (error: {str(e)})")

        # Log any invalid sources
        if invalid_sources:
            ctx.logger.warning(
                f"Skipped {len(invalid_sources)} invalid sources: {', '.join(invalid_sources)}"
            )
            pass

        # ctx.logger.info(f"Returning {len(result_sources)} valid sources")
        return result_sources
