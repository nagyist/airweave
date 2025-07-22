"""Configuration classes for platform components."""

from pydantic import Field, validator

from airweave.platform.configs._base import BaseConfig


class SourceConfig(BaseConfig):
    """Source config schema."""

    pass


class AsanaConfig(SourceConfig):
    """Asana configuration schema."""

    pass


class BitbucketConfig(SourceConfig):
    """Bitbucket configuration schema."""

    branch: str = Field(
        default="",
        title="Branch name",
        description=(
            "Specific branch to sync (e.g., 'main', 'develop'). If empty, uses the default branch."
        ),
    )
    file_extensions: list[str] = Field(
        default=[],
        title="File Extensions",
        description=(
            "List of file extensions to include (e.g., '.py', '.js', '.md'). "
            "If empty, includes all text files. Use '.*' to include all files."
        ),
    )

    @validator("file_extensions", pre=True)
    def parse_file_extensions(cls, value):
        """Convert string input to list if needed."""
        if isinstance(value, str):
            if not value.strip():
                return []
            # Split by commas and strip whitespace
            return [ext.strip() for ext in value.split(",") if ext.strip()]
        return value


class ClickUpConfig(SourceConfig):
    """ClickUp configuration schema."""

    pass


class ConfluenceConfig(SourceConfig):
    """Confluence configuration schema."""

    pass


class DropboxConfig(SourceConfig):
    """Dropbox configuration schema."""


class ElasticsearchConfig(SourceConfig):
    """Elasticsearch configuration schema."""

    pass


class GitHubConfig(SourceConfig):
    """Github configuration schema."""

    branch: str = Field(
        default="",
        title="Branch name",
        description=(
            "Specific branch to sync (e.g., 'main', 'development'). "
            "If empty, uses the default branch."
        ),
    )


class GmailConfig(SourceConfig):
    """Gmail configuration schema."""

    pass


class GoogleCalendarConfig(SourceConfig):
    """Google Calendar configuration schema."""

    pass


class GoogleDriveConfig(SourceConfig):
    """Google Drive configuration schema."""

    exclude_patterns: list[str] = Field(
        default=[],
        title="Exclude Patterns",
        description=(
            "List of file/folder paths or patterns to exclude from synchronization. "
            "Examples: '*.tmp', 'Private/*', 'Confidential Reports/'. "
            "Separate multiple patterns with commas."
        ),
    )

    @validator("exclude_patterns", pre=True)
    def parse_exclude_patterns(cls, value):
        """Convert string input to list if needed."""
        if isinstance(value, str):
            if not value.strip():
                return []
            # Split by commas and strip whitespace
            return [pattern.strip() for pattern in value.split(",") if pattern.strip()]
        return value


class HubspotConfig(SourceConfig):
    """Hubspot configuration schema."""

    pass


class IntercomConfig(SourceConfig):
    """Intercom configuration schema."""

    pass


class JiraConfig(SourceConfig):
    """Jira configuration schema."""

    pass


class LinearConfig(SourceConfig):
    """Linear configuration schema."""

    pass


class MondayConfig(SourceConfig):
    """Monday configuration schema."""

    pass


class MySQLConfig(SourceConfig):
    """MySQL configuration schema."""

    pass


class NotionConfig(SourceConfig):
    """Notion configuration schema."""

    pass


class OneDriveConfig(SourceConfig):
    """OneDrive configuration schema."""

    pass


class OracleConfig(SourceConfig):
    """Oracle configuration schema."""

    pass


class OutlookCalendarConfig(SourceConfig):
    """Outlook Calendar configuration schema."""

    pass


class OutlookMailConfig(SourceConfig):
    """Outlook Mail configuration schema."""

    pass


class CTTIConfig(SourceConfig):
    """CTTI AACT configuration schema."""

    limit: int = Field(
        default=10000,
        title="Study Limit",
        description="Maximum number of clinical trial studies to fetch from AACT database",
    )

    skip: int = Field(
        default=0,
        title="Skip Studies",
        description=(
            "Number of clinical trial studies to skip (for pagination). "
            "Use with limit to fetch different batches."
        ),
    )

    @validator("limit", pre=True)
    def parse_limit(cls, value):
        """Convert string input to integer if needed."""
        if isinstance(value, str):
            if not value.strip():
                return 10000
            try:
                return int(value.strip())
            except ValueError as e:
                raise ValueError("Limit must be a valid integer") from e
        return value

    @validator("skip", pre=True)
    def parse_skip(cls, value):
        """Convert string input to integer if needed."""
        if isinstance(value, str):
            if not value.strip():
                return 0
            try:
                skip_val = int(value.strip())
                if skip_val < 0:
                    raise ValueError("Skip must be non-negative")
                return skip_val
            except ValueError as e:
                if "non-negative" in str(e):
                    raise e
                raise ValueError("Skip must be a valid integer") from e
        if isinstance(value, (int, float)):
            if value < 0:
                raise ValueError("Skip must be non-negative")
            return int(value)
        return value


class PostgreSQLConfig(SourceConfig):
    """Postgres configuration schema."""

    pass


class SlackConfig(SourceConfig):
    """Slack configuration schema."""

    pass


class SQLServerConfig(SourceConfig):
    """SQL Server configuration schema."""

    pass


class SQliteConfig(SourceConfig):
    """SQlite configuration schema."""

    pass


class StripeConfig(SourceConfig):
    """Stripe configuration schema."""

    pass


class TodoistConfig(SourceConfig):
    """Todoist configuration schema."""

    pass


# AUTH PROVIDER CONFIGURATION CLASSES
# These are for configuring auth provider behavior


class AuthProviderConfig(BaseConfig):
    """Base auth provider configuration schema."""

    pass


class ComposioConfig(AuthProviderConfig):
    """Composio Auth Provider configuration schema."""

    integration_id: str = Field(
        title="Integration ID",
        description="Integration ID for the Composio connection (nanoid)",
    )
    account_id: str = Field(
        title="Account ID",
        description="Account ID for the Composio connection (nanoid)",
    )
