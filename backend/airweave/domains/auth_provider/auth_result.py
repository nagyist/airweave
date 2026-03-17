"""Auth result types for explicit auth mode communication."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class AuthProviderMode(Enum):
    """Explicit auth mode enumeration."""

    DIRECT = "direct"  # Use credentials directly


@dataclass
class AuthResult:
    """Result of auth provider credential fetch.

    source_config carries non-secret config fields (e.g., instance_url) that
    the auth provider extracted alongside credentials.
    """

    mode: AuthProviderMode
    credentials: Optional[Any] = None
    source_config: Optional[Dict[str, Any]] = None

    @classmethod
    def direct(cls, credentials: Any) -> "AuthResult":
        """Create a direct auth result with credentials."""
        return cls(mode=AuthProviderMode.DIRECT, credentials=credentials)
