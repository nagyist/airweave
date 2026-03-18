"""Source rate limiting domain — enforces API quota limits per source."""

from airweave.domains.sources.rate_limiting.service import SourceRateLimiter

__all__ = ["SourceRateLimiter"]
