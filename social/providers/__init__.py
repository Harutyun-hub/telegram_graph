from __future__ import annotations

from functools import lru_cache

from social.contracts import normalize_provider_key
from social.providers.base import SocialProviderAdapter, SocialProviderError
from social.providers.scrapecreators import ScrapeCreatorsClient


@lru_cache(maxsize=8)
def get_provider_adapter(provider_key: str) -> SocialProviderAdapter:
    normalized = normalize_provider_key(provider_key)
    if normalized == "scrapecreators":
        return ScrapeCreatorsClient()
    raise ValueError(f"Unsupported social provider adapter: {provider_key}")


__all__ = [
    "SocialProviderAdapter",
    "SocialProviderError",
    "ScrapeCreatorsClient",
    "get_provider_adapter",
]
