from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class SocialProviderError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        health_status: str,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.health_status = health_status
        self.status_code = status_code


class SocialProviderAdapter(ABC):
    provider_key: str

    @abstractmethod
    def capabilities(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def validate_source(self, source: dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def collect_pages(
        self,
        source: dict[str, Any],
        *,
        max_pages: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def normalize_page(
        self,
        source: dict[str, Any],
        collected_page: dict[str, Any],
        *,
        page_index: int,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    def collect_source(
        self,
        source: dict[str, Any],
        *,
        max_pages: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        self.validate_source(source)
        collected_pages = self.collect_pages(
            source,
            max_pages=max_pages,
            page_size=page_size,
        )
        activities: list[dict[str, Any]] = []
        seen_activity_uids: set[str] = set()
        for page_index, collected_page in enumerate(collected_pages):
            for activity in self.normalize_page(source, collected_page, page_index=page_index):
                activity_uid = str(activity.get("activity_uid") or "").strip()
                if not activity_uid or activity_uid in seen_activity_uids:
                    continue
                seen_activity_uids.add(activity_uid)
                activities.append(activity)
        return activities
