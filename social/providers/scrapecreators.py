from __future__ import annotations

import json
import ssl
from datetime import datetime, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from loguru import logger

import config
from social.contracts import (
    DEFAULT_CONTENT_TYPES_BY_PLATFORM,
    DEFAULT_TARGET_TYPE_BY_PLATFORM,
    build_activity_uid,
    build_source_key,
    clean_optional,
    identifier_from_source,
    normalize_content_types,
    normalize_engagement_metrics,
    normalize_platform,
    normalize_provider_key,
    normalize_target_type,
)
from social.providers.base import SocialProviderAdapter, SocialProviderError

try:
    import certifi
except Exception:  # pragma: no cover
    certifi = None  # type: ignore[assignment]

BASE_URL = "https://api.scrapecreators.com"


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        if (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]")):
            try:
                return _clean_text(json.loads(text))
            except Exception:
                return text
        return text
    if isinstance(value, dict):
        for key in (
            "text",
            "body",
            "caption",
            "description",
            "title",
            "message",
            "ad_text",
            "link_description",
            "snapshot",
            "snapshot_data",
        ):
            text = _clean_text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        for item in value[:4]:
            text = _clean_text(item)
            if text:
                return text
        return ""
    return str(value).strip()


def _to_iso_datetime(value: Any) -> str | None:
    text = _trimmed(value)
    if not text:
        return None
    candidates = [text]
    if len(text) == 10 and text.count("-") == 2:
        candidates.append(f"{text}T00:00:00+00:00")
    for candidate in candidates:
        try:
            parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except Exception:
            continue
    return None


def _coalesce(*values: Any) -> str | None:
    for value in values:
        text = _trimmed(value)
        if text:
            return text
    return None


def _collect_assets(payload: dict[str, Any]) -> list[dict[str, Any]]:
    assets: list[dict[str, Any]] = []

    def _push(kind: str, url: str | None) -> None:
        if not url:
            return
        assets.append({"kind": kind, "url": url})

    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    for image in snapshot.get("images") or []:
        if isinstance(image, dict):
            _push("image", _coalesce(image.get("original_image_url"), image.get("url")))
    for video in snapshot.get("videos") or []:
        if isinstance(video, dict):
            _push("video", _coalesce(video.get("video_hd_url"), video.get("url")))

    _push("image", _coalesce(payload.get("ad_image_url"), payload.get("display_uri"), payload.get("image_url")))
    _push("profile", _coalesce(payload.get("profile_pic_url")))
    _push("landing", _coalesce(payload.get("ad_link_url"), payload.get("link_url")))
    return assets


class ScrapeCreatorsClient(SocialProviderAdapter):
    provider_key = "scrapecreators"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = _trimmed(api_key or config.SCRAPECREATORS_API_KEY)
        if not self.api_key:
            raise RuntimeError("SCRAPECREATORS_API_KEY is not configured")
        self._ssl_context = self._build_ssl_context()

    def capabilities(self) -> dict[str, Any]:
        return {
            "facebook": {"target_type": "page_id", "content_types": ["ad"]},
            "instagram": {"target_type": "handle", "content_types": ["post"]},
            "google": {"target_type": "domain", "content_types": ["ad"]},
            "tiktok": {"target_type": "handle", "content_types": ["video"]},
        }

    @staticmethod
    def _build_ssl_context():
        if certifi is None:
            return None
        try:
            return ssl.create_default_context(cafile=certifi.where())
        except Exception:
            return None

    def validate_source(self, source: dict[str, Any]) -> None:
        provider_key = normalize_provider_key(source.get("provider_key"))
        if provider_key != self.provider_key:
            raise ValueError(f"Unsupported provider for ScrapeCreators adapter: {provider_key}")
        platform = normalize_platform(source.get("platform"))
        target_type = normalize_target_type(source.get("target_type"), platform=platform)
        content_types = normalize_content_types(source.get("content_types"), platform=platform)
        identifier = identifier_from_source(source)
        if not identifier:
            raise SocialProviderError("Source identifier is required", health_status="invalid_identifier")
        expected = self.capabilities()[platform]
        if target_type != expected["target_type"]:
            raise SocialProviderError(
                f"ScrapeCreators expects {expected['target_type']} for {platform}",
                health_status="invalid_identifier",
            )
        if content_types != expected["content_types"]:
            raise SocialProviderError(
                f"ScrapeCreators expects {expected['content_types']} for {platform}",
                health_status="invalid_identifier",
            )
        if platform == "tiktok" and not config.SOCIAL_TIKTOK_ENABLED:
            raise SocialProviderError("TikTok collection is disabled", health_status="invalid_identifier")

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        query = {key: value for key, value in params.items() if value not in (None, "", [])}
        url = f"{BASE_URL}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        request = Request(
            url,
            headers={
                "x-api-key": self.api_key,
                "Accept": "application/json",
            },
            method="GET",
        )
        try:
            with urlopen(
                request,
                timeout=max(10, int(config.AI_REQUEST_TIMEOUT_SECONDS)),
                context=self._ssl_context,
            ) as response:
                raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
        except HTTPError as exc:
            status_code = int(getattr(exc, "code", 0) or 0)
            if status_code == 404:
                health_status = "provider_404"
            elif status_code == 429:
                health_status = "rate_limited"
            elif status_code in {401, 403}:
                health_status = "auth_error"
            else:
                health_status = "network_error"
            raise SocialProviderError(
                f"ScrapeCreators HTTP {status_code} for {path}",
                health_status=health_status,
                status_code=status_code,
            ) from exc
        except (URLError, TimeoutError, ssl.SSLError) as exc:
            raise SocialProviderError(
                f"ScrapeCreators network error for {path}: {exc}",
                health_status="network_error",
            ) from exc

    def fetch_facebook_ads(self, *, page_id: str, cursor: str | None = None, page_size: int = 50) -> dict[str, Any]:
        return self._get(
            "/v1/facebook/adlibrary/company/ads",
            {
                "pageId": page_id,
                "count": page_size,
                "cursor": cursor,
            },
        )

    def fetch_instagram_posts(self, *, handle: str, cursor: str | None = None, page_size: int = 50) -> dict[str, Any]:
        return self._get(
            "/v2/instagram/user/posts",
            {
                "handle": handle,
                "count": page_size,
                "cursor": cursor,
            },
        )

    def fetch_google_ads(
        self,
        *,
        domain: str,
        cursor: str | None = None,
        page_size: int = 50,
        get_ad_details: bool = False,
    ) -> dict[str, Any]:
        return self._get(
            "/v1/google/company/ads",
            {
                "domain": domain,
                "count": page_size,
                "cursor": cursor,
                "get_ad_details": "true" if get_ad_details else "false",
            },
        )

    def fetch_tiktok_videos(self, *, handle: str, cursor: str | None = None, page_size: int = 50) -> dict[str, Any]:
        return self._get(
            "/v3/tiktok/profile/videos",
            {
                "handle": handle,
                "count": page_size,
                "cursor": cursor,
            },
        )

    def collect_account(
        self,
        account: dict[str, Any],
        *,
        max_pages: int,
        page_size: int,
        include_tiktok: bool,
    ) -> list[dict[str, Any]]:
        source = dict(account)
        source.setdefault("provider_key", self.provider_key)
        source.setdefault("target_type", DEFAULT_TARGET_TYPE_BY_PLATFORM.get(source.get("platform")))
        identifier = identifier_from_source(source)
        if identifier and not source.get("source_key"):
            source["source_key"] = build_source_key(
                provider_key=source.get("provider_key"),
                platform=source.get("platform"),
                target_type=source.get("target_type"),
                identifier=identifier,
            )
        if include_tiktok:
            source.setdefault("content_types", DEFAULT_CONTENT_TYPES_BY_PLATFORM.get(source.get("platform"), []))
        return [page["payload"] for page in self.collect_pages(source, max_pages=max_pages, page_size=page_size)]

    def normalize_payloads(self, account: dict[str, Any], payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        source = dict(account)
        source.setdefault("provider_key", self.provider_key)
        source.setdefault("target_type", DEFAULT_TARGET_TYPE_BY_PLATFORM.get(source.get("platform")))
        source.setdefault("content_types", DEFAULT_CONTENT_TYPES_BY_PLATFORM.get(source.get("platform"), []))
        identifier = identifier_from_source(source)
        if identifier and not source.get("source_key"):
            source["source_key"] = build_source_key(
                provider_key=source.get("provider_key"),
                platform=source.get("platform"),
                target_type=source.get("target_type"),
                identifier=identifier,
            )
        return [
            activity
            for page_index, payload in enumerate(payloads)
            for activity in self.normalize_page(
                source,
                {
                    "payload": payload,
                    "context": {
                        "provider": self.provider_key,
                        "platform": normalize_platform(source.get("platform")),
                        "target_type": normalize_target_type(source.get("target_type"), platform=source.get("platform")),
                        "page_index": page_index,
                        "request_params": {"count": None},
                        "normalization_version": config.SOCIAL_NORMALIZATION_VERSION,
                    },
                },
                page_index=page_index,
            )
        ]

    def collect_pages(
        self,
        source: dict[str, Any],
        *,
        max_pages: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        self.validate_source(source)
        platform = normalize_platform(source.get("platform"))
        target_type = normalize_target_type(source.get("target_type"), platform=platform)
        identifier = identifier_from_source(source)
        cursor = None
        pages: list[dict[str, Any]] = []

        for page_index in range(max(1, max_pages)):
            request_params = {"count": page_size}
            if platform == "facebook":
                payload = self.fetch_facebook_ads(page_id=identifier or "", cursor=cursor, page_size=page_size)
                request_params["pageId"] = identifier
            elif platform == "instagram":
                payload = self.fetch_instagram_posts(handle=identifier or "", cursor=cursor, page_size=page_size)
                request_params["handle"] = identifier
            elif platform == "google":
                payload = self.fetch_google_ads(domain=identifier or "", cursor=cursor, page_size=page_size, get_ad_details=False)
                request_params["domain"] = identifier
            elif platform == "tiktok":
                payload = self.fetch_tiktok_videos(handle=identifier or "", cursor=cursor, page_size=page_size)
                request_params["handle"] = identifier
            else:  # pragma: no cover - guarded by validate_source
                raise ValueError(f"Unsupported ScrapeCreators platform: {platform}")

            pages.append(
                {
                    "payload": payload,
                    "context": {
                        "provider": self.provider_key,
                        "platform": platform,
                        "target_type": target_type,
                        "page_index": page_index,
                        "request_params": request_params,
                        "normalization_version": config.SOCIAL_NORMALIZATION_VERSION,
                    },
                }
            )
            cursor = self._extract_next_cursor(payload)
            if not cursor or not self._has_more(payload):
                break
            logger.info("Social collect pagination | provider={} platform={} page={}", self.provider_key, platform, page_index + 1)

        return pages

    def normalize_page(
        self,
        source: dict[str, Any],
        collected_page: dict[str, Any],
        *,
        page_index: int,
    ) -> list[dict[str, Any]]:
        platform = normalize_platform(source.get("platform"))
        payload = collected_page.get("payload") if isinstance(collected_page, dict) else {}
        context = dict(collected_page.get("context") or {}) if isinstance(collected_page, dict) else {}
        entity = source.get("entity") or {}
        rows: list[dict[str, Any]]
        if platform == "facebook":
            rows = payload.get("results") or payload.get("items") or []
        elif platform == "instagram":
            rows = payload.get("items") or payload.get("posts") or payload.get("results") or []
        elif platform == "google":
            rows = payload.get("results") or payload.get("items") or payload.get("ads") or []
        else:
            rows = payload.get("videos") or payload.get("items") or payload.get("results") or []

        activities: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            normalized = self._normalize_activity_row(
                source=source,
                entity=entity,
                row=row,
                page_index=page_index,
                context=context,
            )
            if normalized:
                activities.append(normalized)
        return activities

    @staticmethod
    def _extract_next_cursor(payload: dict[str, Any]) -> str | None:
        for key in ("next_cursor", "nextCursor", "cursor"):
            value = _coalesce(payload.get(key))
            if value:
                return value
        page_info = payload.get("page_info")
        if isinstance(page_info, dict):
            return _coalesce(page_info.get("end_cursor"))
        return None

    @staticmethod
    def _has_more(payload: dict[str, Any]) -> bool:
        for key in ("has_more", "has_next_page", "hasNextPage"):
            value = payload.get(key)
            if isinstance(value, bool):
                return value
        page_info = payload.get("page_info")
        if isinstance(page_info, dict) and isinstance(page_info.get("has_next_page"), bool):
            return bool(page_info.get("has_next_page"))
        return False

    def _normalize_activity_row(
        self,
        *,
        source: dict[str, Any],
        entity: dict[str, Any],
        row: dict[str, Any],
        page_index: int,
        context: dict[str, Any],
    ) -> dict[str, Any] | None:
        platform = normalize_platform(source.get("platform"))
        provider_item_id = _coalesce(
            row.get("id"),
            row.get("pk"),
            row.get("ad_id"),
            row.get("adArchiveId"),
            row.get("creativeId"),
            row.get("creative_id"),
            row.get("url"),
        )
        source_url = _coalesce(
            row.get("url"),
            row.get("link_url"),
            row.get("ad_link_url"),
            row.get("page_profile_uri"),
            row.get("permalink"),
        )
        if not source_url:
            source_url = provider_item_id
        if not source_url:
            return None

        source_kind = DEFAULT_CONTENT_TYPES_BY_PLATFORM[platform][0]
        if not provider_item_id:
            provider_item_id = source_url

        snapshot = row.get("snapshot")
        text_content = _clean_text(
            _coalesce(
                row.get("text"),
                row.get("ad_text"),
                row.get("caption"),
                row.get("description"),
                snapshot,
                row,
            )
        )
        published_at = _to_iso_datetime(
            _coalesce(
                row.get("published_at"),
                row.get("created_at"),
                row.get("taken_at"),
                row.get("start_date_string"),
                row.get("start_date"),
            )
        )
        author_handle = _coalesce(
            row.get("username"),
            row.get("handle"),
            row.get("page_name"),
            source.get("account_handle"),
            source.get("account_external_id"),
            source.get("domain"),
        )
        cta_type = _coalesce(
            row.get("cta"),
            row.get("ad_cta_type"),
            row.get("cta_type"),
            (snapshot or {}).get("cta_type") if isinstance(snapshot, dict) else None,
        )
        content_format = _coalesce(
            row.get("display_format"),
            row.get("ad_display_format"),
            row.get("format"),
            row.get("media_type"),
            (snapshot or {}).get("display_format") if isinstance(snapshot, dict) else None,
        )
        region_name = _coalesce(row.get("region_name"), row.get("location"), row.get("country"))
        source_key = _trimmed(source.get("source_key"))
        provider_context = {
            "provider": self.provider_key,
            "platform": platform,
            "target_type": normalize_target_type(source.get("target_type"), platform=platform),
            "page_index": page_index,
            "request_params": dict(context.get("request_params") or {}),
            "normalization_version": config.SOCIAL_NORMALIZATION_VERSION,
        }

        return {
            "entity_id": entity.get("id") or source.get("entity_id"),
            "account_id": source.get("id"),
            "activity_uid": build_activity_uid(
                provider_key=self.provider_key,
                platform=platform,
                source_key=source_key,
                provider_item_id=provider_item_id,
                source_kind=source_kind,
            ),
            "provider_key": self.provider_key,
            "source_key": source_key,
            "platform": platform,
            "source_kind": source_kind,
            "provider_item_id": provider_item_id,
            "source_url": source_url,
            "text_content": text_content or None,
            "published_at": published_at,
            "author_handle": author_handle,
            "cta_type": cta_type,
            "content_format": content_format,
            "region_name": region_name,
            "engagement_metrics": normalize_engagement_metrics(row),
            "assets": _collect_assets(row),
            "provider_context": provider_context,
            "provider_payload": row,
            "normalization_version": config.SOCIAL_NORMALIZATION_VERSION,
            "ingest_status": "normalized",
        }


SocialCollectionError = SocialProviderError
