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
    video_details = payload.get("videoDetails") if isinstance(payload.get("videoDetails"), dict) else {}
    for image in snapshot.get("images") or []:
        if isinstance(image, dict):
            _push("image", _coalesce(image.get("original_image_url"), image.get("url")))
    for video in snapshot.get("videos") or []:
        if isinstance(video, dict):
            _push("video", _coalesce(video.get("video_hd_url"), video.get("url")))
    _push("image", _coalesce(payload.get("image_url"), payload.get("thumbnail_url")))
    _push("video", _coalesce(video_details.get("hdUrl"), video_details.get("sdUrl")))

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
            "facebook": {"target_type": "page_id", "content_types": ["post"]},
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

    def fetch_facebook_profile_posts(
        self,
        *,
        page_id: str | None = None,
        url: str | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return self._get(
            "/v1/facebook/profile/posts",
            {
                "pageId": page_id,
                "url": url,
                "cursor": cursor,
            },
        )

    def fetch_facebook_post(self, *, url: str, get_comments: bool = False) -> dict[str, Any]:
        return self._get(
            "/v1/facebook/post",
            {
                "url": url,
                "get_comments": "true" if get_comments else "false",
            },
        )

    def fetch_facebook_post_comments(
        self,
        *,
        url: str | None = None,
        feedback_id: str | None = None,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return self._get(
            "/v1/facebook/post/comments",
            {
                "url": url,
                "feedback_id": feedback_id,
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

    @staticmethod
    def _provider_metadata(source: dict[str, Any]) -> dict[str, Any]:
        payload = source.get("provider_metadata")
        return dict(payload) if isinstance(payload, dict) else {}

    def _provider_int(self, source: dict[str, Any], key: str, default: int, *, minimum: int = 0) -> int:
        metadata = self._provider_metadata(source)
        raw = metadata.get(key, default)
        try:
            value = int(raw)
        except Exception:
            value = int(default)
        return max(minimum, value)

    def _provider_bool(self, source: dict[str, Any], key: str, default: bool = False) -> bool:
        metadata = self._provider_metadata(source)
        raw = metadata.get(key, default)
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _source_metadata(source: dict[str, Any]) -> dict[str, Any]:
        payload = source.get("metadata")
        return dict(payload) if isinstance(payload, dict) else {}

    def _facebook_page_url(self, source: dict[str, Any]) -> str | None:
        metadata = self._source_metadata(source)
        return _coalesce(
            metadata.get("page_url"),
            metadata.get("url"),
            source.get("page_url"),
            source.get("url"),
        )

    def collect_source(
        self,
        source: dict[str, Any],
        *,
        max_pages: int,
        page_size: int,
    ) -> list[dict[str, Any]]:
        self.validate_source(source)
        platform = normalize_platform(source.get("platform"))
        if platform != "facebook":
            return super().collect_source(source, max_pages=max_pages, page_size=page_size)

        max_post_pages = self._provider_int(source, "max_post_pages", max_pages, minimum=1)
        max_posts = self._provider_int(source, "max_posts", 0, minimum=0)
        include_comments = self._provider_bool(source, "include_comments", False)
        pages = self.collect_pages(
            source,
            max_pages=max(1, min(max_pages, max_post_pages)),
            page_size=page_size,
        )

        activities: list[dict[str, Any]] = []
        seen_activity_uids: set[str] = set()
        normalized_posts: list[dict[str, Any]] = []
        for page_index, collected_page in enumerate(pages):
            for post in self.normalize_page(source, collected_page, page_index=page_index):
                if post["activity_uid"] in seen_activity_uids:
                    continue
                seen_activity_uids.add(post["activity_uid"])
                normalized_posts.append(post)
                if max_posts and len(normalized_posts) >= max_posts:
                    break
            if max_posts and len(normalized_posts) >= max_posts:
                break

        for post in normalized_posts:
            hydrated_post = self._hydrate_facebook_post(source, post) if include_comments else post
            activities.append(hydrated_post)
            if not include_comments:
                continue
            for comment in self._collect_facebook_comments_for_post(source, hydrated_post):
                if comment["activity_uid"] in seen_activity_uids:
                    continue
                seen_activity_uids.add(comment["activity_uid"])
                activities.append(comment)
        return activities

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
                payload = self.fetch_facebook_profile_posts(page_id=identifier or "", cursor=cursor)
                request_params["pageId"] = identifier
                rows = payload.get("posts") or payload.get("results") or payload.get("items") or []
                page_url = self._facebook_page_url(source)
                if not rows and page_url:
                    payload = self.fetch_facebook_profile_posts(url=page_url, cursor=cursor)
                    rows = payload.get("posts") or payload.get("results") or payload.get("items") or []
                    if rows:
                        request_params["url"] = page_url
                        request_params["pageId_fallback_empty"] = True
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

    def _hydrate_facebook_post(self, source: dict[str, Any], activity: dict[str, Any]) -> dict[str, Any]:
        post_url = _trimmed(activity.get("source_url"))
        if not post_url:
            return activity
        detail = self.fetch_facebook_post(url=post_url, get_comments=False)
        if not isinstance(detail, dict):
            return activity
        feedback_id = _coalesce(detail.get("feedback_id"))
        engagement = normalize_engagement_metrics(detail)
        provider_context = dict(activity.get("provider_context") or {})
        if feedback_id:
            provider_context["feedback_id"] = feedback_id
        provider_context["detail_endpoint"] = "/v1/facebook/post"

        provider_payload = activity.get("provider_payload")
        if isinstance(provider_payload, dict):
            merged_payload = {
                "profile_post": provider_payload,
                "post_detail": detail,
            }
        else:
            merged_payload = {"post_detail": detail}

        hydrated = dict(activity)
        hydrated["provider_context"] = provider_context
        hydrated["provider_payload"] = merged_payload
        if not hydrated.get("text_content"):
            hydrated["text_content"] = _clean_text(_coalesce(detail.get("description"), detail.get("text")))
        if not hydrated.get("published_at"):
            hydrated["published_at"] = _to_iso_datetime(detail.get("creation_time"))
        if hydrated.get("author_handle") in (None, "", source.get("account_external_id")):
            author = detail.get("author") if isinstance(detail.get("author"), dict) else {}
            hydrated["author_handle"] = _coalesce(author.get("name"), hydrated.get("author_handle"))
        if sum(int(value or 0) for value in engagement.values()) > 0:
            hydrated["engagement_metrics"] = engagement
        assets = list(hydrated.get("assets") or [])
        for asset in _collect_assets(detail):
            if asset not in assets:
                assets.append(asset)
        hydrated["assets"] = assets
        return hydrated

    def _collect_facebook_comments_for_post(
        self,
        source: dict[str, Any],
        post_activity: dict[str, Any],
    ) -> list[dict[str, Any]]:
        feedback_id = _coalesce((post_activity.get("provider_context") or {}).get("feedback_id"))
        post_url = _trimmed(post_activity.get("source_url"))
        use_feedback_id = self._provider_bool(source, "use_feedback_id_for_comments", True)
        max_comment_pages = self._provider_int(source, "max_comment_pages_per_post", 1, minimum=1)
        max_comments = self._provider_int(source, "max_comments_per_post", 0, minimum=0)
        cursor = None
        comments: list[dict[str, Any]] = []

        for comment_page_index in range(max_comment_pages):
            payload = self.fetch_facebook_post_comments(
                feedback_id=feedback_id if use_feedback_id else None,
                url=None if feedback_id and use_feedback_id else post_url,
                cursor=cursor,
            )
            rows = payload.get("comments") or payload.get("items") or payload.get("results") or []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                normalized = self._normalize_facebook_comment_row(
                    source=source,
                    post_activity=post_activity,
                    row=row,
                    page_index=comment_page_index,
                    cursor=cursor,
                    feedback_id=feedback_id,
                )
                if normalized:
                    comments.append(normalized)
                if max_comments and len(comments) >= max_comments:
                    return comments[:max_comments]
            cursor = self._extract_next_cursor(payload)
            if not cursor or not self._has_more(payload):
                break
        return comments[:max_comments] if max_comments else comments

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
            rows = payload.get("posts") or payload.get("results") or payload.get("items") or []
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

    def _normalize_facebook_comment_row(
        self,
        *,
        source: dict[str, Any],
        post_activity: dict[str, Any],
        row: dict[str, Any],
        page_index: int,
        cursor: str | None,
        feedback_id: str | None,
    ) -> dict[str, Any] | None:
        source_key = _trimmed(source.get("source_key"))
        provider_item_id = _coalesce(row.get("id"))
        if not source_key or not provider_item_id:
            return None
        author = row.get("author") if isinstance(row.get("author"), dict) else {}
        parent_provider_item_id = _trimmed(post_activity.get("provider_item_id"))
        parent_activity_uid = _trimmed(post_activity.get("activity_uid"))
        return {
            "entity_id": source.get("entity_id") or (source.get("entity") or {}).get("id"),
            "account_id": source.get("id"),
            "activity_uid": build_activity_uid(
                provider_key=self.provider_key,
                platform="facebook",
                source_key=source_key,
                provider_item_id=provider_item_id,
                source_kind="comment",
            ),
            "provider_key": self.provider_key,
            "source_key": source_key,
            "platform": "facebook",
            "source_kind": "comment",
            "provider_item_id": provider_item_id,
            "parent_provider_item_id": parent_provider_item_id or None,
            "parent_activity_uid": parent_activity_uid or None,
            "source_url": _trimmed(post_activity.get("source_url")) or parent_activity_uid or provider_item_id,
            "text_content": _clean_text(row.get("text")) or None,
            "published_at": _to_iso_datetime(row.get("created_at")),
            "author_handle": _coalesce(author.get("name"), author.get("short_name")),
            "cta_type": None,
            "content_format": "comment",
            "region_name": None,
            "engagement_metrics": normalize_engagement_metrics(row),
            "assets": [],
            "provider_context": {
                "provider": self.provider_key,
                "platform": "facebook",
                "target_type": normalize_target_type(source.get("target_type"), platform="facebook"),
                "page_index": page_index,
                "comment_cursor": cursor,
                "feedback_id": feedback_id,
                "parent_provider_item_id": parent_provider_item_id or None,
                "parent_activity_uid": parent_activity_uid or None,
                "normalization_version": config.SOCIAL_NORMALIZATION_VERSION,
            },
            "provider_payload": row,
            "normalization_version": config.SOCIAL_NORMALIZATION_VERSION,
            "ingest_status": "normalized",
        }

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
            row.get("post_id"),
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
                row.get("description"),
                row.get("caption"),
                snapshot,
                row,
            )
        )
        published_at = _to_iso_datetime(
            _coalesce(
                row.get("published_at"),
                row.get("created_at"),
                row.get("creation_time"),
                row.get("publishTime"),
                row.get("taken_at"),
                row.get("start_date_string"),
                row.get("start_date"),
            )
        )
        author = row.get("author") if isinstance(row.get("author"), dict) else {}
        author_handle = _coalesce(
            author.get("name"),
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
            "video" if isinstance(row.get("videoDetails"), dict) else None,
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
            "parent_provider_item_id": None,
            "parent_activity_uid": None,
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
