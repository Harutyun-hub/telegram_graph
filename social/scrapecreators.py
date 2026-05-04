from __future__ import annotations

import json
import re
import ssl
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from loguru import logger

import config
from social.text_cleaning import extract_readable_social_text

try:
    import certifi
except Exception:  # pragma: no cover
    certifi = None  # type: ignore[assignment]

BASE_URL = "https://api.scrapecreators.com"
RECENT_POST_LIMIT = 5
FACEBOOK_PAGE_MAX_PAGES = 2
INSTAGRAM_MAX_PAGES = 3
META_ADS_MAX_PAGES = 20
GOOGLE_ADS_MAX_PAGES = 10


class SocialCollectionError(RuntimeError):
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


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clean_text(value: Any) -> str:
    return extract_readable_social_text(value)


def _to_iso_datetime(value: Any) -> str | None:
    text = _trimmed(value)
    if not text:
        return None
    if re.match(r"^[0-9]{10,13}$", text):
        try:
            timestamp = int(text)
            if len(text) == 13:
                timestamp = timestamp / 1000
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
        except Exception:
            pass
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


def _coalesce_raw(*values: Any) -> Any | None:
    for value in values:
        if isinstance(value, str):
            if value.strip():
                return value
            continue
        if value:
            return value
    return None


def _utc_yesterday_date() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def _account_source_kind(account: dict[str, Any]) -> str:
    metadata = account.get("metadata") if isinstance(account.get("metadata"), dict) else {}
    value = _trimmed(account.get("source_kind") or metadata.get("source_kind")).lower()
    if value:
        return value
    platform = _trimmed(account.get("platform")).lower()
    return {
        "facebook": "meta_ads",
        "instagram": "instagram_profile",
        "google": "google_domain",
        "tiktok": "tiktok_profile",
    }.get(platform, "post")


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


def _engagement_metrics(payload: dict[str, Any]) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for key in (
        "like_count",
        "comment_count",
        "share_count",
        "view_count",
        "play_count",
        "impression_count",
        "reactionCount",
        "commentCount",
        "videoViewCount",
        "reply_count",
        "reaction_count",
    ):
        value = payload.get(key)
        if isinstance(value, (int, float)):
            metrics[key] = value
    return metrics


class ScrapeCreatorsClient:
    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = _trimmed(api_key or config.SCRAPECREATORS_API_KEY)
        if not self.api_key:
            raise RuntimeError("SCRAPECREATORS_API_KEY is not configured")
        self._ssl_context = self._build_ssl_context()

    @staticmethod
    def _build_ssl_context():
        if certifi is None:
            return None
        try:
            return ssl.create_default_context(cafile=certifi.where())
        except Exception:
            return None

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
            raise SocialCollectionError(
                f"ScrapeCreators HTTP {status_code} for {path}",
                health_status=health_status,
                status_code=status_code,
            ) from exc
        except (URLError, TimeoutError, ssl.SSLError) as exc:
            raise SocialCollectionError(
                f"ScrapeCreators network error for {path}: {exc}",
                health_status="network_error",
            ) from exc

    def fetch_facebook_ads(
        self,
        *,
        page_id: str,
        cursor: str | None = None,
        page_size: int = 50,
        status: str = "ACTIVE",
    ) -> dict[str, Any]:
        return self._get(
            "/v1/facebook/adLibrary/company/ads",
            {
                "pageId": page_id,
                "count": page_size,
                "cursor": cursor,
                "status": status,
            },
        )

    def fetch_facebook_profile_posts(
        self,
        *,
        page_url: str | None = None,
        page_id: str | None = None,
        cursor: str | None = None,
        page_size: int = 3,
    ) -> dict[str, Any]:
        return self._get(
            "/v1/facebook/profile/posts",
            {
                "url": page_url,
                "pageId": page_id,
                "count": page_size,
                "cursor": cursor,
            },
        )

    def fetch_facebook_post_comments(
        self,
        *,
        post_url: str,
        cursor: str | None = None,
        page_size: int = 10,
    ) -> dict[str, Any]:
        return self._get(
            "/v1/facebook/post/comments",
            {
                "url": post_url,
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
                "next_max_id": cursor,
            },
        )

    def fetch_google_ads(
        self,
        *,
        domain: str,
        cursor: str | None = None,
        page_size: int = 50,
        get_ad_details: bool = False,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        return self._get(
            "/v1/google/company/ads",
            {
                "domain": domain,
                "count": page_size,
                "cursor": cursor,
                "start_date": start_date,
                "end_date": end_date,
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
        platform = _trimmed(account.get("platform")).lower()
        source_kind = _account_source_kind(account)
        cursor = None
        pages: list[dict[str, Any]] = []
        collected_recent_posts = 0
        google_day = _utc_yesterday_date() if platform == "google" else None

        for page_index in range(self._max_pages_for(platform, source_kind, max_pages)):
            if platform == "facebook":
                if source_kind == "facebook_page":
                    if collected_recent_posts >= RECENT_POST_LIMIT:
                        break
                    metadata = account.get("metadata") if isinstance(account.get("metadata"), dict) else {}
                    page_url = _trimmed(metadata.get("page_url") or metadata.get("source_url"))
                    page_id = _trimmed(account.get("account_external_id") or metadata.get("source_key"))
                    if not page_url and not page_id:
                        raise SocialCollectionError("Missing Facebook page URL or page ID", health_status="invalid_identifier")
                    payload = self.fetch_facebook_profile_posts(
                        page_url=page_url or None,
                        page_id=page_id or None,
                        cursor=cursor,
                        page_size=min(page_size, 3),
                    )
                    posts = payload.get("posts") or payload.get("items") or payload.get("results") or []
                    posts = [row for row in posts if isinstance(row, dict)]
                    selected_posts = posts[: max(0, RECENT_POST_LIMIT - collected_recent_posts)]
                    self._replace_payload_rows(payload, selected_posts, "posts", "items", "results")
                    collected_recent_posts += len(selected_posts)
                    comments_by_post: dict[str, list[dict[str, Any]]] = {}
                    for post in selected_posts:
                        post_url = _coalesce(post.get("permalink"), post.get("url"))
                        if not post_url:
                            continue
                        try:
                            comments_payload = self.fetch_facebook_post_comments(
                                post_url=post_url,
                                page_size=min(page_size, 10),
                            )
                        except SocialCollectionError as exc:
                            logger.warning("Facebook page comments skipped | url={} error={}", post_url, exc)
                            continue
                        comments = comments_payload.get("comments") or comments_payload.get("items") or comments_payload.get("results") or []
                        comments_by_post[post_url] = [row for row in comments if isinstance(row, dict)]
                    payload["comments_by_post"] = comments_by_post
                else:
                    page_id = _trimmed(account.get("account_external_id"))
                    if not page_id:
                        raise SocialCollectionError("Missing Facebook page ID", health_status="invalid_identifier")
                    payload = self.fetch_facebook_ads(
                        page_id=page_id,
                        cursor=cursor,
                        page_size=page_size,
                        status="ACTIVE",
                    )
            elif platform == "instagram":
                if collected_recent_posts >= RECENT_POST_LIMIT:
                    break
                handle = _trimmed(account.get("account_handle"))
                if not handle:
                    raise SocialCollectionError("Missing Instagram handle", health_status="invalid_identifier")
                payload = self.fetch_instagram_posts(
                    handle=handle,
                    cursor=cursor,
                    page_size=min(page_size, RECENT_POST_LIMIT),
                )
                posts = payload.get("items") or payload.get("posts") or payload.get("results") or []
                posts = [row for row in posts if isinstance(row, dict)]
                selected_posts = posts[: max(0, RECENT_POST_LIMIT - collected_recent_posts)]
                self._replace_payload_rows(payload, selected_posts, "items", "posts", "results")
                collected_recent_posts += len(selected_posts)
            elif platform == "google":
                domain = _trimmed(account.get("domain"))
                if not domain:
                    raise SocialCollectionError("Missing Google Ads domain", health_status="invalid_identifier")
                payload = self.fetch_google_ads(
                    domain=domain,
                    cursor=cursor,
                    page_size=page_size,
                    get_ad_details=False,
                    start_date=google_day,
                    end_date=google_day,
                )
            elif platform == "tiktok":
                if not include_tiktok:
                    logger.info("TikTok collection skipped because the feature flag is disabled")
                    break
                handle = _trimmed(account.get("account_handle") or account.get("account_external_id"))
                if not handle:
                    raise SocialCollectionError("Missing TikTok handle", health_status="invalid_identifier")
                payload = self.fetch_tiktok_videos(
                    handle=handle,
                    cursor=cursor,
                    page_size=page_size,
                )
            else:
                raise ValueError(f"Unsupported account platform: {platform}")

            pages.append(payload)
            if platform in {"facebook", "instagram"} and source_kind in {"facebook_page", "instagram_profile"} and collected_recent_posts >= RECENT_POST_LIMIT:
                break
            cursor = self._extract_next_cursor(payload)
            if not cursor:
                break
            if not self._has_more(payload):
                break
            logger.info("Social collect pagination | platform={} page={}", platform, page_index + 1)

        return pages

    @staticmethod
    def _max_pages_for(platform: str, source_kind: str, requested_max_pages: int) -> int:
        if platform == "facebook" and source_kind == "meta_ads":
            return META_ADS_MAX_PAGES
        if platform == "facebook" and source_kind == "facebook_page":
            return FACEBOOK_PAGE_MAX_PAGES
        if platform == "instagram":
            return INSTAGRAM_MAX_PAGES
        if platform == "google":
            return GOOGLE_ADS_MAX_PAGES
        return max(1, int(requested_max_pages))

    @staticmethod
    def _replace_payload_rows(payload: dict[str, Any], rows: list[dict[str, Any]], *keys: str) -> None:
        target_key = next((key for key in keys if isinstance(payload.get(key), list)), keys[0])
        payload[target_key] = rows

    @staticmethod
    def _extract_next_cursor(payload: dict[str, Any]) -> str | None:
        for key in ("next_cursor", "nextCursor", "next_max_id", "nextMaxId", "cursor"):
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
        return True

    def normalize_payloads(self, account: dict[str, Any], payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        platform = _trimmed(account.get("platform")).lower()
        source_kind = _account_source_kind(account)
        entity = account.get("entity") or {}
        entity_id = entity.get("id")
        account_id = account.get("id")
        seen_uids: set[str] = set()
        activities: list[dict[str, Any]] = []

        for payload in payloads:
            rows = []
            if platform == "facebook" and source_kind == "meta_ads":
                rows = payload.get("results") or payload.get("items") or []
            elif platform == "facebook" and source_kind == "facebook_page":
                rows = payload.get("posts") or payload.get("items") or payload.get("results") or []
            elif platform == "instagram":
                rows = payload.get("items") or payload.get("posts") or payload.get("results") or []
            elif platform == "google":
                rows = payload.get("results") or payload.get("items") or payload.get("ads") or []
            elif platform == "tiktok":
                rows = payload.get("videos") or payload.get("items") or payload.get("results") or []

            for row in rows:
                if not isinstance(row, dict):
                    continue
                normalized = self._normalize_activity_row(
                    platform=platform,
                    account_source_kind=source_kind,
                    entity_id=entity_id,
                    account_id=account_id,
                    account=account,
                    row=row,
                )
                if not normalized:
                    continue
                uid = normalized["activity_uid"]
                if uid in seen_uids:
                    continue
                seen_uids.add(uid)
                activities.append(normalized)
                if platform == "facebook" and source_kind == "facebook_page":
                    post_url = _coalesce(row.get("permalink"), row.get("url"))
                    comments = (payload.get("comments_by_post") or {}).get(post_url, [])
                    for comment in comments:
                        normalized_comment = self._normalize_activity_row(
                            platform=platform,
                            account_source_kind="facebook_page_comment",
                            entity_id=entity_id,
                            account_id=account_id,
                            account=account,
                            row={**comment, "__parent_post_url": post_url, "__parent_post_id": normalized.get("provider_item_id")},
                        )
                        if not normalized_comment:
                            continue
                        comment_uid = normalized_comment["activity_uid"]
                        if comment_uid in seen_uids:
                            continue
                        seen_uids.add(comment_uid)
                        activities.append(normalized_comment)
        return activities

    def _normalize_activity_row(
        self,
        *,
        platform: str,
        account_source_kind: str,
        entity_id: str,
        account_id: str,
        account: dict[str, Any],
        row: dict[str, Any],
    ) -> dict[str, Any] | None:
        provider_item_id = _coalesce(
            row.get("id"),
            row.get("pk"),
            row.get("ad_id"),
            row.get("adArchiveId"),
            row.get("creativeId"),
            row.get("creative_id"),
            row.get("url"),
            row.get("__parent_post_id"),
        )
        source_url = _coalesce(
            row.get("url"),
            row.get("adUrl"),
            row.get("ad_url"),
            row.get("link_url"),
            row.get("ad_link_url"),
            row.get("page_profile_uri"),
            row.get("permalink"),
            row.get("__parent_post_url"),
        )
        if not source_url:
            source_url = provider_item_id
        if not source_url:
            return None

        snapshot = row.get("snapshot")
        text_content = _clean_text(
            _coalesce_raw(
                row.get("text"),
                row.get("ad_text"),
                row.get("caption"),
                row.get("message"),
                row.get("description"),
                row.get("body"),
                row.get("title"),
                row.get("link_description"),
                snapshot,
            )
        )
        source_kind = {
            ("facebook", "meta_ads"): "ad",
            ("facebook", "facebook_page"): "post",
            ("facebook", "facebook_page_comment"): "comment",
            ("google", "google_domain"): "ad",
            ("instagram", "instagram_profile"): "post",
            ("tiktok", "tiktok_profile"): "video",
        }.get((platform, account_source_kind), "post")
        if not provider_item_id:
            provider_item_id = source_url

        published_at = _to_iso_datetime(
            _coalesce(
                row.get("published_at"),
                row.get("created_at"),
                row.get("taken_at"),
                row.get("lastShown"),
                row.get("last_shown"),
                row.get("start_date_string"),
                row.get("start_date"),
                row.get("firstShown"),
                row.get("first_shown"),
                row.get("publishTime"),
            )
        )
        author_handle = _coalesce(
            row.get("username"),
            row.get("handle"),
            row.get("page_name"),
            (row.get("author") or {}).get("name") if isinstance(row.get("author"), dict) else None,
            (row.get("author") or {}).get("id") if isinstance(row.get("author"), dict) else None,
            account.get("account_handle"),
            account.get("account_external_id"),
            account.get("domain"),
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

        return {
            "entity_id": entity_id,
            "account_id": account_id,
            "activity_uid": f"{platform}:{source_kind}:{provider_item_id}",
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
            "engagement_metrics": _engagement_metrics(row),
            "assets": _collect_assets(row),
            "provider_payload": row,
            "normalization_version": config.SOCIAL_ANALYSIS_PROMPT_VERSION,
            "ingest_status": "normalized",
        }
