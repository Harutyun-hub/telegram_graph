from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, time, timezone
from typing import Any, Iterable

from loguru import logger
from supabase import Client, create_client

import config

SUPPORTED_SOCIAL_PLATFORMS = ("facebook", "instagram", "google", "tiktok")
SUPPORTED_SOCIAL_SOURCE_KINDS = (
    "facebook_page",
    "meta_ads",
    "instagram_profile",
    "google_domain",
    "tiktok_profile",
)
DEFAULT_SOURCE_KIND_BY_PLATFORM = {
    "facebook": "meta_ads",
    "instagram": "instagram_profile",
    "google": "google_domain",
    "tiktok": "tiktok_profile",
}
ACCOUNT_HEALTH_STATUSES = (
    "unknown",
    "healthy",
    "invalid_identifier",
    "provider_404",
    "rate_limited",
    "auth_error",
    "network_error",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _trimmed(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clean_optional(value: Any) -> str | None:
    text = _trimmed(value)
    return text or None


def _normalize_platform(value: Any) -> str:
    platform = _trimmed(value).lower()
    if platform not in SUPPORTED_SOCIAL_PLATFORMS:
        raise ValueError(f"Unsupported social platform: {value}")
    return platform


def _normalize_source_kind(value: Any, platform: Any | None = None) -> str:
    normalized_platform = _normalize_platform(platform) if platform is not None else None
    source_kind = _trimmed(value).lower()
    if not source_kind and normalized_platform:
        source_kind = DEFAULT_SOURCE_KIND_BY_PLATFORM[normalized_platform]
    if source_kind not in SUPPORTED_SOCIAL_SOURCE_KINDS:
        raise ValueError(f"Unsupported social source kind: {value}")
    if normalized_platform == "facebook" and source_kind not in {"facebook_page", "meta_ads"}:
        raise ValueError(f"Unsupported Facebook source kind: {source_kind}")
    if normalized_platform == "instagram" and source_kind != "instagram_profile":
        raise ValueError(f"Unsupported Instagram source kind: {source_kind}")
    if normalized_platform == "google" and source_kind != "google_domain":
        raise ValueError(f"Unsupported Google source kind: {source_kind}")
    if normalized_platform == "tiktok" and source_kind != "tiktok_profile":
        raise ValueError(f"Unsupported TikTok source kind: {source_kind}")
    return source_kind


def _serialize_metadata(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _merge_metadata(base: Any, extra: dict[str, Any]) -> dict[str, Any]:
    merged = dict(_serialize_metadata(base))
    for key, value in extra.items():
        if value is not None:
            merged[key] = value
    return merged


def _normalize_health_status(value: Any) -> str:
    status = _trimmed(value).lower() or "unknown"
    return status if status in ACCOUNT_HEALTH_STATUSES else "unknown"


def _account_scope_key(account: dict[str, Any]) -> str:
    entity_id = _trimmed(account.get("entity_id"))
    platform = _normalize_platform(account.get("platform"))
    source_kind = _normalize_source_kind(account.get("source_kind"), platform)
    return f"{entity_id}:{platform}:{source_kind}"


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _analysis_list(payload: dict[str, Any] | None, key: str) -> list[str]:
    values = []
    for item in _as_list((payload or {}).get(key)):
        if isinstance(item, str):
            text = _trimmed(item)
        elif isinstance(item, dict):
            text = _trimmed(item.get("name") or item.get("claim") or item.get("label") or item.get("value"))
        else:
            text = ""
        if text:
            values.append(text)
    return values


def _analysis_text(payload: dict[str, Any] | None, key: str) -> str | None:
    text = _trimmed((payload or {}).get(key))
    return text or None


def _published_bounds(from_date: str | None, to_date: str | None) -> tuple[str | None, str | None]:
    def _format(value: str, *, end_of_day: bool) -> str | None:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            try:
                parsed = datetime.fromisoformat(f"{value}T00:00:00")
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0 and parsed.microsecond == 0:
            parsed = datetime.combine(
                parsed.date(),
                time.max if end_of_day else time.min,
                tzinfo=timezone.utc,
            )
        return parsed.astimezone(timezone.utc).isoformat()

    return _format(from_date, end_of_day=False), _format(to_date, end_of_day=True)


def _sentiment_bucket(activity: dict[str, Any]) -> str:
    analysis = dict(activity.get("analysis") or {})
    payload = dict(analysis.get("analysis_payload") or {})
    label = _trimmed(analysis.get("sentiment") or payload.get("sentiment")).lower()
    if label in {"positive", "negative", "neutral"}:
        return label
    try:
        score = float(analysis.get("sentiment_score"))
    except Exception:
        score = 0.0
    if score >= 0.2:
        return "positive"
    if score <= -0.2:
        return "negative"
    return "neutral"


def _sentiment_score(activity: dict[str, Any]) -> float:
    analysis = dict(activity.get("analysis") or {})
    try:
        return float(analysis.get("sentiment_score"))
    except Exception:
        return 0.0


def _engagement_total(activity: dict[str, Any]) -> int:
    metrics = activity.get("engagement_metrics")
    if not isinstance(metrics, dict):
        return 0
    total = 0
    for key in ("likes", "comments", "shares", "views", "reactions"):
        try:
            total += int(metrics.get(key) or 0)
        except Exception:
            continue
    return total


def _humanize_slug(value: str) -> str:
    slug = _trimmed(value).strip("/").replace(".", " ").replace("-", " ").replace("_", " ")
    return " ".join(part.capitalize() for part in slug.split() if part) or "Facebook Source"


def _stable_company_slug(value: str) -> str:
    text = _trimmed(value).lower()
    out: list[str] = []
    dash = False
    for char in text:
        if char.isalnum():
            out.append(char)
            dash = False
        elif not dash:
            out.append("-")
            dash = True
    return "".join(out).strip("-") or "company"


class SocialStore:
    """Operational data access layer for the social media activities domain."""

    def __init__(self) -> None:
        self.client: Client = create_client(
            config.SOCIAL_SUPABASE_URL,
            config.SOCIAL_SUPABASE_SERVICE_ROLE_KEY,
        )

    def _select_rows(
        self,
        table: str,
        *,
        columns: str = "*",
        filters: Iterable[tuple[str, str, Any]] | None = None,
        order_by: str | None = None,
        desc: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = self.client.table(table).select(columns)
        for op, column, value in filters or ():
            if op == "eq":
                query = query.eq(column, value)
            elif op == "in":
                query = query.in_(column, value)
            elif op == "neq":
                query = query.neq(column, value)
            elif op == "is":
                query = query.is_(column, value)
            elif op == "gte":
                query = query.gte(column, value)
            elif op == "lte":
                query = query.lte(column, value)
            elif op == "gt":
                query = query.gt(column, value)
            elif op == "lt":
                query = query.lt(column, value)
            else:
                raise ValueError(f"Unsupported Supabase filter op: {op}")
        if order_by:
            query = query.order(order_by, desc=desc)
        if limit is not None:
            query = query.limit(limit)
        response = query.execute()
        return list(response.data or [])

    def _single_row(
        self,
        table: str,
        *,
        columns: str = "*",
        filters: Iterable[tuple[str, str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        rows = self._select_rows(table, columns=columns, filters=filters, limit=1)
        return rows[0] if rows else None

    def get_runtime_setting(self, key: str, default: dict[str, Any]) -> dict[str, Any]:
        row = self._single_row(
            "social_runtime_settings",
            filters=(("eq", "key", key),),
        )
        payload = row.get("value") if isinstance(row, dict) else None
        return payload if isinstance(payload, dict) else dict(default)

    def save_runtime_setting(self, key: str, value: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "key": key,
            "value": dict(value),
            "updated_at": _utc_now_iso(),
        }
        self.client.table("social_runtime_settings").upsert(payload, on_conflict="key").execute()
        return dict(payload["value"])

    def list_companies_for_social_sync(self, company_ids: list[str] | None = None) -> list[dict[str, Any]]:
        filters: list[tuple[str, str, Any]] = []
        if company_ids:
            filters.append(("in", "id", company_ids))
        return self._select_rows(
            "companies",
            columns=(
                "id,company_key,name,industry,website,logo_url,metadata,is_active,"
                "facebook_page_id,facebook_url,instagram_username,google_ads_domain"
            ),
            filters=filters,
            order_by="name",
        )

    def sync_entities_from_companies(self, company_ids: list[str] | None = None) -> dict[str, Any]:
        companies = self.list_companies_for_social_sync(company_ids)
        if not companies:
            return {"companies": 0, "entities": 0, "accounts": 0}

        entity_payloads = []
        now = _utc_now_iso()
        for company in companies:
            entity_payloads.append(
                {
                    "legacy_company_id": company["id"],
                    "company_key": _clean_optional(company.get("company_key")),
                    "name": _trimmed(company.get("name")) or "Unknown Company",
                    "industry": _clean_optional(company.get("industry")),
                    "website": _clean_optional(company.get("website")),
                    "logo_url": _clean_optional(company.get("logo_url")),
                    "metadata": _serialize_metadata(company.get("metadata")),
                    "is_active": bool(company.get("is_active", True)),
                    "last_company_sync_at": now,
                }
            )
        self.client.table("social_entities").upsert(
            entity_payloads,
            on_conflict="legacy_company_id",
        ).execute()

        entities = self._select_rows(
            "social_entities",
            columns="id,legacy_company_id,is_active",
            filters=(("in", "legacy_company_id", [company["id"] for company in companies]),),
        )
        entity_by_company = {row["legacy_company_id"]: row for row in entities}

        account_payloads: list[dict[str, Any]] = []
        for company in companies:
            entity = entity_by_company.get(company["id"])
            if not entity:
                continue
            entity_id = entity["id"]
            if _clean_optional(company.get("facebook_page_id")):
                account_payloads.append(
                    {
                        "entity_id": entity_id,
                        "platform": "facebook",
                        "source_kind": "meta_ads",
                        "account_external_id": _clean_optional(company.get("facebook_page_id")),
                        "account_handle": None,
                        "domain": None,
                        "import_source": "companies_seed",
                        "metadata": {"seeded_from": "companies.facebook_page_id"},
                        "is_active": bool(company.get("is_active", True)),
                    }
                )
            if _clean_optional(company.get("facebook_url")):
                account_payloads.append(
                    {
                        "entity_id": entity_id,
                        "platform": "facebook",
                        "source_kind": "facebook_page",
                        "account_external_id": None,
                        "account_handle": None,
                        "domain": None,
                        "import_source": "companies_seed",
                        "metadata": {
                            "seeded_from": "companies.facebook_url",
                            "source_url": _clean_optional(company.get("facebook_url")),
                            "page_url": _clean_optional(company.get("facebook_url")),
                        },
                        "is_active": bool(company.get("is_active", True)),
                    }
                )
            if _clean_optional(company.get("instagram_username")):
                account_payloads.append(
                    {
                        "entity_id": entity_id,
                        "platform": "instagram",
                        "source_kind": "instagram_profile",
                        "account_external_id": None,
                        "account_handle": _clean_optional(company.get("instagram_username")),
                        "domain": None,
                        "import_source": "companies_seed",
                        "metadata": {"seeded_from": "companies.instagram_username"},
                        "is_active": bool(company.get("is_active", True)),
                    }
                )
            if _clean_optional(company.get("google_ads_domain")):
                account_payloads.append(
                    {
                        "entity_id": entity_id,
                        "platform": "google",
                        "source_kind": "google_domain",
                        "account_external_id": None,
                        "account_handle": None,
                        "domain": _clean_optional(company.get("google_ads_domain")),
                        "import_source": "companies_seed",
                        "metadata": {"seeded_from": "companies.google_ads_domain"},
                        "is_active": bool(company.get("is_active", True)),
                    }
                )
        if account_payloads:
            self.client.table("social_entity_accounts").upsert(
                account_payloads,
                on_conflict="entity_id,platform,source_kind",
            ).execute()

        return {
            "companies": len(companies),
            "entities": len(entities),
            "accounts": len(account_payloads),
        }

    def ensure_entity_from_company(self, legacy_company_id: str) -> dict[str, Any]:
        sync_result = self.sync_entities_from_companies([legacy_company_id])
        if sync_result["entities"] <= 0:
            raise ValueError("Company not found in master registry")
        entity = self._single_row(
            "social_entities",
            filters=(("eq", "legacy_company_id", legacy_company_id),),
        )
        if not entity:
            raise ValueError("Social entity sync failed")
        return self.get_entity(entity["id"])

    def _load_accounts_for_entities(self, entity_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        if not entity_ids:
            return {}
        rows = self._select_rows(
            "social_entity_accounts",
            filters=(("in", "entity_id", entity_ids),),
            order_by="platform",
        )
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            grouped[str(row.get("entity_id"))].append(dict(row))
        return grouped

    def _enrich_entities(self, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        entity_ids = [str(row.get("id")) for row in entities if row.get("id")]
        accounts_by_entity = self._load_accounts_for_entities(entity_ids)
        for entity in entities:
            accounts = accounts_by_entity.get(str(entity.get("id")), [])
            entity["accounts"] = accounts
            sorted_accounts = sorted(
                accounts,
                key=lambda item: (
                    _trimmed(item.get("platform")).lower(),
                    0
                    if _trimmed(item.get("source_kind")).lower()
                    == DEFAULT_SOURCE_KIND_BY_PLATFORM.get(_trimmed(item.get("platform")).lower(), "")
                    else 1,
                    _trimmed(item.get("source_kind")).lower(),
                ),
            )
            entity["platform_accounts"] = {
                platform: next((row for row in sorted_accounts if row.get("platform") == platform), None)
                for platform in SUPPORTED_SOCIAL_PLATFORMS
            }
        return entities

    def list_entities(self) -> list[dict[str, Any]]:
        entities = self._select_rows("social_entities", order_by="name")
        return self._enrich_entities(entities)

    def get_entity(self, entity_id: str) -> dict[str, Any] | None:
        entity = self._single_row("social_entities", filters=(("eq", "id", entity_id),))
        if not entity:
            return None
        return self._enrich_entities([entity])[0]

    def _project_source_row(self, account: dict[str, Any], entity: dict[str, Any] | None = None) -> dict[str, Any]:
        metadata = _serialize_metadata(account.get("metadata"))
        platform = _normalize_platform(account.get("platform"))
        source_kind = _normalize_source_kind(account.get("source_kind"), platform)
        display_url = _clean_optional(metadata.get("source_url"))
        if not display_url:
            if platform == "facebook" and source_kind == "facebook_page":
                display_url = _clean_optional(metadata.get("page_url"))
            elif platform == "instagram":
                handle = _clean_optional(account.get("account_handle"))
                if handle:
                    display_url = f"https://www.instagram.com/{handle.lstrip('@')}/"
            elif platform == "google":
                domain = _clean_optional(account.get("domain"))
                if domain:
                    display_url = f"https://{domain}"
            elif platform == "tiktok":
                handle = _clean_optional(account.get("account_handle"))
                if handle:
                    display_url = f"https://www.tiktok.com/@{handle.lstrip('@')}"

        return {
            "id": account.get("id"),
            "entity_id": account.get("entity_id"),
            "company_id": (entity or {}).get("legacy_company_id"),
            "company_name": _trimmed((entity or {}).get("name")) or "Unknown Company",
            "company_website": _clean_optional((entity or {}).get("website")),
            "platform": platform,
            "source_kind": source_kind,
            "display_url": display_url,
            "account_external_id": _clean_optional(account.get("account_external_id")),
            "is_active": bool(account.get("is_active", True)),
            "health_status": _normalize_health_status(account.get("health_status")),
            "last_collected_at": account.get("last_collected_at"),
            "last_error": _clean_optional(account.get("last_health_error")),
            "metadata": metadata,
        }

    def list_source_rows(self) -> list[dict[str, Any]]:
        accounts = self._select_rows(
            "social_entity_accounts",
            order_by="updated_at",
            desc=True,
        )
        if not accounts:
            return []
        entity_ids = [str(row.get("entity_id")) for row in accounts if row.get("entity_id")]
        entities = {
            row["id"]: row
            for row in self._select_rows(
                "social_entities",
                filters=(("in", "id", entity_ids),),
            )
        }
        return [self._project_source_row(row, entities.get(row.get("entity_id"))) for row in accounts]

    def get_source_row(self, account_id: str) -> dict[str, Any] | None:
        account = self.get_account(account_id)
        if not account:
            return None
        entity = account.get("entity") if isinstance(account.get("entity"), dict) else None
        return self._project_source_row(account, entity)

    def _find_existing_source(
        self,
        *,
        platform: str,
        source_kind: str,
        account_external_id: str | None = None,
        account_handle: str | None = None,
        domain: str | None = None,
        source_url: str | None = None,
    ) -> dict[str, Any] | None:
        filters: list[tuple[str, str, Any]] = [
            ("eq", "platform", _normalize_platform(platform)),
            ("eq", "source_kind", _normalize_source_kind(source_kind, platform)),
        ]
        if account_external_id:
            filters.append(("eq", "account_external_id", account_external_id))
        elif account_handle:
            filters.append(("eq", "account_handle", account_handle))
        elif domain:
            filters.append(("eq", "domain", domain))
        account = None
        if len(filters) > 2:
            account = self._single_row(
                "social_entity_accounts",
                filters=filters,
            )
        if not account:
            source_url_value = _clean_optional(source_url)
            if not source_url_value:
                return None
            rows = self._select_rows(
                "social_entity_accounts",
                filters=(("eq", "platform", _normalize_platform(platform)), ("eq", "source_kind", _normalize_source_kind(source_kind, platform))),
            )
            account = next(
                (
                    row
                    for row in rows
                    if _clean_optional(_serialize_metadata(row.get("metadata")).get("source_url")) == source_url_value
                    or _clean_optional(_serialize_metadata(row.get("metadata")).get("page_url")) == source_url_value
                ),
                None,
            )
            if not account:
                return None
        entity = self._single_row("social_entities", filters=(("eq", "id", account.get("entity_id")),))
        if entity:
            account["entity"] = entity
        return account

    def _find_company_for_source(
        self,
        *,
        platform: str,
        source_kind: str,
        source_key: str | None,
        source_url: str | None,
        display_name: str,
        company_key: str,
    ) -> dict[str, Any]:
        platform = _normalize_platform(platform)
        source_kind = _normalize_source_kind(source_kind, platform)
        source_key = _clean_optional(source_key)
        source_url = _clean_optional(source_url)
        company = None
        if platform == "facebook" and source_kind == "meta_ads" and source_key:
            company = self._single_row("companies", filters=(("eq", "facebook_page_id", source_key),))
        elif platform == "facebook" and source_kind == "facebook_page" and source_url:
            company = self._single_row("companies", filters=(("eq", "facebook_url", source_url),))
        elif platform == "instagram" and source_key:
            company = self._single_row("companies", filters=(("eq", "instagram_username", source_key),))
        elif platform == "google" and source_key:
            company = self._single_row("companies", filters=(("eq", "google_ads_domain", source_key),))
        if company:
            return company
        company = self._single_row("companies", filters=(("eq", "company_key", company_key),))
        if company:
            return company
        company = self._single_row("companies", filters=(("eq", "name", display_name),))
        if company:
            return company
        raise ValueError("Company not found")

    def _ensure_company_for_source(
        self,
        *,
        platform: str,
        source_kind: str,
        source_key: str | None,
        source_url: str | None,
        display_name: str,
    ) -> dict[str, Any]:
        platform = _normalize_platform(platform)
        source_kind = _normalize_source_kind(source_kind, platform)
        source_key = _clean_optional(source_key)
        source_url = _clean_optional(source_url)
        company_key_seed = source_key or source_url or display_name
        company_key = f"{source_kind}:{company_key_seed}"
        try:
            company = self._find_company_for_source(
                platform=platform,
                source_kind=source_kind,
                source_key=source_key,
                source_url=source_url,
                display_name=display_name,
                company_key=company_key,
            )
        except ValueError:
            company = None
        if company:
            update_payload: dict[str, Any] = {}
            if not _trimmed(company.get("name")):
                update_payload["name"] = display_name
            if not _trimmed(company.get("company_key")):
                update_payload["company_key"] = company_key
            if platform == "facebook" and source_kind == "meta_ads" and _clean_optional(company.get("facebook_page_id")) != source_key:
                update_payload["facebook_page_id"] = source_key
            if platform == "facebook" and source_kind == "facebook_page" and _clean_optional(company.get("facebook_url")) != source_url:
                update_payload["facebook_url"] = source_url
            if platform == "instagram" and _clean_optional(company.get("instagram_username")) != source_key:
                update_payload["instagram_username"] = source_key
            if platform == "google" and _clean_optional(company.get("google_ads_domain")) != source_key:
                update_payload["google_ads_domain"] = source_key
            metadata = {"source_platform": platform, "source_kind": source_kind}
            if source_url:
                metadata["source_url"] = source_url
                if source_kind == "facebook_page":
                    metadata["page_url"] = source_url
            if source_key:
                metadata["source_key"] = source_key
            update_payload["metadata"] = _merge_metadata(company.get("metadata"), metadata)
            if update_payload:
                self.client.table("companies").update(update_payload).eq("id", company["id"]).execute()
                company = self._single_row("companies", filters=(("eq", "id", company["id"]),)) or company
            return company

        payload = {
            "name": display_name,
            "company_key": company_key,
            "metadata": {
                "source_platform": platform,
                "source_kind": source_kind,
                **({"source_url": source_url} if source_url else {}),
                **({"page_url": source_url} if source_kind == "facebook_page" and source_url else {}),
                **({"source_key": source_key} if source_key else {}),
            },
            "is_active": True,
        }
        if platform == "facebook" and source_kind == "meta_ads":
            payload["facebook_page_id"] = source_key
        elif platform == "facebook" and source_kind == "facebook_page":
            payload["facebook_url"] = source_url
        elif platform == "instagram":
            payload["instagram_username"] = source_key
        elif platform == "google":
            payload["google_ads_domain"] = source_key
        response = self.client.table("companies").insert(payload).execute()
        rows = list(response.data or [])
        if rows:
            return dict(rows[0])
        try:
            company = self._find_company_for_source(
                platform=platform,
                source_kind=source_kind,
                source_key=source_key,
                source_url=source_url,
                display_name=display_name,
                company_key=company_key,
            )
        except ValueError as exc:
            raise ValueError("Failed to create company record for social source") from exc
        return company

    def _find_company_for_company_sources(
        self,
        *,
        company_id: str | None,
        company_key: str,
        company_name: str,
        website: str | None,
        sources: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if company_id:
            company = self._single_row("companies", filters=(("eq", "id", company_id),))
            if not company:
                raise ValueError("Company not found")
            return company

        if website:
            company = self._single_row("companies", filters=(("eq", "website", website),))
            if company:
                return company

        company = self._single_row("companies", filters=(("eq", "company_key", company_key),))
        if company:
            return company

        for source in sources:
            source_kind = _normalize_source_kind(source.get("source_type"))
            source_key = _clean_optional(source.get("source_key"))
            source_url = _clean_optional(source.get("source_url"))
            if source_kind == "google_domain" and source_key:
                company = self._single_row("companies", filters=(("eq", "google_ads_domain", source_key),))
            elif source_kind == "facebook_page" and source_url:
                company = self._single_row("companies", filters=(("eq", "facebook_url", source_url),))
            elif source_kind == "instagram_profile" and source_key:
                company = self._single_row("companies", filters=(("eq", "instagram_username", source_key),))
            elif source_kind == "meta_ads" and source_key:
                company = self._single_row("companies", filters=(("eq", "facebook_page_id", source_key),))
            else:
                company = None
            if company:
                return company

        company = self._single_row("companies", filters=(("eq", "name", company_name),))
        return company

    def _account_payload_for_company_source(self, source: dict[str, Any]) -> dict[str, Any]:
        source_kind = _normalize_source_kind(source.get("source_type"))
        platform = (
            "facebook"
            if source_kind in {"facebook_page", "meta_ads"}
            else "instagram"
            if source_kind == "instagram_profile"
            else "google"
            if source_kind == "google_domain"
            else "tiktok"
        )
        source_key = _clean_optional(source.get("source_key"))
        source_url = _clean_optional(source.get("source_url"))
        metadata = {
            "source_url": source_url,
            "source_key": source_key,
            "source_platform": platform,
            "source_kind": source_kind,
            "created_from": "company_sources_modal",
        }
        if source_kind == "facebook_page" and source_url:
            metadata["page_url"] = source_url
        payload: dict[str, Any] = {
            "platform": platform,
            "source_kind": source_kind,
            "import_source": "sources_page",
            "metadata": metadata,
            "is_active": True,
        }
        if source_kind == "meta_ads":
            payload["account_external_id"] = source_key
        elif source_kind == "facebook_page" and source_key and not source_url:
            payload["account_external_id"] = source_key
        elif source_kind == "instagram_profile":
            payload["account_handle"] = source_key
        elif source_kind == "google_domain":
            payload["domain"] = source_key
        return payload

    def create_or_update_company_sources(
        self,
        *,
        company_name: str,
        website: str | None,
        website_domain: str | None,
        sources: list[dict[str, Any]],
        company_id: str | None = None,
    ) -> dict[str, Any]:
        name = _trimmed(company_name)
        if not name:
            raise ValueError("Company name is required")
        if not sources:
            raise ValueError("Add at least one scraping source")

        company_key_seed = _clean_optional(website_domain) or name
        company_key = f"company:{_stable_company_slug(company_key_seed)}"
        company = self._find_company_for_company_sources(
            company_id=company_id,
            company_key=company_key,
            company_name=name,
            website=website,
            sources=sources,
        )

        metadata = {
            "created_from": "company_sources_modal",
            "source_count": len(sources),
            **({"website_domain": website_domain} if website_domain else {}),
        }
        for source in sources:
            source_kind = _normalize_source_kind(source.get("source_type"))
            source_key = _clean_optional(source.get("source_key"))
            source_url = _clean_optional(source.get("source_url"))
            if source_kind == "meta_ads" and source_key:
                metadata["meta_ads_page_id"] = source_key
            elif source_kind == "facebook_page" and source_url:
                metadata["facebook_url"] = source_url
            elif source_kind == "instagram_profile" and source_key:
                metadata["instagram_username"] = source_key
            elif source_kind == "google_domain" and source_key:
                metadata["google_ads_domain"] = source_key

        company_payload: dict[str, Any] = {
            "name": name,
            "company_key": company_key,
            "metadata": _merge_metadata(company.get("metadata") if company else {}, metadata),
            "is_active": True,
        }
        if website:
            company_payload["website"] = website
        for source in sources:
            source_kind = _normalize_source_kind(source.get("source_type"))
            source_key = _clean_optional(source.get("source_key"))
            source_url = _clean_optional(source.get("source_url"))
            if source_kind == "meta_ads":
                company_payload["facebook_page_id"] = source_key
            elif source_kind == "facebook_page":
                company_payload["facebook_url"] = source_url
            elif source_kind == "instagram_profile":
                company_payload["instagram_username"] = source_key
            elif source_kind == "google_domain":
                company_payload["google_ads_domain"] = source_key

        if company:
            self.client.table("companies").update(company_payload).eq("id", company["id"]).execute()
            action = "updated"
            company = self._single_row("companies", filters=(("eq", "id", company["id"]),)) or {**company, **company_payload}
        else:
            response = self.client.table("companies").insert(company_payload).execute()
            rows = list(response.data or [])
            if not rows:
                company = self._single_row("companies", filters=(("eq", "company_key", company_key),))
                if not company:
                    raise ValueError("Failed to create company record for social sources")
            else:
                company = dict(rows[0])
            action = "created"

        entity = self.ensure_entity_from_company(str(company["id"]))
        account_payloads = [self._account_payload_for_company_source(source) for source in sources]
        self.upsert_accounts(str(entity["id"]), account_payloads)
        items = [
            row
            for row in self.list_source_rows()
            if str(row.get("entity_id")) == str(entity["id"])
        ]
        return {
            "action": action,
            "company": {
                "id": company.get("id"),
                "name": _trimmed(company.get("name")) or name,
                "website": _clean_optional(company.get("website")),
            },
            "entity": {
                "id": entity.get("id"),
                "company_id": company.get("id"),
                "name": _trimmed(entity.get("name")) or name,
            },
            "items": items,
        }

    def create_or_update_source(
        self,
        *,
        source_type: str,
        source_key: str | None,
        source_url: str | None,
        display_name: str,
    ) -> dict[str, Any]:
        source_kind = _normalize_source_kind(source_type)
        platform = "facebook" if source_kind in {"facebook_page", "meta_ads"} else "instagram" if source_kind == "instagram_profile" else "google" if source_kind == "google_domain" else "tiktok"
        source_key = _clean_optional(source_key)
        source_url = _clean_optional(source_url)
        existing = self._find_existing_source(
            platform=platform,
            source_kind=source_kind,
            account_external_id=source_key if source_kind == "meta_ads" else None,
            account_handle=source_key if source_kind == "instagram_profile" else None,
            domain=source_key if source_kind == "google_domain" else None,
            source_url=source_url if source_kind == "facebook_page" else None,
        )
        metadata = {
            "source_url": source_url,
            "source_key": source_key,
            "source_platform": platform,
            "source_kind": source_kind,
            "created_from": "sources_page",
        }
        if source_kind == "facebook_page" and source_url:
            metadata["page_url"] = source_url
        if existing:
            update_payload = {
                "metadata": _merge_metadata(existing.get("metadata"), metadata),
                "import_source": _clean_optional(existing.get("import_source")) or "sources_page",
            }
            action = "exists"
            if not bool(existing.get("is_active", True)):
                update_payload["is_active"] = True
                action = "reactivated"
            self.client.table("social_entity_accounts").update(update_payload).eq("id", existing["id"]).execute()
            item = self.get_source_row(str(existing["id"]))
            if not item:
                raise ValueError("Social source row not found after update")
            return {"action": action, "item": item}

        company = self._ensure_company_for_source(
            platform=platform,
            source_kind=source_kind,
            source_key=source_key,
            source_url=source_url,
            display_name=display_name,
        )
        entity = self.ensure_entity_from_company(str(company["id"]))
        account_payload: dict[str, Any] = {
            "platform": platform,
            "source_kind": source_kind,
            "import_source": "sources_page",
            "metadata": metadata,
            "is_active": True,
        }
        if source_kind == "meta_ads":
            account_payload["account_external_id"] = source_key
        elif source_kind == "facebook_page":
            account_payload["account_external_id"] = source_key if not source_url else None
        elif source_kind == "instagram_profile":
            account_payload["account_handle"] = source_key
        elif source_kind == "google_domain":
            account_payload["domain"] = source_key
        self.upsert_accounts(
            str(entity["id"]),
            [account_payload],
        )
        item = next(
            (
                row
                for row in self.list_source_rows()
                if str(row.get("entity_id")) == str(entity["id"])
                and str(row.get("platform")) == platform
                and str(row.get("source_kind")) == source_kind
            ),
            None,
        )
        if not item:
            raise ValueError("Social source row not found after create")
        return {"action": "created", "item": item}

    def create_or_update_facebook_source(
        self,
        *,
        source_key: str,
        source_url: str,
        display_name: str,
    ) -> dict[str, Any]:
        return self.create_or_update_source(
            source_type="facebook_page",
            source_key=source_key,
            source_url=source_url,
            display_name=display_name,
        )

    def update_source_account(self, account_id: str, *, is_active: bool) -> dict[str, Any]:
        account = self.get_account(account_id)
        if not account:
            raise ValueError("Social source not found")
        self.client.table("social_entity_accounts").update(
            {
                "is_active": bool(is_active),
            }
        ).eq("id", account_id).execute()
        item = self.get_source_row(account_id)
        if not item:
            raise ValueError("Social source not found after update")
        return item

    def delete_source_account(self, account_id: str) -> dict[str, Any]:
        item = self.get_source_row(account_id)
        if not item:
            raise ValueError("Social source not found")
        self.client.table("social_entity_accounts").delete().eq("id", account_id).execute()
        return item

    def update_entity(
        self,
        entity_id: str,
        *,
        is_active: bool | None = None,
        metadata: dict[str, Any] | None = None,
        accounts: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if is_active is not None:
            payload["is_active"] = bool(is_active)
        if metadata is not None:
            payload["metadata"] = _serialize_metadata(metadata)
        if payload:
            self.client.table("social_entities").update(payload).eq("id", entity_id).execute()
        if accounts:
            self.upsert_accounts(entity_id, accounts)
        entity = self.get_entity(entity_id)
        if not entity:
            raise ValueError("Social entity not found")
        return entity

    def upsert_accounts(self, entity_id: str, accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not accounts:
            return []
        payloads = []
        for account in accounts:
            platform = _normalize_platform(account.get("platform"))
            source_kind = _normalize_source_kind(account.get("source_kind"), platform)
            payloads.append(
                {
                    "entity_id": entity_id,
                    "platform": platform,
                    "source_kind": source_kind,
                    "account_handle": _clean_optional(account.get("account_handle")),
                    "account_external_id": _clean_optional(account.get("account_external_id")),
                    "domain": _clean_optional(account.get("domain")),
                    "import_source": _clean_optional(account.get("import_source")) or "manual",
                    "metadata": _serialize_metadata(account.get("metadata")),
                    "is_active": bool(account.get("is_active", True)),
                }
            )
        self.client.table("social_entity_accounts").upsert(
            payloads,
            on_conflict="entity_id,platform,source_kind",
        ).execute()
        entity = self.get_entity(entity_id)
        return list(entity.get("accounts") or []) if entity else []

    def get_account(self, account_id: str) -> dict[str, Any] | None:
        row = self._single_row("social_entity_accounts", filters=(("eq", "id", account_id),))
        if not row:
            return None
        entity = self._single_row("social_entities", filters=(("eq", "id", row.get("entity_id")),))
        if entity:
            row["entity"] = entity
        return row

    def get_account_by_scope_key(self, scope_key: str) -> dict[str, Any] | None:
        parts = [part.strip() for part in str(scope_key or "").split(":") if part.strip()]
        if len(parts) < 2:
            return None
        entity_id, platform = parts[0], parts[1]
        source_kind = parts[2] if len(parts) > 2 else None
        filters: list[tuple[str, str, Any]] = [
            ("eq", "entity_id", entity_id),
            ("eq", "platform", _normalize_platform(platform)),
        ]
        if source_kind:
            filters.append(("eq", "source_kind", _normalize_source_kind(source_kind, platform)))
        row = self._single_row(
            "social_entity_accounts",
            filters=filters,
        )
        if not row:
            return None
        entity = self._single_row("social_entities", filters=(("eq", "id", entity_id),))
        if entity:
            row["entity"] = entity
        return row

    def mark_account_collect_success(self, account_id: str) -> None:
        self.client.table("social_entity_accounts").update(
            {
                "health_status": "healthy",
                "last_health_error": None,
                "last_health_checked_at": _utc_now_iso(),
                "last_collected_at": _utc_now_iso(),
                "collect_claimed_at": None,
                "collect_claimed_by": None,
            }
        ).eq("id", account_id).execute()
        account = self.get_account(account_id)
        if not account:
            return
        self.clear_failure(
            stage="ingest",
            scope_key=_account_scope_key(account),
        )

    def mark_account_collect_failure(
        self,
        account_id: str,
        *,
        health_status: str,
        error: str,
    ) -> None:
        self.client.table("social_entity_accounts").update(
            {
                "health_status": _normalize_health_status(health_status),
                "last_health_error": str(error)[:4000],
                "last_health_checked_at": _utc_now_iso(),
                "collect_claimed_at": None,
                "collect_claimed_by": None,
            }
        ).eq("id", account_id).execute()

    def list_active_accounts(self, platforms: list[str] | None = None) -> list[dict[str, Any]]:
        filters: list[tuple[str, str, Any]] = [("eq", "is_active", True)]
        normalized_platforms = [_normalize_platform(item) for item in (platforms or [])]
        if normalized_platforms:
            filters.append(("in", "platform", normalized_platforms))
        accounts = self._select_rows(
            "social_entity_accounts",
            filters=filters,
            order_by="platform",
        )
        if not accounts:
            return []
        entity_ids = [str(row.get("entity_id")) for row in accounts if row.get("entity_id")]
        entities = {
            row["id"]: row
            for row in self._select_rows(
                "social_entities",
                filters=(("in", "id", entity_ids), ("eq", "is_active", True)),
            )
        }
        enriched: list[dict[str, Any]] = []
        for account in accounts:
            entity = entities.get(account["entity_id"])
            if not entity:
                continue
            has_fetch_key = any(
                _clean_optional(account.get(field))
                for field in ("account_external_id", "account_handle", "domain")
            )
            if not has_fetch_key and _normalize_source_kind(account.get("source_kind"), account.get("platform")) == "facebook_page":
                has_fetch_key = bool(
                    _clean_optional(_serialize_metadata(account.get("metadata")).get("page_url"))
                    or _clean_optional(_serialize_metadata(account.get("metadata")).get("source_url"))
                )
            if not has_fetch_key:
                continue
            enriched.append(
                {
                    **dict(account),
                    "entity": dict(entity),
                }
            )
        return enriched

    def create_ingest_run(
        self,
        *,
        run_kind: str,
        entity_id: str | None = None,
        platform: str | None = None,
        status: str = "running",
        metrics: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "run_kind": run_kind,
            "entity_id": entity_id,
            "platform": platform,
            "status": status,
            "metrics": metrics or {},
            "error": error,
        }
        response = self.client.table("social_ingest_runs").insert(payload).execute()
        return dict((response.data or [{}])[0])

    def finish_ingest_run(
        self,
        run_id: str,
        *,
        status: str,
        metrics: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, Any] | None:
        response = (
            self.client.table("social_ingest_runs")
            .update(
                {
                    "status": status,
                    "metrics": metrics or {},
                    "error": error,
                    "finished_at": _utc_now_iso(),
                }
            )
            .eq("id", run_id)
            .execute()
        )
        rows = response.data or []
        return dict(rows[0]) if rows else None

    def list_recent_runs(self, limit: int = 12) -> list[dict[str, Any]]:
        return self._select_rows(
            "social_ingest_runs",
            order_by="started_at",
            desc=True,
            limit=limit,
        )

    def record_failure(
        self,
        *,
        stage: str,
        scope_key: str,
        error: str,
        activity_id: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
        metadata: dict[str, Any] | None = None,
        max_attempts: int = 5,
    ) -> dict[str, Any]:
        existing = self._single_row(
            "social_processing_failures",
            filters=(("eq", "stage", stage), ("eq", "scope_key", scope_key)),
        )
        attempt_count = int(existing.get("attempt_count", 0)) + 1 if existing else 1
        backoff_seconds = min(
            int(config.SOCIAL_RETRY_MAX_SECONDS),
            int(config.SOCIAL_RETRY_BASE_SECONDS) * max(1, 2 ** max(0, attempt_count - 1)),
        )
        payload = {
            "activity_id": activity_id,
            "entity_id": entity_id,
            "platform": platform,
            "stage": stage,
            "scope_key": scope_key,
            "attempt_count": attempt_count,
            "last_error": str(error)[:4000],
            "metadata": metadata or {},
            "last_failed_at": _utc_now_iso(),
            "next_retry_at": datetime.now(timezone.utc).timestamp() + backoff_seconds,
            "is_dead_letter": attempt_count >= max(1, int(max_attempts)),
            "resolved_at": None,
        }
        if not existing:
            payload["first_failed_at"] = _utc_now_iso()
        payload["next_retry_at"] = datetime.fromtimestamp(
            float(payload["next_retry_at"]),
            tz=timezone.utc,
        ).isoformat()
        self.client.table("social_processing_failures").upsert(
            payload,
            on_conflict="stage,scope_key",
        ).execute()
        return self._single_row(
            "social_processing_failures",
            filters=(("eq", "stage", stage), ("eq", "scope_key", scope_key)),
        ) or payload

    def clear_failure(self, *, stage: str, scope_key: str) -> None:
        self.client.table("social_processing_failures").update(
            {
                "resolved_at": _utc_now_iso(),
                "is_dead_letter": False,
            }
        ).eq("stage", stage).eq("scope_key", scope_key).execute()

    def list_failures(
        self,
        *,
        dead_letter_only: bool = False,
        stage: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        filters: list[tuple[str, str, Any]] = [("is", "resolved_at", "null")]
        if dead_letter_only:
            filters.append(("eq", "is_dead_letter", True))
        if stage:
            filters.append(("eq", "stage", stage))
        return self._select_rows(
            "social_processing_failures",
            filters=filters,
            order_by="last_failed_at",
            desc=True,
            limit=limit,
        )

    def get_failure(self, *, stage: str, scope_key: str) -> dict[str, Any] | None:
        return self._single_row(
            "social_processing_failures",
            filters=(("eq", "stage", stage), ("eq", "scope_key", scope_key), ("is", "resolved_at", "null")),
        )

    def upsert_activities(self, activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not activities:
            return []
        activity_uids = [str(item["activity_uid"]) for item in activities if item.get("activity_uid")]
        existing_rows = self._select_rows(
            "social_activities",
            columns=(
                "id,activity_uid,text_content,ingest_status,analysis_status,graph_status,"
                "first_seen_at,analysis_version,graph_projection_version"
            ),
            filters=(("in", "activity_uid", activity_uids),),
        )
        existing_by_uid = {row["activity_uid"]: row for row in existing_rows}
        now = _utc_now_iso()

        payloads: list[dict[str, Any]] = []
        for activity in activities:
            uid = str(activity["activity_uid"])
            existing = existing_by_uid.get(uid)
            text_content = _clean_optional(activity.get("text_content"))
            old_text = _clean_optional(existing.get("text_content")) if existing else None
            text_changed = existing is None or old_text != text_content
            ingest_status = _trimmed(activity.get("ingest_status")).lower() or "normalized"
            if ingest_status not in {"collected", "normalized", "failed", "dead_letter"}:
                ingest_status = "normalized"
            analysis_status = "pending" if ingest_status == "normalized" else "not_needed"
            graph_status = "not_ready"
            analysis_version = _trimmed(activity.get("analysis_version")) or config.SOCIAL_ANALYSIS_PROMPT_VERSION
            normalization_version = _trimmed(activity.get("normalization_version")) or config.SOCIAL_ANALYSIS_PROMPT_VERSION
            if existing:
                analysis_status = str(existing.get("analysis_status") or analysis_status)
                graph_status = str(existing.get("graph_status") or graph_status)
                if (
                    ingest_status == "normalized"
                    and (
                        text_changed
                        or _trimmed(existing.get("analysis_version")) != analysis_version
                    )
                ):
                    analysis_status = "pending"
                    graph_status = "not_ready"
                elif (
                    ingest_status == "normalized"
                    and analysis_status == "analyzed"
                    and _trimmed(existing.get("graph_projection_version")) != config.SOCIAL_GRAPH_PROJECTION_VERSION
                ):
                    graph_status = "pending"
            payloads.append(
                {
                    "entity_id": activity["entity_id"],
                    "account_id": activity.get("account_id"),
                    "activity_uid": uid,
                    "platform": _normalize_platform(activity.get("platform")),
                    "source_kind": _trimmed(activity.get("source_kind")) or "post",
                    "provider_item_id": _clean_optional(activity.get("provider_item_id")),
                    "source_url": _trimmed(activity.get("source_url")) or uid,
                    "text_content": text_content,
                    "published_at": activity.get("published_at"),
                    "author_handle": _clean_optional(activity.get("author_handle")),
                    "cta_type": _clean_optional(activity.get("cta_type")),
                    "content_format": _clean_optional(activity.get("content_format")),
                    "region_name": _clean_optional(activity.get("region_name")),
                    "engagement_metrics": activity.get("engagement_metrics") or {},
                    "assets": activity.get("assets") or [],
                    "provider_payload": activity.get("provider_payload") or {},
                    "normalization_version": normalization_version,
                    "ingest_status": ingest_status,
                    "analysis_status": analysis_status,
                    "graph_status": graph_status,
                    "analysis_version": analysis_version if analysis_status == "analyzed" else existing.get("analysis_version") if existing else None,
                    "first_seen_at": existing.get("first_seen_at") if existing else now,
                    "last_seen_at": now,
                    "last_error": None if ingest_status == "normalized" else activity.get("last_error"),
                }
            )

        self.client.table("social_activities").upsert(payloads, on_conflict="activity_uid").execute()
        return self._select_rows(
            "social_activities",
            filters=(("in", "activity_uid", activity_uids),),
        )

    def list_pending_analysis(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._select_rows(
            "social_activities",
            filters=(("eq", "ingest_status", "normalized"),),
            order_by="last_seen_at",
            desc=False,
            limit=limit,
        )
        eligible = [
            row
            for row in rows
            if row.get("analysis_status") in {"pending", "failed"}
            or _trimmed(row.get("analysis_version")) != config.SOCIAL_ANALYSIS_PROMPT_VERSION
        ]
        entity_ids = {row["entity_id"] for row in eligible}
        entities = {
            row["id"]: row
            for row in self._select_rows("social_entities", filters=(("in", "id", list(entity_ids)),))
        } if entity_ids else {}
        return [{**row, "entity": entities.get(row["entity_id"])} for row in eligible]

    def save_analysis(
        self,
        *,
        activity_id: str,
        entity_id: str,
        platform: str,
        activity_uid: str,
        analysis_payload: dict[str, Any],
        raw_model_output: dict[str, Any],
        model: str,
        prompt_version: str,
        analysis_version: str,
    ) -> dict[str, Any]:
        payload = {
            "activity_id": activity_id,
            "entity_id": entity_id,
            "platform": platform,
            "analysis_version": analysis_version,
            "prompt_version": prompt_version,
            "model": model,
            "summary": _clean_optional(analysis_payload.get("summary")),
            "marketing_intent": _clean_optional(analysis_payload.get("marketing_intent")),
            "sentiment": _clean_optional(analysis_payload.get("sentiment")),
            "sentiment_score": analysis_payload.get("sentiment_score"),
            "analysis_payload": analysis_payload,
            "raw_model_output": raw_model_output,
            "analyzed_at": _utc_now_iso(),
        }
        self.client.table("social_activity_analysis").upsert(payload, on_conflict="activity_id").execute()
        self.client.table("social_activities").update(
            {
                "analysis_status": "analyzed",
                "graph_status": "pending",
                "analysis_version": analysis_version,
                "last_error": None,
                "analysis_claimed_at": None,
                "analysis_claimed_by": None,
            }
        ).eq("id", activity_id).execute()
        self.clear_failure(stage="analysis", scope_key=activity_uid)
        return payload

    def mark_activity_failure(
        self,
        *,
        activity_id: str,
        activity_uid: str,
        stage: str,
        error: str,
        dead_letter: bool,
    ) -> None:
        status_column = "analysis_status" if stage == "analysis" else "graph_status"
        failed_status = "dead_letter" if dead_letter else "failed"
        update_payload = {
            status_column: failed_status,
            "last_error": str(error)[:4000],
        }
        if stage == "graph":
            update_payload["graph_status"] = failed_status
            update_payload["graph_claimed_at"] = None
            update_payload["graph_claimed_by"] = None
        else:
            update_payload["analysis_claimed_at"] = None
            update_payload["analysis_claimed_by"] = None
        self.client.table("social_activities").update(update_payload).eq("id", activity_id).execute()
        logger.warning("Social activity stage failure | stage={} activity_uid={} dead_letter={}", stage, activity_uid, dead_letter)

    def list_pending_graph(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._select_rows(
            "social_activities",
            filters=(("eq", "ingest_status", "normalized"), ("eq", "analysis_status", "analyzed")),
            order_by="last_seen_at",
            desc=False,
            limit=limit,
        )
        rows = [
            row
            for row in rows
            if row.get("graph_status") in {"pending", "failed"}
            or _trimmed(row.get("graph_projection_version")) != config.SOCIAL_GRAPH_PROJECTION_VERSION
        ]
        if not rows:
            return []
        activity_ids = [row["id"] for row in rows]
        entity_ids = [row["entity_id"] for row in rows]
        analyses = {
            row["activity_id"]: row
            for row in self._select_rows(
                "social_activity_analysis",
                filters=(("in", "activity_id", activity_ids),),
            )
        }
        entities = {
            row["id"]: row
            for row in self._select_rows(
                "social_entities",
                filters=(("in", "id", entity_ids),),
            )
        }
        return [
            {
                **row,
                "analysis": analyses.get(row["id"]),
                "entity": entities.get(row["entity_id"]),
            }
            for row in rows
            if analyses.get(row["id"])
        ]

    def mark_graph_synced(
        self,
        *,
        activity_id: str,
        activity_uid: str,
        projection_version: str,
    ) -> None:
        self.client.table("social_activities").update(
            {
                "graph_status": "synced",
                "graph_projection_version": projection_version,
                "last_error": None,
                "graph_claimed_at": None,
                "graph_claimed_by": None,
            }
        ).eq("id", activity_id).execute()
        self.clear_failure(stage="graph", scope_key=activity_uid)

    def list_activities(
        self,
        *,
        limit: int = 100,
        entity_id: str | None = None,
        platform: str | None = None,
    ) -> list[dict[str, Any]]:
        filters: list[tuple[str, str, Any]] = []
        if entity_id:
            filters.append(("eq", "entity_id", entity_id))
        if platform:
            filters.append(("eq", "platform", _normalize_platform(platform)))
        activities = self._select_rows(
            "social_activities",
            filters=filters,
            order_by="published_at",
            desc=True,
            limit=limit,
        )
        if not activities:
            return []
        entity_ids = list({row["entity_id"] for row in activities})
        entities = {
            row["id"]: row
            for row in self._select_rows("social_entities", filters=(("in", "id", entity_ids),))
        }
        analyses = {
            row["activity_id"]: row
            for row in self._select_rows(
                "social_activity_analysis",
                filters=(("in", "activity_id", [row["id"] for row in activities]),),
            )
        }
        return [
            {
                **row,
                "entity": entities.get(row["entity_id"]),
                "analysis": analyses.get(row["id"]),
            }
            for row in activities
        ]

    def _list_filtered_intelligence_activities(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        activity_uid: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
        source_kind: str | None = None,
        cta_type: str | None = None,
        content_format: str | None = None,
        topic: str | None = None,
        marketing_intent: str | None = None,
        pain_point: str | None = None,
        customer_intent: str | None = None,
        sentiment: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        filters: list[tuple[str, str, Any]] = []
        start_iso, end_iso = _published_bounds(from_date, to_date)
        if entity_id:
            filters.append(("eq", "entity_id", entity_id))
        if activity_uid:
            filters.append(("eq", "activity_uid", activity_uid))
        if platform and platform != "all":
            filters.append(("eq", "platform", _normalize_platform(platform)))
        if source_kind:
            filters.append(("eq", "source_kind", _trimmed(source_kind).lower()))
        if cta_type:
            filters.append(("eq", "cta_type", cta_type))
        if content_format:
            filters.append(("eq", "content_format", content_format))
        if start_iso:
            filters.append(("gte", "published_at", start_iso))
        if end_iso:
            filters.append(("lte", "published_at", end_iso))

        activities = self._select_rows(
            "social_activities",
            filters=filters,
            order_by="published_at",
            desc=True,
            limit=limit,
        )
        if not activities:
            return []

        entity_ids = list({row["entity_id"] for row in activities if row.get("entity_id")})
        entities = {
            row["id"]: row
            for row in self._select_rows("social_entities", filters=(("in", "id", entity_ids),))
        }
        analyses = {
            row["activity_id"]: row
            for row in self._select_rows(
                "social_activity_analysis",
                filters=(("in", "activity_id", [row["id"] for row in activities]),),
            )
        }
        enriched = [
            {
                **row,
                "entity": entities.get(row["entity_id"]),
                "analysis": analyses.get(row["id"]),
            }
            for row in activities
        ]

        normalized_topic = _trimmed(topic).lower()
        normalized_intent = _trimmed(marketing_intent).lower()
        normalized_pain = _trimmed(pain_point).lower()
        normalized_customer_intent = _trimmed(customer_intent).lower()
        normalized_sentiment = _trimmed(sentiment).lower()

        filtered_items: list[dict[str, Any]] = []
        for item in enriched:
            analysis = dict(item.get("analysis") or {})
            payload = dict(analysis.get("analysis_payload") or {})
            topics = [value.lower() for value in _analysis_list(payload, "topics")]
            pain_points = [value.lower() for value in _analysis_list(payload, "pain_points")]
            customer_intent_value = (_analysis_text(payload, "customer_intent") or "").lower()
            marketing_intent_value = (_analysis_text(payload, "marketing_intent") or "").lower()
            if normalized_topic and normalized_topic not in topics:
                continue
            if normalized_intent and normalized_intent != marketing_intent_value:
                continue
            if normalized_pain and normalized_pain not in pain_points:
                continue
            if normalized_customer_intent and normalized_customer_intent != customer_intent_value:
                continue
            if normalized_sentiment and normalized_sentiment != _sentiment_bucket(item):
                continue
            filtered_items.append(item)
        return filtered_items

    def get_intelligence_summary(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
    ) -> dict[str, Any]:
        activities = self._list_filtered_intelligence_activities(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
        )
        entities = self.list_entities()
        if entity_id:
            entities = [entity for entity in entities if entity.get("id") == entity_id]

        topic_counts = Counter()
        sentiment_scores: list[float] = []
        ads_detected = 0
        for activity in activities:
            payload = dict((activity.get("analysis") or {}).get("analysis_payload") or {})
            for topic_name in _analysis_list(payload, "topics"):
                topic_counts[topic_name] += 1
            sentiment_scores.append(_sentiment_score(activity))
            if (activity.get("source_kind") or "").lower() == "ad" or (activity.get("platform") or "").lower() == "google":
                ads_detected += 1

        dominant_topic, dominant_topic_count = ("—", 0)
        if topic_counts:
            dominant_topic, dominant_topic_count = topic_counts.most_common(1)[0]
        average_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0.0
        return {
            "trackedCompetitors": len([entity for entity in entities if entity.get("is_active")]),
            "postsCollected": len(activities),
            "adsDetected": ads_detected,
            "averageSentimentScore": round(average_sentiment, 4),
            "averageSentimentPct": round(((average_sentiment + 1.0) / 2.0) * 100, 1),
            "dominantTopic": {
                "name": dominant_topic,
                "count": dominant_topic_count,
            },
        }

    def get_topic_timeline(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
        topic: str | None = None,
        bucket: str = "day",
    ) -> dict[str, Any]:
        activities = self._list_filtered_intelligence_activities(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
            topic=topic,
        )
        timeline: dict[str, dict[str, Any]] = {}
        for activity in activities:
            published_at = _trimmed(activity.get("published_at"))
            if not published_at:
                continue
            try:
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            except ValueError:
                continue
            if bucket == "week":
                bucket_key = dt.date().isoformat()
            else:
                bucket_key = dt.date().isoformat()
            entry = timeline.setdefault(
                bucket_key,
                {"bucket": bucket_key, "total": 0, "positive": 0, "neutral": 0, "negative": 0},
            )
            entry["total"] += 1
            entry[_sentiment_bucket(activity)] += 1
        items = [timeline[key] for key in sorted(timeline.keys())]
        return {"items": items}

    def get_topic_intelligence(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        activities = self._list_filtered_intelligence_activities(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
        )
        aggregated: dict[str, dict[str, Any]] = {}
        for activity in activities:
            payload = dict((activity.get("analysis") or {}).get("analysis_payload") or {})
            entity_name = _trimmed((activity.get("entity") or {}).get("name")) or "Unknown"
            summary = _analysis_text(payload, "summary") or _trimmed(activity.get("text_content")) or ""
            for topic_name in _analysis_list(payload, "topics"):
                record = aggregated.setdefault(
                    topic_name,
                    {
                        "topic": topic_name,
                        "count": 0,
                        "sentimentScoreTotal": 0.0,
                        "positive": 0,
                        "neutral": 0,
                        "negative": 0,
                        "entities": Counter(),
                        "platforms": Counter(),
                        "sampleSummary": summary,
                    },
                )
                record["count"] += 1
                record["sentimentScoreTotal"] += _sentiment_score(activity)
                record[_sentiment_bucket(activity)] += 1
                record["entities"][entity_name] += 1
                record["platforms"][_trimmed(activity.get("platform")) or "unknown"] += 1
                if summary and len(summary) > len(record["sampleSummary"]):
                    record["sampleSummary"] = summary
        items = []
        for value in aggregated.values():
            count = max(1, int(value["count"]))
            items.append(
                {
                    "topic": value["topic"],
                    "count": count,
                    "avgSentimentScore": round(float(value["sentimentScoreTotal"]) / count, 4),
                    "sentimentCounts": {
                        "positive": value["positive"],
                        "neutral": value["neutral"],
                        "negative": value["negative"],
                    },
                    "topEntities": [name for name, _ in value["entities"].most_common(3)],
                    "topPlatforms": [name for name, _ in value["platforms"].most_common(2)],
                    "sampleSummary": value["sampleSummary"],
                }
            )
        items.sort(key=lambda item: (-item["count"], item["topic"]))
        return {"items": items[:limit]}

    def get_topic_metric_enrichment(
        self,
        topic_names: list[str],
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
        max_rows: int = 5000,
        evidence_per_topic: int = 3,
    ) -> dict[str, dict[str, Any]]:
        normalized_topics = {
            _trimmed(topic_name).lower(): _trimmed(topic_name)
            for topic_name in topic_names
            if _trimmed(topic_name)
        }
        if not normalized_topics:
            return {}

        activities = self._list_filtered_intelligence_activities(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
            source_kind="post",
            limit=max(1, min(int(max_rows or 5000), 10000)),
        )
        enrichment: dict[str, dict[str, Any]] = {
            canonical: {
                "engagementTotal": 0,
                "likes": 0,
                "comments": 0,
                "shares": 0,
                "views": 0,
                "reactions": 0,
                "evidenceCount": 0,
                "sampleSummary": "",
                "evidence": [],
            }
            for canonical in normalized_topics.values()
        }
        for activity in activities:
            payload = dict((activity.get("analysis") or {}).get("analysis_payload") or {})
            activity_topics = {_trimmed(value).lower() for value in _analysis_list(payload, "topics")}
            matched_topics = activity_topics.intersection(normalized_topics.keys())
            if not matched_topics:
                continue

            metrics = dict(activity.get("metrics") or {})
            likes = int(metrics.get("likes") or metrics.get("like_count") or metrics.get("reactions") or 0)
            comments = int(metrics.get("comments") or metrics.get("comment_count") or 0)
            shares = int(metrics.get("shares") or metrics.get("share_count") or 0)
            views = int(metrics.get("views") or metrics.get("impressions") or 0)
            reactions = int(metrics.get("reactions") or metrics.get("reaction_count") or likes)
            engagement_total = _engagement_total(activity)
            summary = _analysis_text(payload, "summary") or _trimmed(activity.get("text_content")) or ""
            evidence_item = {
                "activity_uid": activity.get("activity_uid"),
                "entity": (activity.get("entity") or {}).get("name"),
                "platform": activity.get("platform"),
                "published_at": activity.get("published_at"),
                "summary": summary,
                "source_url": activity.get("source_url"),
                "metrics": {
                    "likes": likes,
                    "comments": comments,
                    "shares": shares,
                    "views": views,
                    "reactions": reactions,
                    "engagementTotal": engagement_total,
                },
            }
            for normalized_topic in matched_topics:
                record = enrichment[normalized_topics[normalized_topic]]
                record["engagementTotal"] += engagement_total
                record["likes"] += likes
                record["comments"] += comments
                record["shares"] += shares
                record["views"] += views
                record["reactions"] += reactions
                record["evidenceCount"] += 1
                if summary and len(summary) > len(record["sampleSummary"]):
                    record["sampleSummary"] = summary
                if len(record["evidence"]) < max(0, min(int(evidence_per_topic or 3), 10)):
                    record["evidence"].append(evidence_item)
        return enrichment

    def get_graph_sync_coverage(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
        source_kind: str = "post",
        limit: int = 10000,
    ) -> dict[str, Any]:
        filters: list[tuple[str, str, Any]] = []
        start_iso, end_iso = _published_bounds(from_date, to_date)
        if entity_id:
            filters.append(("eq", "entity_id", entity_id))
        if platform and platform != "all":
            filters.append(("eq", "platform", _normalize_platform(platform)))
        if source_kind:
            filters.append(("eq", "source_kind", _trimmed(source_kind).lower()))
        if start_iso:
            filters.append(("gte", "published_at", start_iso))
        if end_iso:
            filters.append(("lte", "published_at", end_iso))

        row_limit = max(1, min(int(limit or 10000), 20000))
        rows = self._select_rows(
            "social_activities",
            columns="id,analysis_status,graph_status",
            filters=filters,
            order_by="published_at",
            desc=True,
            limit=row_limit,
        )
        total = len(rows)
        analyzed = sum(1 for row in rows if row.get("analysis_status") == "analyzed")
        graph_synced = sum(1 for row in rows if row.get("graph_status") == "synced")
        failed = sum(1 for row in rows if row.get("graph_status") in {"failed", "dead_letter"})
        pending = max(0, analyzed - graph_synced)
        return {
            "totalParentActivities": total,
            "analyzedParentActivities": analyzed,
            "graphSyncedParentActivities": graph_synced,
            "graphPendingParentActivities": pending,
            "failedParentActivities": failed,
            "semanticCoveragePct": round((graph_synced / total) * 100, 1) if total else 0.0,
            "rowCap": row_limit,
            "rowCapReached": total >= row_limit,
        }

    def get_ad_intelligence(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
        cta_type: str | None = None,
        content_format: str | None = None,
        sort: str = "recent",
        page: int = 1,
        size: int = 20,
    ) -> dict[str, Any]:
        activities = self._list_filtered_intelligence_activities(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
            cta_type=cta_type,
            content_format=content_format,
        )
        ads = [
            activity
            for activity in activities
            if (activity.get("source_kind") or "").lower() == "ad" or (activity.get("platform") or "").lower() == "google"
        ]
        if sort == "engagement":
            ads.sort(key=lambda item: (-_engagement_total(item), _trimmed(item.get("published_at"))), reverse=False)
        elif sort == "entity":
            ads.sort(key=lambda item: ((_trimmed((item.get("entity") or {}).get("name")) or "zzz"), _trimmed(item.get("published_at"))), reverse=False)
            ads.reverse()
        else:
            ads.sort(key=lambda item: _trimmed(item.get("published_at")), reverse=True)

        items = []
        intent_counter = Counter()
        cta_counter = Counter()
        product_counter = Counter()
        for activity in ads:
            analysis = dict(activity.get("analysis") or {})
            payload = dict(analysis.get("analysis_payload") or {})
            intent = _analysis_text(payload, "marketing_intent")
            if intent:
                intent_counter[intent] += 1
            cta = _trimmed(activity.get("cta_type"))
            if cta:
                cta_counter[cta] += 1
            for product in _analysis_list(payload, "products"):
                product_counter[product] += 1
            items.append(
                {
                    **activity,
                    "engagementTotal": _engagement_total(activity),
                    "analysisHighlights": {
                        "marketingIntent": intent,
                        "products": _analysis_list(payload, "products"),
                        "valuePropositions": _analysis_list(payload, "value_propositions"),
                        "urgencyIndicators": _analysis_list(payload, "urgency_indicators"),
                    },
                }
            )
        start = max(0, (max(1, page) - 1) * max(1, size))
        end = start + max(1, size)
        return {
            "count": len(items),
            "items": items[start:end],
            "summary": {
                "topMarketingIntent": intent_counter.most_common(1)[0][0] if intent_counter else None,
                "topCtaType": cta_counter.most_common(1)[0][0] if cta_counter else None,
                "topProduct": product_counter.most_common(1)[0][0] if product_counter else None,
            },
        }

    def get_audience_response(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        activities = self._list_filtered_intelligence_activities(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
        )
        entity_rows: dict[str, dict[str, Any]] = {}
        pain_point_rows: dict[str, dict[str, Any]] = {}
        customer_intent_rows: dict[str, dict[str, Any]] = {}

        for activity in activities:
            entity = dict(activity.get("entity") or {})
            entity_name = _trimmed(entity.get("name")) or "Unknown"
            entity_key = _trimmed(entity.get("id")) or entity_name
            analysis = dict(activity.get("analysis") or {})
            payload = dict(analysis.get("analysis_payload") or {})
            bucket = _sentiment_bucket(activity)
            score = _sentiment_score(activity)

            entity_entry = entity_rows.setdefault(
                entity_key,
                {
                    "entityId": entity.get("id"),
                    "entityName": entity_name,
                    "total": 0,
                    "positive": 0,
                    "neutral": 0,
                    "negative": 0,
                    "avgSentimentScoreTotal": 0.0,
                },
            )
            entity_entry["total"] += 1
            entity_entry[bucket] += 1
            entity_entry["avgSentimentScoreTotal"] += score

            for label in _analysis_list(payload, "pain_points"):
                row = pain_point_rows.setdefault(
                    label,
                    {"label": label, "count": 0, "entities": Counter(), "positive": 0, "neutral": 0, "negative": 0},
                )
                row["count"] += 1
                row["entities"][entity_name] += 1
                row[bucket] += 1

            customer_intent_label = _analysis_text(payload, "customer_intent")
            if customer_intent_label:
                row = customer_intent_rows.setdefault(
                    customer_intent_label,
                    {"label": customer_intent_label, "count": 0, "entities": Counter(), "positive": 0, "neutral": 0, "negative": 0},
                )
                row["count"] += 1
                row["entities"][entity_name] += 1
                row[bucket] += 1

        entity_items = []
        for row in entity_rows.values():
            total = max(1, int(row["total"]))
            entity_items.append(
                {
                    "entityId": row["entityId"],
                    "entityName": row["entityName"],
                    "total": total,
                    "positive": row["positive"],
                    "neutral": row["neutral"],
                    "negative": row["negative"],
                    "avgSentimentScore": round(float(row["avgSentimentScoreTotal"]) / total, 4),
                }
            )
        entity_items.sort(key=lambda row: (-row["negative"], row["entityName"]))

        def _ranked_signal_items(rows: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
            items = []
            for row in rows.values():
                dominant_sentiment = max(
                    ("positive", row["positive"]),
                    ("neutral", row["neutral"]),
                    ("negative", row["negative"]),
                    key=lambda item: item[1],
                )[0]
                items.append(
                    {
                        "label": row["label"],
                        "count": row["count"],
                        "entities": [name for name, _ in row["entities"].most_common(3)],
                        "dominantSentiment": dominant_sentiment,
                    }
                )
            items.sort(key=lambda item: (-item["count"], item["label"]))
            return items[:limit]

        return {
            "entitySentiment": entity_items,
            "painPoints": _ranked_signal_items(pain_point_rows),
            "customerIntent": _ranked_signal_items(customer_intent_rows),
        }

    def get_competitor_scorecard(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        platform: str | None = None,
        sort_by: str = "posts",
        sort_dir: str = "desc",
    ) -> dict[str, Any]:
        activities = self._list_filtered_intelligence_activities(
            from_date=from_date,
            to_date=to_date,
            platform=platform,
        )
        rows: dict[str, dict[str, Any]] = {}
        for activity in activities:
            entity = dict(activity.get("entity") or {})
            entity_id = _trimmed(entity.get("id")) or _trimmed(activity.get("entity_id"))
            if not entity_id:
                continue
            entity_name = _trimmed(entity.get("name")) or "Unknown"
            analysis = dict(activity.get("analysis") or {})
            payload = dict(analysis.get("analysis_payload") or {})
            row = rows.setdefault(
                entity_id,
                {
                    "entityId": entity_id,
                    "entityName": entity_name,
                    "posts": 0,
                    "adsRunning": 0,
                    "sentimentTotal": 0.0,
                    "marketingIntent": Counter(),
                    "topics": Counter(),
                    "valueProps": Counter(),
                    "products": Counter(),
                    "recentActivities": [],
                },
            )
            row["posts"] += 1
            row["sentimentTotal"] += _sentiment_score(activity)
            if (activity.get("source_kind") or "").lower() == "ad" or (activity.get("platform") or "").lower() == "google":
                row["adsRunning"] += 1
            intent = _analysis_text(payload, "marketing_intent")
            if intent:
                row["marketingIntent"][intent] += 1
            for topic_name in _analysis_list(payload, "topics"):
                row["topics"][topic_name] += 1
            for value_prop in _analysis_list(payload, "value_propositions"):
                row["valueProps"][value_prop] += 1
            for product in _analysis_list(payload, "products"):
                row["products"][product] += 1
            if len(row["recentActivities"]) < 5:
                row["recentActivities"].append(activity)

        items = []
        for row in rows.values():
            posts = max(1, int(row["posts"]))
            items.append(
                {
                    "entityId": row["entityId"],
                    "entityName": row["entityName"],
                    "posts": row["posts"],
                    "adsRunning": row["adsRunning"],
                    "avgSentimentScore": round(float(row["sentimentTotal"]) / posts, 4),
                    "topMarketingIntent": row["marketingIntent"].most_common(1)[0][0] if row["marketingIntent"] else None,
                    "keyTopics": [name for name, _ in row["topics"].most_common(3)],
                    "valueProps": [name for name, _ in row["valueProps"].most_common(2)],
                    "productsPromoted": [name for name, _ in row["products"].most_common(2)],
                    "recentActivities": row["recentActivities"],
                }
            )
        reverse = sort_dir.lower() != "asc"
        items.sort(key=lambda item: item.get(sort_by) if sort_by in item else item["posts"], reverse=reverse)
        return {"items": items}

    def get_intelligence_evidence(
        self,
        *,
        from_date: str | None = None,
        to_date: str | None = None,
        activity_uid: str | None = None,
        entity_id: str | None = None,
        platform: str | None = None,
        topic: str | None = None,
        marketing_intent: str | None = None,
        pain_point: str | None = None,
        customer_intent: str | None = None,
        source_kind: str | None = None,
        cta_type: str | None = None,
        content_format: str | None = None,
        sentiment: str | None = None,
        page: int = 1,
        size: int = 25,
    ) -> dict[str, Any]:
        activities = self._list_filtered_intelligence_activities(
            from_date=from_date,
            to_date=to_date,
            activity_uid=activity_uid,
            entity_id=entity_id,
            platform=platform,
            source_kind=source_kind,
            cta_type=cta_type,
            content_format=content_format,
            topic=topic,
            marketing_intent=marketing_intent,
            pain_point=pain_point,
            customer_intent=customer_intent,
            sentiment=sentiment,
        )
        start = max(0, (max(1, page) - 1) * max(1, size))
        end = start + max(1, size)
        return {
            "count": len(activities),
            "items": activities[start:end],
        }

    def get_activity_by_uid(self, activity_uid: str, *, include_analysis: bool = True) -> dict[str, Any] | None:
        row = self._single_row("social_activities", filters=(("eq", "activity_uid", activity_uid),))
        if not row:
            return None
        entity = self._single_row("social_entities", filters=(("eq", "id", row.get("entity_id")),))
        if entity:
            row["entity"] = entity
        if include_analysis:
            analysis = self._single_row(
                "social_activity_analysis",
                filters=(("eq", "activity_id", row.get("id")),),
            )
            if analysis:
                row["analysis"] = analysis
        return row

    def prepare_activity_replay(self, activity_uids: list[str], *, stage: str) -> list[dict[str, Any]]:
        if not activity_uids:
            return []
        normalized_stage = _trimmed(stage).lower() or "analysis"
        if normalized_stage not in {"analysis", "graph"}:
            raise ValueError("Replay stage must be analysis or graph")
        update_payload: dict[str, Any] = {"last_error": None}
        if normalized_stage == "analysis":
            update_payload.update(
                {
                    "analysis_status": "pending",
                    "graph_status": "not_ready",
                    "analysis_claimed_at": None,
                    "analysis_claimed_by": None,
                    "graph_claimed_at": None,
                    "graph_claimed_by": None,
                }
            )
        else:
            update_payload.update(
                {
                    "graph_status": "pending",
                    "graph_claimed_at": None,
                    "graph_claimed_by": None,
                }
            )
        for activity_uid in activity_uids:
            self.client.table("social_activities").update(update_payload).eq("activity_uid", activity_uid).execute()
            if normalized_stage == "analysis":
                self.clear_failure(stage="analysis", scope_key=activity_uid)
                self.clear_failure(stage="graph", scope_key=activity_uid)
            else:
                self.clear_failure(stage="graph", scope_key=activity_uid)
        return [
            item
            for item in (self.get_activity_by_uid(activity_uid, include_analysis=True) for activity_uid in activity_uids)
            if item
        ]

    def get_overview(self) -> dict[str, Any]:
        entities = self.list_entities()
        activities = self.list_activities(limit=200)
        failures = self.list_failures(limit=50)
        runs = self.list_recent_runs(limit=12)

        activity_count = len(activities)
        active_entities = sum(1 for entity in entities if entity.get("is_active"))
        platform_counts = Counter(row.get("platform") or "unknown" for row in activities)
        recent_counts = Counter(
            (row.get("analysis_status") or "unknown")
            for row in activities
        )
        account_health_counts = Counter()
        for entity in entities:
            for account in entity.get("accounts") or []:
                account_health_counts[_normalize_health_status(account.get("health_status"))] += 1
        stale_entities = []
        last_seen_by_entity: dict[str, str] = {}
        for activity in activities:
            entity = activity.get("entity") or {}
            entity_id = entity.get("id")
            if entity_id and activity.get("last_seen_at"):
                current = last_seen_by_entity.get(entity_id)
                if not current or str(activity["last_seen_at"]) > current:
                    last_seen_by_entity[entity_id] = str(activity["last_seen_at"])
        now = datetime.now(timezone.utc)
        for entity in entities:
            last_seen = last_seen_by_entity.get(entity["id"])
            if not last_seen:
                stale_entities.append(
                    {
                        "entity_id": entity["id"],
                        "name": entity.get("name"),
                        "reason": "never_collected",
                    }
                )
                continue
            try:
                age_hours = (now - datetime.fromisoformat(last_seen.replace("Z", "+00:00"))).total_seconds() / 3600.0
            except Exception:
                age_hours = 0.0
            if age_hours > 48:
                stale_entities.append(
                    {
                        "entity_id": entity["id"],
                        "name": entity.get("name"),
                        "reason": "stale_refresh",
                        "age_hours": round(age_hours, 1),
                    }
                )

        return {
            "entities_total": len(entities),
            "entities_active": active_entities,
            "activities_total": activity_count,
            "platform_counts": dict(platform_counts),
            "analysis_status_counts": dict(recent_counts),
            "account_health_counts": dict(account_health_counts),
            "queue_depth": {
                "analysis": sum(1 for row in activities if row.get("analysis_status") in {"pending", "failed"}),
                "graph": sum(1 for row in activities if row.get("graph_status") in {"pending", "failed"}),
            },
            "dead_letter_failures": sum(1 for row in failures if row.get("is_dead_letter")),
            "recent_failures": failures[:10],
            "stale_entities": stale_entities[:10],
            "recent_runs": runs[:6],
        }
