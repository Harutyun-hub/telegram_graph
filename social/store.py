from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

from loguru import logger
from supabase import Client, create_client

import config

SUPPORTED_SOCIAL_PLATFORMS = ("facebook", "instagram", "google", "tiktok")
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


def _serialize_metadata(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _normalize_health_status(value: Any) -> str:
    status = _trimmed(value).lower() or "unknown"
    return status if status in ACCOUNT_HEALTH_STATUSES else "unknown"


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
                "facebook_page_id,instagram_username,google_ads_domain"
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
                        "account_external_id": _clean_optional(company.get("facebook_page_id")),
                        "account_handle": None,
                        "domain": None,
                        "import_source": "companies_seed",
                        "metadata": {"seeded_from": "companies.facebook_page_id"},
                        "is_active": bool(company.get("is_active", True)),
                    }
                )
            if _clean_optional(company.get("instagram_username")):
                account_payloads.append(
                    {
                        "entity_id": entity_id,
                        "platform": "instagram",
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
                on_conflict="entity_id,platform",
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
            entity["platform_accounts"] = {
                platform: next((row for row in accounts if row.get("platform") == platform), None)
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
            payloads.append(
                {
                    "entity_id": entity_id,
                    "platform": platform,
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
            on_conflict="entity_id,platform",
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
        entity_id, separator, platform = scope_key.partition(":")
        if not separator or not entity_id or not platform:
            return None
        row = self._single_row(
            "social_entity_accounts",
            filters=(("eq", "entity_id", entity_id), ("eq", "platform", _normalize_platform(platform))),
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
            scope_key=f"{account['entity_id']}:{account['platform']}",
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
