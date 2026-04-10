"""
supabase_writer.py — All Supabase read/write operations.

Central data access layer. All other modules call this — never 
touch Supabase directly from scrapers or processors.
"""
from __future__ import annotations
from collections import defaultdict
from contextlib import contextmanager
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import json
import ssl
import uuid
from urllib.request import urlopen
from loguru import logger
import config
from utils.topic_normalizer import set_runtime_topic_aliases

try:
    import certifi
except Exception:  # pragma: no cover
    certifi = None  # type: ignore[assignment]

try:  # pragma: no cover - optional until pipeline queue workers use direct Postgres
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


def _parse_iso_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        normalized = text.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(normalized)
        except Exception:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _looks_like_versioned_runtime_json_name(name: str) -> bool:
    text = str(name or "")
    return (
        len(text) >= 17
        and text[:8].isdigit()
        and text[8] == "T"
        and text[9:15].isdigit()
        and text[15] == "Z"
        and text.endswith(".json")
    )


class SupabaseWriter:

    def __init__(self):
        self.client: Client = create_client(
            config.SUPABASE_URL,
            config.SUPABASE_SERVICE_ROLE_KEY
        )
        self._failure_table_warning_emitted = False
        self._topic_review_warning_emitted = False
        self._topic_promotion_warning_emitted = False
        self._resolution_jobs_warning_emitted = False
        self._resolution_slots_warning_emitted = False
        self._peer_refs_warning_emitted = False
        self._pipeline_queue_warning_emitted: set[str] = set()
        self._runtime_bucket_name = "runtime-config"
        self._scheduler_settings_path = "scraper/scheduler_settings.json"
        self.refresh_runtime_topic_aliases()

    def _warn_failure_table_once(self, error: Exception):
        if self._failure_table_warning_emitted:
            return
        self._failure_table_warning_emitted = True
        logger.warning(
            "ai_processing_failures table unavailable; retry/dead-letter safeguards disabled until migration is applied "
            f"({error})"
        )

    def _warn_topic_review_table_once(self, error: Exception):
        if self._topic_review_warning_emitted:
            return
        self._topic_review_warning_emitted = True
        logger.warning(
            "topic_review_queue table unavailable; proposed-topic review queue is disabled until migration is applied "
            f"({error})"
        )

    def _warn_topic_promotion_table_once(self, error: Exception):
        if self._topic_promotion_warning_emitted:
            return
        self._topic_promotion_warning_emitted = True
        logger.warning(
            "topic_taxonomy_promotions table unavailable; runtime taxonomy promotions are disabled until migration is applied "
            f"({error})"
        )

    def _warn_resolution_jobs_table_once(self, error: Exception):
        if self._resolution_jobs_warning_emitted:
            return
        self._resolution_jobs_warning_emitted = True
        logger.warning(
            "telegram_source_resolution_jobs table unavailable; source resolution queue is disabled until migration is applied "
            f"({error})"
        )

    def _warn_resolution_slots_table_once(self, error: Exception):
        if self._resolution_slots_warning_emitted:
            return
        self._resolution_slots_warning_emitted = True
        logger.warning(
            "telegram_session_slots table unavailable; source resolution session pacing is disabled until migration is applied "
            f"({error})"
        )

    def _warn_peer_refs_table_once(self, error: Exception):
        if self._peer_refs_warning_emitted:
            return
        self._peer_refs_warning_emitted = True
        logger.warning(
            "telegram_channel_peer_refs table unavailable; peer-ref lookup is disabled until migration is applied "
            f"({error})"
        )

    def _warn_pipeline_queue_once(self, queue_name: str, error: Exception | str):
        key = str(queue_name or "pipeline_queue")
        if key in self._pipeline_queue_warning_emitted:
            return
        self._pipeline_queue_warning_emitted.add(key)
        logger.warning(f"{key} unavailable; pipeline queue helpers disabled until runtime config is ready ({error})")

    def _ensure_runtime_bucket(self):
        """Ensure runtime config bucket exists in Supabase Storage."""
        buckets = self.client.storage.list_buckets()
        names = []
        for bucket in buckets:
            if isinstance(bucket, dict):
                names.append(bucket.get("name"))
            else:
                names.append(getattr(bucket, "name", None))

        if self._runtime_bucket_name not in names:
            self.client.storage.create_bucket(
                self._runtime_bucket_name,
                self._runtime_bucket_name,
                {"public": False},
            )

    def get_scraper_scheduler_settings(self, default_interval_minutes: int = 15) -> dict:
        """Read persisted scraper scheduler config from Supabase Storage."""
        default = {
            "is_active": False,
            "interval_minutes": int(default_interval_minutes),
            "updated_at": None,
        }

        try:
            self._ensure_runtime_bucket()
            raw = self.client.storage.from_(self._runtime_bucket_name).download(self._scheduler_settings_path)
            if not raw:
                return default
            parsed = json.loads(raw.decode("utf-8"))
            interval = int(parsed.get("interval_minutes", default_interval_minutes))
            if interval < 1:
                interval = int(default_interval_minutes)
            return {
                "is_active": bool(parsed.get("is_active", False)),
                "interval_minutes": interval,
                "updated_at": parsed.get("updated_at"),
            }
        except Exception:
            return default

    def save_scraper_scheduler_settings(self, *, is_active: bool, interval_minutes: int) -> dict:
        """Persist scraper scheduler config to Supabase Storage."""
        self._ensure_runtime_bucket()
        payload = {
            "is_active": bool(is_active),
            "interval_minutes": int(interval_minutes),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.client.storage.from_(self._runtime_bucket_name).upload(
            self._scheduler_settings_path,
            json.dumps(payload, ensure_ascii=True).encode("utf-8"),
            {"content-type": "application/json", "upsert": "true"},
        )
        return payload

    # ── Source Resolution ────────────────────────────────────────────────────

    def get_source_resolution_slot(self, slot_key: str) -> dict | None:
        key = str(slot_key or "").strip() or "primary"
        try:
            res = self.client.table("telegram_session_slots") \
                .select("*") \
                .eq("slot_key", key) \
                .limit(1) \
                .execute()
            return res.data[0] if res.data else None
        except Exception as e:
            self._warn_resolution_slots_table_once(e)
            return None

    def list_source_resolution_slots(self, *, active_only: bool = False) -> list[dict]:
        try:
            query = self.client.table("telegram_session_slots") \
                .select("*") \
                .order("priority", desc=False)
            if active_only:
                query = query.eq("is_active", True)
            res = query.execute()
            return res.data or []
        except Exception as e:
            self._warn_resolution_slots_table_once(e)
            return []

    def ensure_source_resolution_slot(
        self,
        slot_key: str = "primary",
        *,
        is_active: bool = True,
        priority: int = 100,
        min_resolve_interval_seconds: int | None = None,
        max_concurrent_resolves: int = 1,
    ) -> dict:
        key = str(slot_key or "").strip() or "primary"
        existing = self.get_source_resolution_slot(key)
        if existing:
            return existing

        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            "slot_key": key,
            "is_active": bool(is_active),
            "priority": int(priority),
            "min_resolve_interval_seconds": int(
                min_resolve_interval_seconds or config.SOURCE_RESOLUTION_MIN_INTERVAL_SECONDS
            ),
            "max_concurrent_resolves": max(1, int(max_concurrent_resolves)),
            "created_at": now_iso,
            "updated_at": now_iso,
        }
        try:
            res = self.client.table("telegram_session_slots") \
                .insert(payload) \
                .execute()
            if res.data:
                return res.data[0]
        except Exception as e:
            self._warn_resolution_slots_table_once(e)
        return payload

    def update_source_resolution_slot(self, slot_key: str, payload: dict) -> dict | None:
        key = str(slot_key or "").strip() or "primary"
        if not payload:
            return self.get_source_resolution_slot(key)
        update_payload = dict(payload)
        update_payload["updated_at"] = datetime.now(timezone.utc).isoformat()
        try:
            res = self.client.table("telegram_session_slots") \
                .update(update_payload) \
                .eq("slot_key", key) \
                .execute()
            if res.data:
                return res.data[0]
            return self.get_source_resolution_slot(key)
        except Exception as e:
            self._warn_resolution_slots_table_once(e)
            return None

    def get_channel_peer_ref(self, channel_uuid: str, session_slot: str = "primary") -> dict | None:
        if not channel_uuid:
            return None
        try:
            res = self.client.table("telegram_channel_peer_refs") \
                .select("*") \
                .eq("channel_id", channel_uuid) \
                .eq("session_slot", session_slot) \
                .limit(1) \
                .execute()
            return res.data[0] if res.data else None
        except Exception as e:
            self._warn_peer_refs_table_once(e)
            return None

    def upsert_channel_peer_ref(self, channel_uuid: str, session_slot: str, payload: dict) -> dict | None:
        if not channel_uuid:
            return None
        peer_id = payload.get("peer_id")
        access_hash = payload.get("access_hash")
        if peer_id is None or access_hash is None:
            return None

        now_iso = datetime.now(timezone.utc).isoformat()
        row = {
            "channel_id": channel_uuid,
            "session_slot": str(session_slot or "").strip() or "primary",
            "peer_id": int(peer_id),
            "access_hash": int(access_hash),
            "resolved_username": (payload.get("resolved_username") or None),
            "resolved_at": payload.get("resolved_at") or now_iso,
            "last_verified_at": payload.get("last_verified_at") or now_iso,
            "updated_at": now_iso,
            "created_at": now_iso,
        }
        try:
            res = self.client.table("telegram_channel_peer_refs") \
                .upsert(row, on_conflict="channel_id,session_slot") \
                .execute()
            if res.data:
                return res.data[0]
        except Exception as e:
            self._warn_peer_refs_table_once(e)
        return None

    def list_channels_missing_peer_refs(
        self,
        *,
        session_slot: str = "primary",
        active_only: bool = False,
        limit: int = 100,
    ) -> list[dict]:
        try:
            channels = self.get_active_channels() if active_only else self.list_channels()
        except Exception:
            channels = []
        if not channels:
            return []

        refs = {}
        try:
            res = self.client.table("telegram_channel_peer_refs") \
                .select("channel_id, session_slot") \
                .eq("session_slot", session_slot) \
                .execute()
            refs = {
                str(item.get("channel_id") or ""): item
                for item in (res.data or [])
                if item.get("channel_id")
            }
        except Exception as e:
            self._warn_peer_refs_table_once(e)
            return []

        missing: list[dict] = []
        for channel in channels:
            channel_id = str(channel.get("id") or "").strip()
            if not channel_id or channel_id in refs:
                continue
            missing.append(channel)
            if len(missing) >= max(1, int(limit)):
                break
        return missing

    def get_source_resolution_job(self, channel_uuid: str, job_kind: str = "resolve_metadata") -> dict | None:
        if not channel_uuid:
            return None
        try:
            res = self.client.table("telegram_source_resolution_jobs") \
                .select("*") \
                .eq("channel_id", channel_uuid) \
                .eq("job_kind", job_kind) \
                .limit(1) \
                .execute()
            return res.data[0] if res.data else None
        except Exception as e:
            self._warn_resolution_jobs_table_once(e)
            return None

    def list_source_resolution_jobs(
        self,
        *,
        statuses: list[str] | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        try:
            query = self.client.table("telegram_source_resolution_jobs") \
                .select("*") \
                .order("priority", desc=False) \
                .order("next_attempt_at", desc=False)
            normalized_statuses = [str(value or "").strip() for value in (statuses or []) if str(value or "").strip()]
            if normalized_statuses:
                query = query.in_("status", normalized_statuses)
            if limit is not None:
                query = query.limit(max(1, int(limit)))
            res = query.execute()
            return res.data or []
        except Exception as e:
            self._warn_resolution_jobs_table_once(e)
            return []

    def enqueue_source_resolution_job(
        self,
        channel_uuid: str,
        *,
        job_kind: str = "resolve_metadata",
        priority: int = 30,
        next_attempt_at: str | None = None,
    ) -> dict | None:
        if not channel_uuid:
            return None
        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            "channel_id": channel_uuid,
            "job_kind": str(job_kind or "").strip() or "resolve_metadata",
            "priority": int(priority),
            "status": "pending",
            "next_attempt_at": next_attempt_at or now_iso,
            "lease_token": None,
            "lease_expires_at": None,
            "last_error_code": None,
            "last_error_message": None,
            "finished_at": None,
            "updated_at": now_iso,
        }
        existing = self.get_source_resolution_job(channel_uuid, payload["job_kind"])
        try:
            if existing and existing.get("id"):
                payload["attempt_count"] = int(existing.get("attempt_count") or 0)
                res = self.client.table("telegram_source_resolution_jobs") \
                    .update(payload) \
                    .eq("id", existing["id"]) \
                    .execute()
            else:
                payload["attempt_count"] = 0
                payload["created_at"] = now_iso
                res = self.client.table("telegram_source_resolution_jobs") \
                    .insert(payload) \
                    .execute()
            if res.data:
                return res.data[0]
        except Exception as e:
            self._warn_resolution_jobs_table_once(e)
        return existing

    def claim_due_source_resolution_jobs(
        self,
        *,
        limit: int,
        lease_seconds: int,
    ) -> list[dict]:
        now = datetime.now(timezone.utc)
        lease_expires_at = (now + timedelta(seconds=max(30, int(lease_seconds)))).isoformat()
        due_jobs = []
        jobs = self.list_source_resolution_jobs(statuses=["pending", "leased"], limit=max(10, int(limit) * 5))
        for job in jobs:
            status = str(job.get("status") or "").strip().lower()
            next_attempt_at = _parse_iso_datetime(job.get("next_attempt_at")) or now
            lease_expires = _parse_iso_datetime(job.get("lease_expires_at"))
            lease_active = status == "leased" and lease_expires is not None and lease_expires > now
            if lease_active:
                continue
            if next_attempt_at > now:
                continue
            due_jobs.append(job)

        claimed: list[dict] = []
        for job in due_jobs[:max(1, int(limit))]:
            token = str(uuid.uuid4())
            try:
                res = self.client.table("telegram_source_resolution_jobs") \
                    .update(
                        {
                            "status": "leased",
                            "lease_token": token,
                            "lease_expires_at": lease_expires_at,
                            "updated_at": now.isoformat(),
                            "finished_at": None,
                        }
                    ) \
                    .eq("id", job["id"]) \
                    .execute()
                if res.data:
                    claimed.append(res.data[0])
            except Exception as e:
                self._warn_resolution_jobs_table_once(e)
                break
        return claimed

    def complete_source_resolution_job(self, job_id: str, *, attempt_count: int | None = None) -> dict | None:
        if not job_id:
            return None
        payload = {
            "status": "done",
            "lease_token": None,
            "lease_expires_at": None,
            "last_error_code": None,
            "last_error_message": None,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if attempt_count is not None:
            payload["attempt_count"] = int(attempt_count)
        try:
            res = self.client.table("telegram_source_resolution_jobs") \
                .update(payload) \
                .eq("id", job_id) \
                .execute()
            return res.data[0] if res.data else None
        except Exception as e:
            self._warn_resolution_jobs_table_once(e)
            return None

    def requeue_source_resolution_job(
        self,
        job_id: str,
        *,
        attempt_count: int,
        next_attempt_at: str,
        last_error_code: str | None,
        last_error_message: str | None,
    ) -> dict | None:
        if not job_id:
            return None
        try:
            res = self.client.table("telegram_source_resolution_jobs") \
                .update(
                    {
                        "status": "pending",
                        "attempt_count": int(attempt_count),
                        "next_attempt_at": next_attempt_at,
                        "lease_token": None,
                        "lease_expires_at": None,
                        "last_error_code": (last_error_code or None),
                        "last_error_message": (last_error_message or None),
                        "finished_at": None,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    }
                ) \
                .eq("id", job_id) \
                .execute()
            return res.data[0] if res.data else None
        except Exception as e:
            self._warn_resolution_jobs_table_once(e)
            return None

    def dead_letter_source_resolution_job(
        self,
        job_id: str,
        *,
        attempt_count: int,
        last_error_code: str | None,
        last_error_message: str | None,
    ) -> dict | None:
        if not job_id:
            return None
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            res = self.client.table("telegram_source_resolution_jobs") \
                .update(
                    {
                        "status": "dead_letter",
                        "attempt_count": int(attempt_count),
                        "lease_token": None,
                        "lease_expires_at": None,
                        "last_error_code": (last_error_code or None),
                        "last_error_message": (last_error_message or None),
                        "finished_at": now_iso,
                        "updated_at": now_iso,
                    }
                ) \
                .eq("id", job_id) \
                .execute()
            return res.data[0] if res.data else None
        except Exception as e:
            self._warn_resolution_jobs_table_once(e)
            return None

    def get_channels_by_ids(self, channel_ids: list[str]) -> dict[str, dict]:
        ids = [str(channel_id).strip() for channel_id in (channel_ids or []) if str(channel_id).strip()]
        if not ids:
            return {}
        try:
            res = self.client.table("telegram_channels") \
                .select("*") \
                .in_("id", ids) \
                .execute()
            return {
                str(item.get("id")): item
                for item in (res.data or [])
                if item.get("id")
            }
        except Exception:
            return {}

    def get_source_resolution_snapshot(self, *, session_slot: str = "primary") -> dict:
        now = datetime.now(timezone.utc)
        jobs = self.list_source_resolution_jobs(limit=5000)
        due_jobs = 0
        leased_jobs = 0
        dead_letter_jobs = 0
        stale_nonclaimable_jobs = 0
        oldest_due_age_seconds: int | None = None

        for job in jobs:
            status = str(job.get("status") or "").strip().lower()
            next_attempt_at = _parse_iso_datetime(job.get("next_attempt_at")) or now
            lease_expires_at = _parse_iso_datetime(job.get("lease_expires_at"))
            if status == "dead_letter":
                dead_letter_jobs += 1
                continue
            if status == "leased" and lease_expires_at and lease_expires_at > now:
                leased_jobs += 1
                continue
            is_claimable_due = status == "pending" or (status == "leased" and (lease_expires_at is None or lease_expires_at <= now))
            if is_claimable_due and next_attempt_at <= now:
                due_jobs += 1
                age_seconds = max(0, int((now - next_attempt_at).total_seconds()))
                if oldest_due_age_seconds is None or age_seconds > oldest_due_age_seconds:
                    oldest_due_age_seconds = age_seconds
                continue
            if next_attempt_at <= now:
                stale_nonclaimable_jobs += 1

        slots = self.list_source_resolution_slots(active_only=True)
        cooldown_slots = 0
        max_cooldown_until: str | None = None
        for slot in slots:
            cooldown_until = _parse_iso_datetime(slot.get("cooldown_until"))
            if cooldown_until and cooldown_until > now:
                cooldown_slots += 1
                cooldown_iso = cooldown_until.isoformat()
                if max_cooldown_until is None or cooldown_iso > max_cooldown_until:
                    max_cooldown_until = cooldown_iso

        active_pending_sources = 0
        active_missing_peer_refs = 0
        active_channels = self.get_active_channels()
        peer_refs = {}
        try:
            res = self.client.table("telegram_channel_peer_refs") \
                .select("channel_id, session_slot") \
                .eq("session_slot", session_slot) \
                .execute()
            peer_refs = {
                str(item.get("channel_id") or ""): True
                for item in (res.data or [])
                if item.get("channel_id")
            }
        except Exception as e:
            self._warn_peer_refs_table_once(e)

        for channel in active_channels:
            resolution_status = str(channel.get("resolution_status") or "").strip().lower()
            if resolution_status == "pending":
                active_pending_sources += 1
            channel_id = str(channel.get("id") or "").strip()
            if resolution_status == "resolved" and channel_id and channel_id not in peer_refs:
                active_missing_peer_refs += 1

        return {
            "slot_key": session_slot,
            "due_jobs": due_jobs,
            "leased_jobs": leased_jobs,
            "dead_letter_jobs": dead_letter_jobs,
            "stale_nonclaimable_jobs": stale_nonclaimable_jobs,
            "cooldown_slots": cooldown_slots,
            "cooldown_until": max_cooldown_until,
            "oldest_due_age_seconds": oldest_due_age_seconds,
            "active_pending_sources": active_pending_sources,
            "active_missing_peer_refs": active_missing_peer_refs,
        }

    # ── Pipeline Stage Queues ────────────────────────────────────────────────

    @staticmethod
    def _pipeline_queue_table(queue_name: str) -> str:
        tables = {
            "ai_post": "ai_post_jobs",
            "ai_comment_group": "ai_comment_group_jobs",
            "neo4j_sync": "neo4j_sync_jobs",
        }
        key = str(queue_name or "").strip().lower()
        if key not in tables:
            raise ValueError(f"Unsupported pipeline queue: {queue_name}")
        return tables[key]

    @contextmanager
    def _pipeline_connection(self):
        database_url = str(getattr(config, "PIPELINE_DATABASE_URL", "") or "").strip()
        if not database_url:
            raise RuntimeError("PIPELINE_DATABASE_URL (or SUPABASE_DB_URL) is required for atomic pipeline queue helpers.")
        if psycopg is None or dict_row is None:
            raise RuntimeError("psycopg is required for atomic pipeline queue helpers.")
        conn = psycopg.connect(database_url, row_factory=dict_row)
        try:
            yield conn
        finally:
            conn.close()

    def _pipeline_queue_backoff_seconds(self, attempt_count: int) -> int:
        base = max(5, int(config.PIPELINE_QUEUE_BACKOFF_SECONDS))
        max_backoff = max(base, int(config.PIPELINE_QUEUE_BACKOFF_MAX_SECONDS))
        value = base * (2 ** max(0, int(attempt_count) - 1))
        return int(min(max_backoff, value))

    def enqueue_ai_post_jobs(self, *, limit: int | None = None) -> int:
        row_limit = max(1, int(limit or config.PIPELINE_QUEUE_REPAIR_BATCH_SIZE))
        try:
            with self._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.ai_post_jobs (
                      post_id,
                      status,
                      next_attempt_at,
                      created_at,
                      updated_at
                    )
                    SELECT
                      tp.id,
                      'pending',
                      timezone('utc', now()),
                      timezone('utc', now()),
                      timezone('utc', now())
                    FROM public.telegram_posts AS tp
                    WHERE tp.is_processed = FALSE
                      AND tp.text IS NOT NULL
                      AND NOT EXISTS (
                        SELECT 1
                        FROM public.ai_post_jobs AS job
                        WHERE job.post_id = tp.id
                      )
                    ORDER BY tp.posted_at ASC
                    LIMIT %s
                    ON CONFLICT (post_id) DO NOTHING
                    RETURNING id
                    """,
                    (row_limit,),
                )
                return len(cur.fetchall() or [])
        except Exception as e:
            self._warn_pipeline_queue_once("ai_post_jobs", e)
            return 0

    def enqueue_ai_comment_group_jobs(self, *, limit: int | None = None) -> int:
        row_limit = max(1, int(limit or config.PIPELINE_QUEUE_REPAIR_BATCH_SIZE))
        try:
            with self._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.ai_comment_group_jobs (
                      scope_key,
                      telegram_user_id,
                      channel_id,
                      post_id,
                      status,
                      next_attempt_at,
                      created_at,
                      updated_at
                    )
                    SELECT
                      grouped.scope_key,
                      grouped.telegram_user_id,
                      grouped.channel_id,
                      grouped.post_id,
                      'pending',
                      timezone('utc', now()),
                      timezone('utc', now()),
                      timezone('utc', now())
                    FROM (
                      SELECT
                        CONCAT(
                          COALESCE(tc.telegram_user_id::text, 'anonymous'),
                          ':',
                          COALESCE(tc.channel_id::text, 'unknown'),
                          ':',
                          COALESCE(tc.post_id::text, 'unknown')
                        ) AS scope_key,
                        tc.telegram_user_id,
                        tc.channel_id,
                        tc.post_id,
                        MIN(tc.posted_at) AS first_posted_at
                      FROM public.telegram_comments AS tc
                      WHERE tc.is_processed = FALSE
                        AND tc.text IS NOT NULL
                        AND tc.channel_id IS NOT NULL
                        AND tc.post_id IS NOT NULL
                      GROUP BY tc.telegram_user_id, tc.channel_id, tc.post_id
                    ) AS grouped
                    WHERE NOT EXISTS (
                      SELECT 1
                      FROM public.ai_comment_group_jobs AS job
                      WHERE job.scope_key = grouped.scope_key
                    )
                    ORDER BY grouped.first_posted_at ASC
                    LIMIT %s
                    ON CONFLICT (scope_key) DO NOTHING
                    RETURNING id
                    """,
                    (row_limit,),
                )
                return len(cur.fetchall() or [])
        except Exception as e:
            self._warn_pipeline_queue_once("ai_comment_group_jobs", e)
            return 0

    def enqueue_neo4j_sync_jobs(self, *, limit: int | None = None) -> int:
        row_limit = max(1, int(limit or config.PIPELINE_QUEUE_REPAIR_BATCH_SIZE))
        try:
            with self._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.neo4j_sync_jobs (
                      post_id,
                      status,
                      next_attempt_at,
                      created_at,
                      updated_at
                    )
                    SELECT
                      tp.id,
                      'pending',
                      timezone('utc', now()),
                      timezone('utc', now()),
                      timezone('utc', now())
                    FROM public.telegram_posts AS tp
                    WHERE tp.is_processed = TRUE
                      AND tp.neo4j_synced = FALSE
                      AND NOT EXISTS (
                        SELECT 1
                        FROM public.neo4j_sync_jobs AS job
                        WHERE job.post_id = tp.id
                      )
                    ORDER BY tp.posted_at ASC
                    LIMIT %s
                    ON CONFLICT (post_id) DO NOTHING
                    RETURNING id
                    """,
                    (row_limit,),
                )
                return len(cur.fetchall() or [])
        except Exception as e:
            self._warn_pipeline_queue_once("neo4j_sync_jobs", e)
            return 0

    def repair_pipeline_stage_queues(self, *, limit: int | None = None) -> dict[str, int]:
        row_limit = max(1, int(limit or config.PIPELINE_QUEUE_REPAIR_BATCH_SIZE))
        return {
            "ai_post_jobs_enqueued": self.enqueue_ai_post_jobs(limit=row_limit),
            "ai_comment_group_jobs_enqueued": self.enqueue_ai_comment_group_jobs(limit=row_limit),
            "neo4j_sync_jobs_enqueued": self.enqueue_neo4j_sync_jobs(limit=row_limit),
        }

    def _claim_pipeline_jobs(
        self,
        queue_name: str,
        *,
        worker_id: str,
        batch_size: int | None = None,
        lease_seconds: int | None = None,
        eligibility_join_sql: str = "",
        eligibility_where_sql: str = "",
    ) -> list[dict]:
        table_name = self._pipeline_queue_table(queue_name)
        requested_batch = max(1, int(batch_size or config.PIPELINE_QUEUE_CLAIM_BATCH_SIZE))
        ttl_seconds = max(30, int(lease_seconds or config.PIPELINE_QUEUE_LEASE_SECONDS))
        try:
            with self._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH candidates AS (
                      SELECT job.id
                      FROM public.{table_name} AS job
                      {eligibility_join_sql}
                      WHERE job.status IN ('pending', 'failed')
                        AND job.next_attempt_at <= timezone('utc', now())
                        {eligibility_where_sql}
                      ORDER BY job.next_attempt_at ASC, job.created_at ASC
                      LIMIT %s
                      FOR UPDATE SKIP LOCKED
                    )
                    UPDATE public.{table_name} AS job
                    SET status = 'leased',
                        lease_owner = %s,
                        lease_token = gen_random_uuid(),
                        lease_expires_at = timezone('utc', now()) + (%s * INTERVAL '1 second'),
                        updated_at = timezone('utc', now())
                    FROM candidates
                    WHERE job.id = candidates.id
                    RETURNING job.*
                    """,
                    (requested_batch, str(worker_id or "").strip(), ttl_seconds),
                )
                return list(cur.fetchall() or [])
        except Exception as e:
            self._warn_pipeline_queue_once(table_name, e)
            return []

    def _ack_pipeline_job(self, queue_name: str, *, job_id: str, lease_token: str) -> dict | None:
        table_name = self._pipeline_queue_table(queue_name)
        if not job_id or not lease_token:
            return None
        try:
            with self._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE public.{table_name}
                    SET status = 'done',
                        lease_owner = NULL,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        last_error = NULL,
                        updated_at = timezone('utc', now())
                    WHERE id = %s
                      AND lease_token = %s::uuid
                    RETURNING *
                    """,
                    (job_id, lease_token),
                )
                return cur.fetchone()
        except Exception as e:
            self._warn_pipeline_queue_once(table_name, e)
            return None

    def _nack_pipeline_job(
        self,
        queue_name: str,
        *,
        job_id: str,
        lease_token: str,
        error: str | Exception,
        max_attempts: int | None = None,
    ) -> dict | None:
        table_name = self._pipeline_queue_table(queue_name)
        if not job_id or not lease_token:
            return None
        try:
            with self._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, attempt_count
                    FROM public.{table_name}
                    WHERE id = %s
                      AND lease_token = %s::uuid
                    FOR UPDATE
                    """,
                    (job_id, lease_token),
                )
                current = cur.fetchone()
                if not current:
                    return None

                next_attempt = int(current.get("attempt_count") or 0) + 1
                max_allowed_attempts = max(1, int(max_attempts or config.PIPELINE_QUEUE_MAX_ATTEMPTS))
                is_dead_letter = next_attempt >= max_allowed_attempts
                now = datetime.now(timezone.utc)
                next_retry_at = now if is_dead_letter else now + timedelta(
                    seconds=self._pipeline_queue_backoff_seconds(next_attempt)
                )

                cur.execute(
                    f"""
                    UPDATE public.{table_name}
                    SET status = %s,
                        attempt_count = %s,
                        next_attempt_at = %s,
                        last_error = %s,
                        lease_owner = NULL,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        updated_at = %s
                    WHERE id = %s
                      AND lease_token = %s::uuid
                    RETURNING *
                    """,
                    (
                        "dead_lettered" if is_dead_letter else "failed",
                        next_attempt,
                        next_retry_at.isoformat(),
                        str(error or "")[:4000],
                        now.isoformat(),
                        job_id,
                        lease_token,
                    ),
                )
                return cur.fetchone()
        except Exception as e:
            self._warn_pipeline_queue_once(table_name, e)
            return None

    def _reclaim_expired_pipeline_jobs(self, queue_name: str, *, worker_id: str | None = None) -> int:
        del worker_id
        table_name = self._pipeline_queue_table(queue_name)
        try:
            with self._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE public.{table_name}
                    SET status = 'pending',
                        lease_owner = NULL,
                        lease_token = NULL,
                        lease_expires_at = NULL,
                        updated_at = timezone('utc', now())
                    WHERE status = 'leased'
                      AND lease_expires_at IS NOT NULL
                      AND lease_expires_at <= timezone('utc', now())
                    RETURNING id
                    """
                )
                return len(cur.fetchall() or [])
        except Exception as e:
            self._warn_pipeline_queue_once(table_name, e)
            return 0

    def claim_ai_post_jobs(
        self,
        *,
        worker_id: str,
        batch_size: int | None = None,
        lease_seconds: int | None = None,
    ) -> list[dict]:
        return self._claim_pipeline_jobs(
            "ai_post",
            worker_id=worker_id,
            batch_size=batch_size,
            lease_seconds=lease_seconds,
        )

    def claim_ai_comment_group_jobs(
        self,
        *,
        worker_id: str,
        batch_size: int | None = None,
        lease_seconds: int | None = None,
    ) -> list[dict]:
        return self._claim_pipeline_jobs(
            "ai_comment_group",
            worker_id=worker_id,
            batch_size=batch_size,
            lease_seconds=lease_seconds,
        )

    def claim_neo4j_sync_jobs(
        self,
        *,
        worker_id: str,
        batch_size: int | None = None,
        lease_seconds: int | None = None,
    ) -> list[dict]:
        return self._claim_pipeline_jobs(
            "neo4j_sync",
            worker_id=worker_id,
            batch_size=batch_size,
            lease_seconds=lease_seconds,
            eligibility_join_sql="""
                      JOIN public.telegram_posts AS post
                        ON post.id = job.post_id
                    """,
            eligibility_where_sql="""
                        AND post.is_processed = TRUE
                        AND post.neo4j_synced = FALSE
                    """,
        )

    def ack_ai_post_job(self, job_id: str, lease_token: str) -> dict | None:
        return self._ack_pipeline_job("ai_post", job_id=job_id, lease_token=lease_token)

    def ack_ai_comment_group_job(self, job_id: str, lease_token: str) -> dict | None:
        return self._ack_pipeline_job("ai_comment_group", job_id=job_id, lease_token=lease_token)

    def ack_neo4j_sync_job(self, job_id: str, lease_token: str) -> dict | None:
        return self._ack_pipeline_job("neo4j_sync", job_id=job_id, lease_token=lease_token)

    def nack_ai_post_job(
        self,
        job_id: str,
        lease_token: str,
        error: str | Exception,
        *,
        max_attempts: int | None = None,
    ) -> dict | None:
        return self._nack_pipeline_job(
            "ai_post",
            job_id=job_id,
            lease_token=lease_token,
            error=error,
            max_attempts=max_attempts,
        )

    def nack_ai_comment_group_job(
        self,
        job_id: str,
        lease_token: str,
        error: str | Exception,
        *,
        max_attempts: int | None = None,
    ) -> dict | None:
        return self._nack_pipeline_job(
            "ai_comment_group",
            job_id=job_id,
            lease_token=lease_token,
            error=error,
            max_attempts=max_attempts,
        )

    def nack_neo4j_sync_job(
        self,
        job_id: str,
        lease_token: str,
        error: str | Exception,
        *,
        max_attempts: int | None = None,
    ) -> dict | None:
        return self._nack_pipeline_job(
            "neo4j_sync",
            job_id=job_id,
            lease_token=lease_token,
            error=error,
            max_attempts=max_attempts,
        )

    def reclaim_expired_ai_post_jobs(self, worker_id: str | None = None) -> int:
        return self._reclaim_expired_pipeline_jobs("ai_post", worker_id=worker_id)

    def reclaim_expired_ai_comment_group_jobs(self, worker_id: str | None = None) -> int:
        return self._reclaim_expired_pipeline_jobs("ai_comment_group", worker_id=worker_id)

    def reclaim_expired_neo4j_sync_jobs(self, worker_id: str | None = None) -> int:
        return self._reclaim_expired_pipeline_jobs("neo4j_sync", worker_id=worker_id)

    def get_runtime_json(self, path: str, default: dict | None = None) -> dict:
        """Load a JSON object from runtime-config storage bucket."""
        fallback = default if isinstance(default, dict) else {}
        key = str(path or "").strip()
        if not key:
            return dict(fallback)

        result = self.read_runtime_json(key)
        if result["status"] != "ok":
            return dict(fallback)
        payload = result.get("payload")
        return dict(payload) if isinstance(payload, dict) else dict(fallback)

    def save_runtime_json(self, path: str, payload: dict) -> bool:
        """Persist a JSON object to runtime-config storage bucket."""
        key = str(path or "").strip()
        if not key:
            return False

        data = payload if isinstance(payload, dict) else {}
        try:
            self._ensure_runtime_bucket()
            bucket = self.client.storage.from_(self._runtime_bucket_name)
            body = json.dumps(data, ensure_ascii=True).encode("utf-8")
            file_options = {"content-type": "application/json"}
            try:
                bucket.upload(key, body, file_options)
            except Exception as exc:
                if "duplicate" not in str(exc).lower():
                    raise
                # Overwrite existing runtime JSON by replacing the object.
                bucket.remove([key])
                bucket.upload(key, body, file_options)
            verify = self.read_runtime_json(key)
            if verify["status"] != "ok":
                logger.error(
                    "Runtime JSON write succeeded but readback failed | path={} status={} error={}",
                    key,
                    verify["status"],
                    verify.get("error") or "",
                )
                return False
            stored = verify.get("payload")
            if not isinstance(stored, dict):
                logger.error(
                    "Runtime JSON write read back non-object payload | path={} status={}",
                    key,
                    verify["status"],
                )
                return False
            if stored != data:
                logger.error("Runtime JSON write read back different payload | path={}", key)
                return False
            return True
        except Exception as exc:
            logger.error("Runtime JSON write failed | path={} error={}", key, exc)
            return False

    def read_runtime_json(self, path: str) -> dict:
        """Load runtime-config JSON with status metadata for callers that need diagnostics."""
        key = str(path or "").strip()
        if not key:
            return {"status": "invalid_path", "payload": {}, "error": "Runtime JSON path is empty"}

        try:
            self._ensure_runtime_bucket()
            raw = self.client.storage.from_(self._runtime_bucket_name).download(key)
        except Exception as exc:
            status = self._classify_runtime_read_error(exc)
            if status == "missing":
                logger.info("Runtime JSON missing | path={}", key)
            else:
                logger.warning("Runtime JSON unreadable | path={} error={}", key, exc)
            return {"status": status, "payload": {}, "error": str(exc)}

        if not raw:
            logger.warning("Runtime JSON unreadable | path={} error=empty response body", key)
            return {"status": "unreadable", "payload": {}, "error": "Empty response body"}

        try:
            parsed = json.loads(raw.decode("utf-8"))
        except UnicodeDecodeError as exc:
            logger.warning("Runtime JSON unreadable | path={} error={}", key, exc)
            return {"status": "unreadable", "payload": {}, "error": str(exc)}
        except json.JSONDecodeError as exc:
            logger.warning("Runtime JSON invalid JSON | path={} error={}", key, exc)
            return {"status": "invalid_json", "payload": {}, "error": str(exc)}

        if not isinstance(parsed, dict):
            logger.warning("Runtime JSON invalid JSON | path={} error=root JSON value must be an object", key)
            return {"status": "invalid_json", "payload": {}, "error": "Root JSON value must be an object"}

        if self._should_prefer_signed_read(key):
            signed = self._read_runtime_json_via_signed_url(key)
            if signed["status"] == "ok":
                return signed

        return {"status": "ok", "payload": parsed, "error": ""}

    @staticmethod
    def _classify_runtime_read_error(error: Exception) -> str:
        status_code = getattr(error, "status_code", None)
        if status_code == 404:
            return "missing"

        low = str(error).lower()
        if any(marker in low for marker in ("not found", "404", "no such", "does not exist", "missing")):
            return "missing"
        return "unreadable"

    @staticmethod
    def _should_prefer_signed_read(path: str) -> bool:
        name = str(path or "").rsplit("/", 1)[-1]
        return bool(name.endswith(".json") and not _looks_like_versioned_runtime_json_name(name))

    def _read_runtime_json_via_signed_url(self, path: str) -> dict:
        key = str(path or "").strip()
        if not key or certifi is None:
            return {"status": "unavailable", "payload": {}, "error": "Signed URL reader unavailable"}

        try:
            signed = self.client.storage.from_(self._runtime_bucket_name).create_signed_url(key, 60)
            signed_url = signed.get("signedURL") if isinstance(signed, dict) else None
            if not signed_url:
                return {"status": "unavailable", "payload": {}, "error": "Signed URL missing from response"}

            context = ssl.create_default_context(cafile=certifi.where())
            with urlopen(signed_url, timeout=10, context=context) as response:
                raw = response.read()
        except Exception as exc:
            logger.warning("Runtime JSON signed read failed | path={} error={}", key, exc)
            return {"status": "unreadable", "payload": {}, "error": str(exc)}

        if not raw:
            return {"status": "unreadable", "payload": {}, "error": "Empty response body"}

        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            return {"status": "invalid_json", "payload": {}, "error": str(exc)}

        if not isinstance(parsed, dict):
            return {"status": "invalid_json", "payload": {}, "error": "Root JSON value must be an object"}

        return {"status": "ok", "payload": parsed, "error": ""}
    def list_runtime_files(self, folder: str) -> list[dict]:
        """List files under a runtime-config folder path."""
        path = str(folder or "").strip().strip("/")
        if not path:
            return []
        try:
            self._ensure_runtime_bucket()
            rows = self.client.storage.from_(self._runtime_bucket_name).list(path)
            return rows if isinstance(rows, list) else []
        except Exception:
            return []

    def delete_runtime_files(self, paths: list[str]) -> int:
        """Delete runtime-config files; returns number of requested paths."""
        keys = [str(p or "").strip() for p in (paths or []) if str(p or "").strip()]
        if not keys:
            return 0
        try:
            self._ensure_runtime_bucket()
            self.client.storage.from_(self._runtime_bucket_name).remove(keys)
            return len(keys)
        except Exception:
            return 0

    # ── Channels ─────────────────────────────────────────────────────────────

    def get_active_channels(self) -> list[dict]:
        """Return all channels with is_active=TRUE."""
        res = self.client.table("telegram_channels") \
            .select("*") \
            .eq("is_active", True) \
            .execute()
        return res.data or []

    def list_channels(self) -> list[dict]:
        """Return all channels ordered by newest first."""
        res = self.client.table("telegram_channels") \
            .select("*") \
            .order("created_at", desc=True) \
            .execute()
        return res.data or []

    def get_channel_by_id(self, channel_uuid: str) -> dict | None:
        """Return a single channel by UUID."""
        res = self.client.table("telegram_channels") \
            .select("*") \
            .eq("id", channel_uuid) \
            .limit(1) \
            .execute()
        return res.data[0] if res.data else None

    def get_channel_by_username(self, channel_username: str) -> dict | None:
        """Case-insensitive lookup by exact username string."""
        normalized = (channel_username or "").strip().lower()
        if not normalized:
            return None
        res = self.client.table("telegram_channels") \
            .select("*") \
            .ilike("channel_username", normalized) \
            .limit(1) \
            .execute()
        return res.data[0] if res.data else None

    def get_channel_by_handle(self, handle: str) -> dict | None:
        """
        Lookup by normalized handle (without @), matching rows with or without @ prefix.
        """
        normalized = (handle or "").strip().lower().lstrip("@")
        if not normalized:
            return None

        # Keep this robust against mixed historical data where usernames were
        # stored both with and without leading '@'.
        rows = self.list_channels()
        for row in rows:
            value = (row.get("channel_username") or "").strip().lower().lstrip("@")
            if value == normalized:
                return row
        return None

    def create_channel(self, payload: dict) -> dict:
        """Create a new channel source and return it."""
        res = self.client.table("telegram_channels") \
            .insert(payload) \
            .execute()
        if not res.data:
            raise RuntimeError("Failed to create telegram channel")
        return res.data[0]

    def update_channel(self, channel_uuid: str, payload: dict) -> dict | None:
        """Update channel source fields and return updated row."""
        if not payload:
            return self.get_channel_by_id(channel_uuid)
        res = self.client.table("telegram_channels") \
            .update(payload) \
            .eq("id", channel_uuid) \
            .execute()
        if res.data:
            return res.data[0]
        return self.get_channel_by_id(channel_uuid)

    def update_channel_metadata(self, channel_uuid: str, metadata: dict):
        """Update channel title, telegram_channel_id, member_count etc."""
        # Filter out None values so we don't overwrite useful data with NULLs
        payload = {k: v for k, v in metadata.items() if v is not None}
        if payload:
            self.client.table("telegram_channels") \
                .update(payload) \
                .eq("id", channel_uuid) \
                .execute()

    def update_channel_last_scraped(self, channel_uuid: str):
        """Set last_scraped_at to now."""
        self.client.table("telegram_channels") \
            .update({"last_scraped_at": datetime.now(timezone.utc).isoformat()}) \
            .eq("id", channel_uuid) \
            .execute()

    # ── Posts ─────────────────────────────────────────────────────────────────

    def upsert_posts(self, posts: list[dict]):
        """
        Insert or update posts. 
        UNIQUE constraint on (channel_id, telegram_message_id) prevents duplicates.
        """
        if not posts:
            return
        self.client.table("telegram_posts") \
            .upsert(posts, on_conflict="channel_id,telegram_message_id") \
            .execute()
        logger.debug(f"Upserted {len(posts)} posts")

    @staticmethod
    def _comment_failure_scope_key(comment: dict) -> str:
        uid = comment.get("telegram_user_id")
        uid_text = str(uid if uid is not None else "anonymous")
        channel_id = str(comment.get("channel_id") or "unknown")
        post_id = str(comment.get("post_id") or "unknown")
        return f"{uid_text}:{channel_id}:{post_id}"

    def _filter_out_blocked_rows(
        self,
        rows: list[dict],
        *,
        scope_type: str,
        scope_key_builder,
        limit: int,
    ) -> list[dict]:
        """Return up to `limit` rows whose retry/dead-letter scopes are not blocked."""
        if not rows or limit <= 0:
            return []

        row_keys: list[str] = []
        keys: list[str] = []
        for row in rows:
            key = str(scope_key_builder(row) or "").strip()
            row_keys.append(key)
            if key:
                keys.append(key)

        blocked = self.get_blocked_scopes(scope_type, keys) if keys else set()
        selected: list[dict] = []
        for row, key in zip(rows, row_keys):
            if key and key in blocked:
                continue
            selected.append(row)
            if len(selected) >= limit:
                break
        return selected

    def _paged_unprocessed_rows(
        self,
        *,
        table_name: str,
        select_columns: str,
        limit: int,
        scope_type: str,
        scope_key_builder,
    ) -> list[dict]:
        """Read unprocessed rows in pages so blocked rows do not starve runnable work."""
        target = max(0, int(limit))
        if target <= 0:
            return []

        page_size = max(target, min(200, target * 4))
        start = 0
        selected: list[dict] = []

        while len(selected) < target:
            query = self.client.table(table_name) \
                .select(select_columns) \
                .eq("is_processed", False) \
                .not_.is_("text", "null") \
                .order("posted_at", desc=False) \
                .range(start, start + page_size - 1)
            res = query.execute()
            rows = res.data or []
            if not rows:
                break

            remaining = target - len(selected)
            selected.extend(
                self._filter_out_blocked_rows(
                    rows,
                    scope_type=scope_type,
                    scope_key_builder=scope_key_builder,
                    limit=remaining,
                )
            )

            if len(rows) < page_size:
                break
            start += len(rows)

        return selected[:target]

    def get_unprocessed_posts(self, limit: int = 100) -> list[dict]:
        """Fetch oldest unprocessed posts that are currently runnable by the AI stage."""
        return self._paged_unprocessed_rows(
            table_name="telegram_posts",
            select_columns=(
                "id, channel_id, telegram_message_id, text, posted_at, "
                "entry_kind, thread_message_count, thread_participant_count, last_activity_at"
            ),
            limit=limit,
            scope_type="post",
            scope_key_builder=lambda row: row.get("id"),
        )

    def get_posts_with_comments_pending(self, limit: int = 50) -> list[dict]:
        """Fetch posts that have comments but haven't had comments scraped yet."""
        query = self.client.table("telegram_posts") \
            .select("id, channel_id, telegram_message_id") \
            .eq("has_comments", True) \
            .is_("comments_scraped_at", "null") \
            .neq("entry_kind", "thread_anchor")
        res = query.limit(limit).execute()
        return res.data or []

    def get_posts_with_comments_pending_for_channel(self, channel_uuid: str, limit: int = 50) -> list[dict]:
        """Fetch pending comment-scrape posts for a specific channel."""
        query = self.client.table("telegram_posts") \
            .select("id, channel_id, telegram_message_id") \
            .eq("channel_id", channel_uuid) \
            .eq("has_comments", True) \
            .is_("comments_scraped_at", "null") \
            .neq("entry_kind", "thread_anchor")
        res = query.limit(limit).execute()
        return res.data or []

    def mark_post_processed(self, post_uuid: str):
        self.client.table("telegram_posts") \
            .update({"is_processed": True}) \
            .eq("id", post_uuid) \
            .execute()

    def mark_post_comments_scraped(self, post_uuid: str, comment_count: int):
        self.client.table("telegram_posts") \
            .update({
                "comment_count":        comment_count,
                "comments_scraped_at":  datetime.now(timezone.utc).isoformat(),
            }) \
            .eq("id", post_uuid) \
            .execute()

    # ── Comments ─────────────────────────────────────────────────────────────

    def upsert_comments(self, comments: list[dict]):
        """Insert or update comments. Dedup on (post_id, telegram_message_id)."""
        if not comments:
            return
        self.client.table("telegram_comments") \
            .upsert(comments, on_conflict="post_id,telegram_message_id") \
            .execute()
        logger.debug(f"Upserted {len(comments)} comments")

    def get_unprocessed_comments(self, limit: int = 200) -> list[dict]:
        """Fetch oldest unprocessed comment groups that are currently runnable by the AI stage."""
        return self._paged_unprocessed_rows(
            table_name="telegram_comments",
            select_columns="id, post_id, channel_id, telegram_user_id, text, posted_at",
            limit=limit,
            scope_type="comment_group",
            scope_key_builder=self._comment_failure_scope_key,
        )

    def mark_comment_processed(self, comment_uuid: str):
        self.client.table("telegram_comments") \
            .update({"is_processed": True}) \
            .eq("id", comment_uuid) \
            .execute()

    # ── Users ─────────────────────────────────────────────────────────────────

    def upsert_user(self, user: dict) -> str | None:
        """
        Insert or update a user by telegram_user_id.
        Returns the internal UUID of the user.
        """
        if not user.get("telegram_user_id"):
            return None

        payload = {k: v for k, v in user.items() if v is not None}
        payload["last_seen_at"] = datetime.now(timezone.utc).isoformat()

        res = self.client.table("telegram_users") \
            .upsert(payload, on_conflict="telegram_user_id") \
            .execute()

        if res.data:
            return res.data[0]["id"]
        return None

    def get_user_by_telegram_id(self, telegram_user_id: int) -> dict | None:
        res = self.client.table("telegram_users") \
            .select("*") \
            .eq("telegram_user_id", telegram_user_id) \
            .limit(1) \
            .execute()
        return res.data[0] if res.data else None

    # ── AI Analysis ─────────────────────────────────────────────────────────

    @staticmethod
    def _analysis_message_key(item: object) -> str | None:
        if not isinstance(item, dict):
            return None
        comment_id = str(item.get("comment_id") or "").strip()
        message_ref = str(item.get("message_ref") or "").strip()
        key = comment_id or message_ref
        return key or None

    @staticmethod
    def _merge_analysis_items(existing_items: object, incoming_items: object) -> list[dict]:
        merged: list[dict] = []
        seen: set[str] = set()

        for source in (incoming_items, existing_items):
            if not isinstance(source, list):
                continue
            for item in source:
                if not isinstance(item, dict):
                    continue
                key = SupabaseWriter._analysis_message_key(item)
                if not key:
                    continue
                if key in seen:
                    continue
                seen.add(key)
                merged.append(dict(item))
        return merged

    @staticmethod
    def _merge_topic_items(existing_topics: object, incoming_topics: object) -> list[dict]:
        merged: list[dict] = []
        seen: set[str] = set()

        for source in (incoming_topics, existing_topics):
            if not isinstance(source, list):
                continue
            for item in source:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "").strip()
                if not name or name in seen:
                    continue
                seen.add(name)
                merged.append(dict(item))
        return merged

    def _merge_batch_raw_llm_response(self, existing_raw: object, incoming_raw: object) -> dict:
        existing = existing_raw if isinstance(existing_raw, dict) else {}
        incoming = incoming_raw if isinstance(incoming_raw, dict) else {}
        if not existing:
            return dict(incoming)
        if not incoming:
            return dict(existing)

        merged = dict(existing)
        merged.update(incoming)

        message_topics = self._merge_analysis_items(existing.get("message_topics"), incoming.get("message_topics"))
        if message_topics:
            merged["message_topics"] = message_topics

        message_sentiments = self._merge_analysis_items(existing.get("message_sentiments"), incoming.get("message_sentiments"))
        if message_sentiments:
            merged["message_sentiments"] = message_sentiments

        merged_topics = self._merge_topic_items(existing.get("topics"), incoming.get("topics"))
        if merged_topics:
            merged["topics"] = merged_topics

        return merged

    def save_analysis(self, analysis: dict) -> dict | None:
        """
        Save AI analysis result.

        Contract hardening:
        - Post-level analyses are idempotent by logical key
          (`content_type='post'` + `content_id`), so repeated processing
          updates the latest row instead of creating duplicates.
        - Comment user-per-post analyses are idempotent by logical key
          (`content_type='batch'` + `content_id` + `channel_id` + `telegram_user_id`)
          when `content_id` is present.
        - Other analysis types keep insert behavior for backward compatibility.
        """
        payload = dict(analysis or {})

        content_type = str(payload.get("content_type") or "")
        content_id = payload.get("content_id")
        channel_id = payload.get("channel_id")
        telegram_user_id = payload.get("telegram_user_id")

        idempotency_query = None
        idempotency_label = None

        if content_type == "post" and content_id:
            idempotency_query = self.client.table("ai_analysis") \
                .select("id, created_at, raw_llm_response") \
                .eq("content_type", "post") \
                .eq("content_id", content_id) \
                .order("created_at", desc=True) \
                .limit(1)
            idempotency_label = f"post content_id={content_id}"
        elif (
            content_type == "batch"
            and content_id
            and channel_id
            and telegram_user_id is not None
        ):
            idempotency_query = self.client.table("ai_analysis") \
                .select("id, created_at, raw_llm_response") \
                .eq("content_type", "batch") \
                .eq("content_id", content_id) \
                .eq("channel_id", channel_id) \
                .eq("telegram_user_id", telegram_user_id) \
                .order("created_at", desc=True) \
                .limit(1)
            idempotency_label = (
                f"batch(content-scoped) content_id={content_id} channel_id={channel_id} "
                f"telegram_user_id={telegram_user_id}"
            )

        if idempotency_query is not None:
            try:
                existing_res = idempotency_query.execute()
                existing = (existing_res.data or [None])[0]

                if existing and existing.get("id"):
                    update_payload = {
                        k: v
                        for k, v in payload.items()
                        if k not in {"id", "created_at"}
                    }
                    if content_type == "batch":
                        current_raw = existing.get("raw_llm_response")
                        next_raw = update_payload.get("raw_llm_response")
                        if isinstance(current_raw, dict) or isinstance(next_raw, dict):
                            update_payload["raw_llm_response"] = self._merge_batch_raw_llm_response(current_raw, next_raw)
                    # Re-analysis must be re-synced to graph.
                    update_payload["neo4j_synced"] = False

                    updated_res = self.client.table("ai_analysis") \
                        .update(update_payload) \
                        .eq("id", existing["id"]) \
                        .execute()
                    self._register_topic_proposals_from_analysis(
                        payload,
                        analysis_id=str(existing.get("id") or "") or None,
                        channel_id=str(channel_id) if channel_id else None,
                        content_type=content_type,
                        content_id=str(content_id) if content_id else None,
                        telegram_user_id=int(telegram_user_id) if telegram_user_id is not None else None,
                    )
                    if updated_res.data:
                        return updated_res.data[0]
                    return {"id": existing["id"], **update_payload}
            except Exception as e:
                logger.warning(
                    f"Analysis idempotent update path failed for {idempotency_label}: {e}. Falling back to insert."
                )

        inserted_res = self.client.table("ai_analysis") \
            .insert(payload) \
            .execute()
        inserted = inserted_res.data[0] if inserted_res.data else None
        self._register_topic_proposals_from_analysis(
            payload,
            analysis_id=str(inserted.get("id") or "") if isinstance(inserted, dict) else None,
            channel_id=str(channel_id) if channel_id else None,
            content_type=content_type,
            content_id=str(content_id) if content_id else None,
            telegram_user_id=int(telegram_user_id) if telegram_user_id is not None else None,
        )
        return inserted

    def get_unsynced_analysis(self, limit: int = 100) -> list[dict]:
        """Fetch AI analysis not yet pushed to Neo4j."""
        res = self.client.table("ai_analysis") \
            .select("*") \
            .eq("neo4j_synced", False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def _failure_backoff_seconds(self, attempt_count: int) -> int:
        base = max(5, int(config.AI_FAILURE_BACKOFF_SECONDS))
        max_backoff = max(base, int(config.AI_FAILURE_BACKOFF_MAX_SECONDS))
        value = base * (2 ** max(0, int(attempt_count) - 1))
        return int(min(max_backoff, value))

    def get_blocked_scopes(self, scope_type: str, scope_keys: list[str]) -> set[str]:
        """Return scope keys blocked by retry delay or dead-letter state."""
        keys = [str(key).strip() for key in (scope_keys or []) if str(key).strip()]
        if not keys:
            return set()

        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            res = self.client.table("ai_processing_failures") \
                .select("scope_key, is_dead_letter, next_retry_at") \
                .eq("scope_type", scope_type) \
                .in_("scope_key", keys) \
                .execute()

            blocked: set[str] = set()
            for row in (res.data or []):
                key = str(row.get("scope_key") or "").strip()
                if not key:
                    continue
                if bool(row.get("is_dead_letter", False)):
                    blocked.add(key)
                    continue
                next_retry_at = row.get("next_retry_at")
                if next_retry_at and str(next_retry_at) > now_iso:
                    blocked.add(key)
            return blocked
        except Exception as e:
            self._warn_failure_table_once(e)
            return set()

    def record_processing_failure(
        self,
        *,
        scope_type: str,
        scope_key: str,
        channel_id: str | None,
        post_id: str | None,
        telegram_user_id: int | None,
        error: str,
    ) -> dict:
        """Increment failure tracking with backoff and dead-letter thresholds."""
        scope_key_norm = str(scope_key or "").strip()
        if not scope_key_norm:
            return {"attempt_count": 0, "is_dead_letter": False}

        try:
            now = datetime.now(timezone.utc)
            existing_res = self.client.table("ai_processing_failures") \
                .select("id, attempt_count, first_failed_at") \
                .eq("scope_type", scope_type) \
                .eq("scope_key", scope_key_norm) \
                .limit(1) \
                .execute()
            existing = (existing_res.data or [None])[0]

            attempt_count = int(existing.get("attempt_count") or 0) + 1 if existing else 1
            dead_letter = attempt_count >= max(1, int(config.AI_FAILURE_MAX_RETRIES))
            next_retry_at = now + timedelta(seconds=self._failure_backoff_seconds(attempt_count))

            payload = {
                "scope_type": scope_type,
                "scope_key": scope_key_norm,
                "channel_id": channel_id,
                "post_id": post_id,
                "telegram_user_id": telegram_user_id,
                "attempt_count": attempt_count,
                "last_error": (error or "")[:1800],
                "last_failed_at": now.isoformat(),
                "next_retry_at": next_retry_at.isoformat(),
                "is_dead_letter": dead_letter,
                "resolved_at": None,
                "updated_at": now.isoformat(),
            }

            if existing and existing.get("first_failed_at"):
                payload["first_failed_at"] = existing.get("first_failed_at")
            else:
                payload["first_failed_at"] = now.isoformat()

            if existing and existing.get("id"):
                self.client.table("ai_processing_failures") \
                    .update(payload) \
                    .eq("id", existing["id"]) \
                    .execute()
            else:
                self.client.table("ai_processing_failures") \
                    .insert(payload) \
                    .execute()

            return {
                "attempt_count": attempt_count,
                "is_dead_letter": dead_letter,
                "next_retry_at": next_retry_at.isoformat(),
            }
        except Exception as e:
            self._warn_failure_table_once(e)
            return {"attempt_count": 0, "is_dead_letter": False}

    def clear_processing_failure(self, scope_type: str, scope_key: str) -> None:
        """Clear failure tracking on successful processing."""
        key = str(scope_key or "").strip()
        if not key:
            return
        try:
            self.client.table("ai_processing_failures") \
                .delete() \
                .eq("scope_type", scope_type) \
                .eq("scope_key", key) \
                .execute()
        except Exception as e:
            self._warn_failure_table_once(e)

    def list_processing_failures(
        self,
        *,
        dead_letter_only: bool = True,
        scope_type: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """List tracked processing failures for operator triage."""
        try:
            query = self.client.table("ai_processing_failures") \
                .select("*") \
                .order("last_failed_at", desc=True) \
                .limit(max(1, min(int(limit), 500)))

            if dead_letter_only:
                query = query.eq("is_dead_letter", True)
            if scope_type:
                query = query.eq("scope_type", scope_type)

            res = query.execute()
            return res.data or []
        except Exception as e:
            self._warn_failure_table_once(e)
            return []

    def retry_processing_failures(self, *, scope_type: str, scope_keys: list[str]) -> int:
        """Unlock selected failures for immediate retry."""
        keys = [str(key).strip() for key in (scope_keys or []) if str(key).strip()]
        if not keys:
            return 0

        try:
            existing_res = self.client.table("ai_processing_failures") \
                .select("scope_key") \
                .eq("scope_type", scope_type) \
                .in_("scope_key", keys) \
                .execute()
            existing_keys = {
                str(row.get("scope_key") or "").strip()
                for row in (existing_res.data or [])
                if row.get("scope_key")
            }
            if not existing_keys:
                return 0

            now_iso = datetime.now(timezone.utc).isoformat()
            self.client.table("ai_processing_failures") \
                .update({
                    "attempt_count": 0,
                    "is_dead_letter": False,
                    "next_retry_at": now_iso,
                    "updated_at": now_iso,
                }) \
                .eq("scope_type", scope_type) \
                .in_("scope_key", list(existing_keys)) \
                .execute()
            return len(existing_keys)
        except Exception as e:
            self._warn_failure_table_once(e)
            return 0

    def get_processing_failure_counts(self) -> dict:
        """Return dead-letter and retry-blocked failure scope counts."""
        try:
            dead = self.client.table("ai_processing_failures") \
                .select("id") \
                .eq("is_dead_letter", True) \
                .execute()

            now_iso = datetime.now(timezone.utc).isoformat()
            blocked = self.client.table("ai_processing_failures") \
                .select("id") \
                .eq("is_dead_letter", False) \
                .gt("next_retry_at", now_iso) \
                .execute()

            return {
                "dead_letter_scopes": len(dead.data or []),
                "retry_blocked_scopes": len(blocked.data or []),
            }
        except Exception as e:
            self._warn_failure_table_once(e)
            return {
                "dead_letter_scopes": 0,
                "retry_blocked_scopes": 0,
            }

    def refresh_runtime_topic_aliases(self) -> int:
        """Load operator-approved topic promotions into runtime normalizer aliases."""
        try:
            res = self.client.table("topic_taxonomy_promotions") \
                .select("alias_name, canonical_topic") \
                .eq("is_active", True) \
                .execute()

            alias_map: dict[str, str] = {}
            for row in (res.data or []):
                alias = str(row.get("alias_name") or "").strip()
                canonical = str(row.get("canonical_topic") or "").strip()
                if alias and canonical:
                    alias_map[alias.lower()] = canonical
            set_runtime_topic_aliases(alias_map)
            return len(alias_map)
        except Exception as e:
            set_runtime_topic_aliases({})
            self._warn_topic_promotion_table_once(e)
            return 0

    def list_topic_promotions(self, *, limit: int = 200, active_only: bool = True) -> list[dict]:
        """List operator-approved topic promotions/aliases."""
        try:
            query = self.client.table("topic_taxonomy_promotions") \
                .select("*") \
                .order("updated_at", desc=True) \
                .limit(max(1, min(int(limit), 500)))

            if active_only:
                query = query.eq("is_active", True)

            res = query.execute()
            return res.data or []
        except Exception as e:
            self._warn_topic_promotion_table_once(e)
            return []

    def _extract_proposed_topics(self, analysis_payload: dict) -> list[dict]:
        raw = analysis_payload.get("raw_llm_response") if isinstance(analysis_payload, dict) else None
        if not isinstance(raw, dict):
            return []

        topics = raw.get("topics")
        if not isinstance(topics, list):
            return []

        proposals: list[dict] = []
        seen: set[str] = set()
        for item in topics:
            if not isinstance(item, dict):
                continue
            if not bool(item.get("proposed", False)):
                continue

            topic_name = str(item.get("proposed_topic") or item.get("name") or "").strip()
            if not topic_name:
                continue
            key = topic_name.lower()
            if key in seen:
                continue
            seen.add(key)

            proposals.append(
                {
                    "topic_name": topic_name,
                    "closest_category": str(item.get("closest_category") or "General").strip() or "General",
                    "domain": str(item.get("domain") or "General").strip() or "General",
                    "evidence": str(item.get("evidence") or "").strip()[:1000] or None,
                }
            )
        return proposals

    def _json_text_list(self, value) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            try:
                decoded = json.loads(text)
                if isinstance(decoded, list):
                    return [str(item).strip() for item in decoded if str(item).strip()]
            except Exception:
                return []
        return []

    def _proposal_scope_key(
        self,
        *,
        analysis_id: str | None,
        channel_id: str | None,
        content_type: str | None,
        content_id: str | None,
        telegram_user_id: int | None,
    ) -> str:
        parts = [
            str(channel_id or "").strip() or "none",
            str(content_type or "").strip() or "unknown",
            str(content_id or "").strip() or "none",
            str(telegram_user_id) if telegram_user_id is not None else "none",
        ]
        scope_key = "|".join(parts)
        if scope_key == "none|unknown|none|none":
            return str(analysis_id or "").strip() or "unknown"
        return scope_key

    def _topic_visibility_state(
        self,
        *,
        distinct_content_count: int,
        distinct_user_count: int,
        distinct_channel_count: int,
    ) -> tuple[bool, str]:
        eligible = (
            distinct_content_count >= 3
            and distinct_user_count >= 3
            and distinct_channel_count >= 2
        )
        return eligible, ("emerging_visible" if eligible else "candidate")

    def _register_topic_proposals_from_analysis(
        self,
        analysis_payload: dict,
        *,
        analysis_id: str | None,
        channel_id: str | None,
        content_type: str | None,
        content_id: str | None,
        telegram_user_id: int | None,
    ) -> None:
        proposals = self._extract_proposed_topics(analysis_payload)
        if not proposals:
            return

        now_iso = datetime.now(timezone.utc).isoformat()
        scope_key = self._proposal_scope_key(
            analysis_id=analysis_id,
            channel_id=channel_id,
            content_type=content_type,
            content_id=content_id,
            telegram_user_id=telegram_user_id,
        )
        channel_marker = str(channel_id or "").strip()
        user_marker = str(telegram_user_id) if telegram_user_id is not None else ""
        content_marker = f"{str(content_type or 'unknown').strip() or 'unknown'}:{str(content_id or '').strip() or 'none'}"

        for proposal in proposals:
            topic_name = proposal["topic_name"]
            try:
                existing_res = self.client.table("topic_review_queue") \
                    .select("*") \
                    .eq("topic_name", topic_name) \
                    .limit(1) \
                    .execute()
                existing = (existing_res.data or [None])[0]

                existing_scope_keys = self._json_text_list((existing or {}).get("seen_scope_keys"))
                existing_content_keys = self._json_text_list((existing or {}).get("seen_content_keys"))
                existing_channel_ids = self._json_text_list((existing or {}).get("seen_channel_ids"))
                existing_user_ids = self._json_text_list((existing or {}).get("seen_user_ids"))

                scope_keys = list(dict.fromkeys(existing_scope_keys + ([scope_key] if scope_key else [])))
                content_keys = list(dict.fromkeys(existing_content_keys + ([content_marker] if content_marker else [])))
                channel_ids = list(dict.fromkeys(existing_channel_ids + ([channel_marker] if channel_marker else [])))
                user_ids = list(dict.fromkeys(existing_user_ids + ([user_marker] if user_marker else [])))

                distinct_scope_count = len(scope_keys)
                distinct_content_count = len(content_keys)
                distinct_channel_count = len(channel_ids)
                distinct_user_count = len(user_ids)
                visibility_eligible, visibility_state = self._topic_visibility_state(
                    distinct_content_count=distinct_content_count,
                    distinct_user_count=distinct_user_count,
                    distinct_channel_count=distinct_channel_count,
                )

                base_payload = {
                    "topic_name": topic_name,
                    "closest_category": proposal.get("closest_category"),
                    "domain": proposal.get("domain"),
                    "latest_evidence": proposal.get("evidence"),
                    "last_seen_at": now_iso,
                    "latest_analysis_id": analysis_id,
                    "latest_channel_id": channel_id,
                    "latest_content_type": content_type,
                    "last_scope_key": scope_key,
                    "seen_scope_keys": scope_keys,
                    "seen_content_keys": content_keys,
                    "seen_channel_ids": channel_ids,
                    "seen_user_ids": user_ids,
                    "distinct_scope_count": distinct_scope_count,
                    "distinct_content_count": distinct_content_count,
                    "distinct_channel_count": distinct_channel_count,
                    "distinct_user_count": distinct_user_count,
                    "visibility_eligible": visibility_eligible,
                    "visibility_state": visibility_state,
                    "updated_at": now_iso,
                }

                legacy_base_payload = {
                    "topic_name": topic_name,
                    "closest_category": proposal.get("closest_category"),
                    "domain": proposal.get("domain"),
                    "latest_evidence": proposal.get("evidence"),
                    "last_seen_at": now_iso,
                    "latest_analysis_id": analysis_id,
                    "latest_channel_id": channel_id,
                    "latest_content_type": content_type,
                    "updated_at": now_iso,
                }

                if existing and existing.get("topic_name"):
                    update_payload = {
                        **base_payload,
                        "proposed_count": max(int(existing.get("proposed_count") or 0), distinct_scope_count),
                    }
                    try:
                        self.client.table("topic_review_queue") \
                            .update(update_payload) \
                            .eq("topic_name", topic_name) \
                            .execute()
                    except Exception:
                        legacy_update_payload = {
                            **legacy_base_payload,
                            "proposed_count": int(existing.get("proposed_count") or 0) + 1,
                        }
                        self.client.table("topic_review_queue") \
                            .update(legacy_update_payload) \
                            .eq("topic_name", topic_name) \
                            .execute()
                else:
                    insert_payload = {
                        **base_payload,
                        "status": "pending",
                        "proposed_count": max(1, distinct_scope_count),
                        "first_seen_at": now_iso,
                        "created_at": now_iso,
                    }
                    try:
                        self.client.table("topic_review_queue") \
                            .insert(insert_payload) \
                            .execute()
                    except Exception:
                        legacy_insert_payload = {
                            **legacy_base_payload,
                            "status": "pending",
                            "proposed_count": 1,
                            "first_seen_at": now_iso,
                            "created_at": now_iso,
                        }
                        self.client.table("topic_review_queue") \
                            .insert(legacy_insert_payload) \
                            .execute()
            except Exception as e:
                self._warn_topic_review_table_once(e)
                break

    def list_topic_proposals(
        self,
        *,
        status: str = "pending",
        limit: int = 100,
    ) -> list[dict]:
        """List taxonomy topic proposals awaiting or post review."""
        try:
            query = self.client.table("topic_review_queue") \
                .select("*") \
                .order("last_seen_at", desc=True) \
                .limit(max(1, min(int(limit), 500)))

            normalized_status = (status or "").strip().lower()
            if normalized_status and normalized_status != "all":
                query = query.eq("status", normalized_status)

            res = query.execute()
            return res.data or []
        except Exception as e:
            self._warn_topic_review_table_once(e)
            return []

    def list_emerging_topic_candidates(
        self,
        *,
        limit: int = 50,
        status: str = "pending",
    ) -> list[dict]:
        """List proposed topics that qualify for frontend emerging visibility."""
        scan_limit = max(100, min(500, max(1, int(limit)) * 8))
        rows = self.list_topic_proposals(status=status, limit=scan_limit)
        output: list[dict] = []

        for row in rows:
            proposed_count = int(row.get("proposed_count") or 0)
            distinct_content_count = int(row.get("distinct_content_count") or 0)
            distinct_user_count = int(row.get("distinct_user_count") or 0)
            distinct_channel_count = int(row.get("distinct_channel_count") or 0)

            if distinct_content_count <= 0:
                distinct_content_count = proposed_count
            if distinct_channel_count <= 0 and row.get("latest_channel_id"):
                distinct_channel_count = 1
            if distinct_user_count <= 0 and row.get("latest_analysis_id"):
                distinct_user_count = 1

            eligible, state = self._topic_visibility_state(
                distinct_content_count=distinct_content_count,
                distinct_user_count=distinct_user_count,
                distinct_channel_count=distinct_channel_count,
            )

            stored_state = str(row.get("visibility_state") or "").strip() or state
            stored_eligible = bool(row.get("visibility_eligible", eligible))
            is_visible = stored_eligible or stored_state == "emerging_visible" or proposed_count >= 3
            if not is_visible:
                continue

            output.append(
                {
                    "topic_name": str(row.get("topic_name") or "").strip(),
                    "closest_category": str(row.get("closest_category") or "General").strip() or "General",
                    "domain": str(row.get("domain") or "General").strip() or "General",
                    "latest_evidence": str(row.get("latest_evidence") or "").strip() or None,
                    "proposed_count": proposed_count,
                    "distinct_content_count": distinct_content_count,
                    "distinct_user_count": distinct_user_count,
                    "distinct_channel_count": distinct_channel_count,
                    "visibility_state": stored_state,
                    "visibility_eligible": stored_eligible,
                    "last_seen_at": row.get("last_seen_at"),
                }
            )

        output.sort(
            key=lambda item: (
                int(item.get("distinct_content_count") or 0),
                int(item.get("distinct_user_count") or 0),
                int(item.get("proposed_count") or 0),
                str(item.get("last_seen_at") or ""),
            ),
            reverse=True,
        )
        return output[:max(1, min(int(limit), 500))]

    def review_topic_proposal(
        self,
        *,
        topic_name: str,
        decision: str,
        canonical_topic: str | None = None,
        aliases: list[str] | None = None,
        notes: str | None = None,
        reviewed_by: str | None = None,
    ) -> dict | None:
        """Approve or reject a proposed topic and optionally promote alias mappings."""
        topic = str(topic_name or "").strip()
        if not topic:
            return None

        normalized_decision = (decision or "").strip().lower()
        if normalized_decision not in {"approve", "reject"}:
            raise ValueError("decision must be 'approve' or 'reject'")

        status = "approved" if normalized_decision == "approve" else "rejected"
        approved_topic = str(canonical_topic or "").strip() if normalized_decision == "approve" else None
        if normalized_decision == "approve" and not approved_topic:
            approved_topic = topic

        now_iso = datetime.now(timezone.utc).isoformat()
        payload = {
            "status": status,
            "approved_topic": approved_topic,
            "review_notes": (notes or "")[:1000] or None,
            "reviewed_by": (reviewed_by or "")[:120] or None,
            "reviewed_at": now_iso,
            "updated_at": now_iso,
        }

        try:
            updated = self.client.table("topic_review_queue") \
                .update(payload) \
                .eq("topic_name", topic) \
                .execute()
            row = updated.data[0] if updated.data else None
        except Exception as e:
            self._warn_topic_review_table_once(e)
            return None

        if normalized_decision == "approve" and approved_topic:
            alias_candidates = [topic]
            for alias in aliases or []:
                cleaned = str(alias or "").strip()
                if cleaned:
                    alias_candidates.append(cleaned)

            seen: set[str] = set()
            for alias in alias_candidates:
                key = alias.lower()
                if key in seen:
                    continue
                seen.add(key)
                try:
                    self.client.table("topic_taxonomy_promotions") \
                        .upsert(
                            {
                                "alias_name": alias,
                                "canonical_topic": approved_topic,
                                "source_topic": topic,
                                "is_active": True,
                                "promoted_by": (reviewed_by or "")[:120] or None,
                                "notes": (notes or "")[:1000] or None,
                                "promoted_at": now_iso,
                                "updated_at": now_iso,
                                "created_at": now_iso,
                            },
                            on_conflict="alias_name",
                        ) \
                        .execute()
                except Exception as e:
                    self._warn_topic_promotion_table_once(e)
                    break

            self.refresh_runtime_topic_aliases()

        return row

    def get_latest_post_analysis(self, post_uuid: str) -> dict | None:
        """Return latest post-level analysis row for a given post UUID."""
        res = self.client.table("ai_analysis") \
            .select("*") \
            .eq("content_type", "post") \
            .eq("content_id", post_uuid) \
            .order("created_at", desc=True) \
            .limit(1) \
            .execute()
        return res.data[0] if res.data else None

    def mark_analysis_synced(self, analysis_uuid: str):
        self.client.table("ai_analysis") \
            .update({"neo4j_synced": True}) \
            .eq("id", analysis_uuid) \
            .execute()

    def mark_analyses_synced(self, analysis_ids: list[str]) -> int:
        ids = [str(item).strip() for item in (analysis_ids or []) if str(item).strip()]
        if not ids:
            return 0
        self.client.table("ai_analysis") \
            .update({"neo4j_synced": True}) \
            .in_("id", ids) \
            .execute()
        return len(ids)

    def mark_post_neo4j_synced(self, post_uuid: str):
        self.client.table("telegram_posts") \
            .update({"neo4j_synced": True}) \
            .eq("id", post_uuid) \
            .execute()

    def mark_posts_neo4j_synced(self, post_ids: list[str]) -> int:
        ids = [str(item).strip() for item in (post_ids or []) if str(item).strip()]
        if not ids:
            return 0
        self.client.table("telegram_posts") \
            .update({"neo4j_synced": True}) \
            .in_("id", ids) \
            .execute()
        return len(ids)

    def reconcile_post_analysis_sync(self, limit: int = 300) -> int:
        """
        Reconcile historic post analyses left unsynced while their posts are synced.

        Returns number of analysis rows marked as synced.
        """
        res = self.client.table("ai_analysis") \
            .select("id, content_id") \
            .eq("content_type", "post") \
            .eq("neo4j_synced", False) \
            .not_.is_("content_id", "null") \
            .order("created_at", desc=False) \
            .limit(max(1, int(limit))) \
            .execute()
        candidates = res.data or []
        if not candidates:
            return 0

        post_ids = list({str(row.get("content_id")) for row in candidates if row.get("content_id")})
        if not post_ids:
            return 0

        synced_posts_res = self.client.table("telegram_posts") \
            .select("id") \
            .in_("id", post_ids) \
            .eq("neo4j_synced", True) \
            .execute()
        synced_posts = {str(row.get("id")) for row in (synced_posts_res.data or []) if row.get("id")}
        if not synced_posts:
            return 0

        reconciled = 0
        for row in candidates:
            analysis_id = row.get("id")
            content_id = row.get("content_id")
            if analysis_id and content_id and str(content_id) in synced_posts:
                self.mark_analysis_synced(str(analysis_id))
                reconciled += 1
        return reconciled

    def _count_rows(self, table_name: str, filters: dict | None = None) -> int | None:
        """Count rows with optional equality filters; returns None on failure."""
        try:
            query = self.client.table(table_name).select("id", count="exact").limit(1)  # type: ignore[arg-type]
            for key, value in (filters or {}).items():
                query = query.eq(key, value)
            res = query.execute()
            count = getattr(res, "count", None)
            if count is None and isinstance(res, dict):
                count = res.get("count")
            return int(count) if count is not None else None
        except Exception as e:
            logger.debug(f"Count query failed for {table_name}: {e}")
            return None

    def _count_rows_since(
        self,
        table_name: str,
        time_column: str,
        since_iso: str,
        filters: dict | None = None,
    ) -> int | None:
        """Count rows newer than the given ISO timestamp, with optional equality filters."""
        try:
            query = self.client.table(table_name).select("id", count="exact").limit(1)  # type: ignore[arg-type]
            query = query.gte(time_column, since_iso)
            for key, value in (filters or {}).items():
                query = query.eq(key, value)
            res = query.execute()
            count = getattr(res, "count", None)
            if count is None and isinstance(res, dict):
                count = res.get("count")
            return int(count) if count is not None else None
        except Exception as e:
            logger.debug(f"Count-since query failed for {table_name}: {e}")
            return None

    def _latest_timestamp(self, table_name: str, column: str, filters: dict | None = None) -> str | None:
        """Fetch latest non-null timestamp-like field from a table."""
        try:
            query = self.client.table(table_name).select(column).not_.is_(column, "null")
            for key, value in (filters or {}).items():
                query = query.eq(key, value)
            res = query.order(column, desc=True).limit(1).execute()
            row = (res.data or [{}])[0]
            return row.get(column)
        except Exception as e:
            logger.debug(f"Latest timestamp query failed for {table_name}.{column}: {e}")
            return None

    def _latest_timestamp_since(
        self,
        table_name: str,
        column: str,
        since_iso: str,
        filters: dict | None = None,
    ) -> str | None:
        """Fetch latest non-null timestamp-like field newer than the given ISO timestamp."""
        try:
            query = self.client.table(table_name).select(column).not_.is_(column, "null").gte(column, since_iso)
            for key, value in (filters or {}).items():
                query = query.eq(key, value)
            res = query.order(column, desc=True).limit(1).execute()
            row = (res.data or [{}])[0]
            return row.get(column)
        except Exception as e:
            logger.debug(f"Latest timestamp-since query failed for {table_name}.{column}: {e}")
            return None

    def _update_rows_since(
        self,
        table_name: str,
        *,
        time_column: str,
        since_iso: str,
        payload: dict,
    ) -> None:
        """Apply an update to rows newer than the given ISO timestamp."""
        self.client.table(table_name) \
            .update(payload) \
            .gte(time_column, since_iso) \
            .execute()

    def _recent_cutoff_iso(self, retention_days: int | None = None) -> str:
        days = max(1, int(retention_days or config.GRAPH_ANALYTICS_RETENTION_DAYS))
        return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    def get_backlog_counts(self) -> dict:
        """Return key pipeline queue counters used for runtime backpressure."""
        failure_counts = self.get_processing_failure_counts()
        return {
            "unprocessed_posts": self._count_rows("telegram_posts", {"is_processed": False}),
            "unprocessed_comments": self._count_rows("telegram_comments", {"is_processed": False}),
            "unsynced_posts": self._count_rows("telegram_posts", {"neo4j_synced": False}),
            "unsynced_analysis": self._count_rows("ai_analysis", {"neo4j_synced": False}),
            "dead_letter_scopes": failure_counts.get("dead_letter_scopes", 0),
            "retry_blocked_scopes": failure_counts.get("retry_blocked_scopes", 0),
        }

    def get_posts_by_ids(self, post_ids: list[str]) -> dict[str, dict]:
        """Fetch post records by UUID list and return dict keyed by id."""
        ids = [str(post_id).strip() for post_id in (post_ids or []) if str(post_id).strip()]
        if not ids:
            return {}

        res = self.client.table("telegram_posts") \
            .select(
                "id, channel_id, telegram_message_id, text, posted_at, "
                "entry_kind, thread_message_count, thread_participant_count, last_activity_at"
            ) \
            .in_("id", ids) \
            .execute()

        return {
            str(row.get("id")): row
            for row in (res.data or [])
            if row.get("id")
        }

    def get_posts_by_message_ids(self, channel_uuid: str, telegram_message_ids: list[int]) -> dict[int, dict]:
        """Fetch post rows for a source keyed by Telegram message id."""
        ids = sorted({
            int(message_id)
            for message_id in (telegram_message_ids or [])
            if isinstance(message_id, int) or (isinstance(message_id, str) and str(message_id).strip().isdigit())
        })
        if not ids:
            return {}

        res = self.client.table("telegram_posts") \
            .select(
                "id, channel_id, telegram_message_id, text, posted_at, entry_kind, "
                "thread_message_count, thread_participant_count, last_activity_at"
            ) \
            .eq("channel_id", channel_uuid) \
            .in_("telegram_message_id", ids) \
            .execute()

        result: dict[int, dict] = {}
        for row in (res.data or []):
            value = row.get("telegram_message_id")
            if value is None:
                continue
            try:
                result[int(value)] = row
            except Exception:
                continue
        return result

    def get_comments_for_post(self, post_uuid: str, limit: int = 25) -> list[dict]:
        """Fetch ordered comments/messages for a single post/thread anchor."""
        res = self.client.table("telegram_comments") \
            .select(
                "id, post_id, channel_id, telegram_message_id, reply_to_message_id, text, "
                "telegram_user_id, posted_at, is_thread_root, thread_top_message_id"
            ) \
            .eq("post_id", post_uuid) \
            .order("posted_at", desc=False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def get_comment_thread_stats(self, post_ids: list[str]) -> dict[str, dict]:
        """Return aggregate message/thread stats for thread anchor posts."""
        ids = [str(post_id).strip() for post_id in (post_ids or []) if str(post_id).strip()]
        if not ids:
            return {}

        res = self.client.table("telegram_comments") \
            .select("post_id, telegram_user_id, posted_at, is_thread_root") \
            .in_("post_id", ids) \
            .execute()

        stats: dict[str, dict] = {
            post_id: {
                "message_count": 0,
                "root_count": 0,
                "participant_ids": set(),
                "last_activity_at": None,
            }
            for post_id in ids
        }

        for row in (res.data or []):
            post_id = str(row.get("post_id") or "").strip()
            if not post_id:
                continue
            bucket = stats.setdefault(
                post_id,
                {
                    "message_count": 0,
                    "root_count": 0,
                    "participant_ids": set(),
                    "last_activity_at": None,
                },
            )
            bucket["message_count"] = int(bucket.get("message_count") or 0) + 1
            if row.get("is_thread_root"):
                bucket["root_count"] = int(bucket.get("root_count") or 0) + 1
            telegram_user_id = row.get("telegram_user_id")
            if telegram_user_id is not None:
                try:
                    bucket["participant_ids"].add(int(telegram_user_id))
                except Exception:
                    pass
            posted_at = row.get("posted_at")
            if posted_at and (
                bucket.get("last_activity_at") is None or str(posted_at) > str(bucket.get("last_activity_at"))
            ):
                bucket["last_activity_at"] = posted_at

        for post_id, bucket in stats.items():
            participant_ids = bucket.pop("participant_ids", set())
            bucket["thread_participant_count"] = len(participant_ids)
            message_count = int(bucket.get("message_count") or 0)
            root_count = int(bucket.get("root_count") or 0)
            bucket["comment_count"] = max(0, message_count - root_count)

        return stats

    def get_pipeline_freshness_snapshot(self) -> dict:
        """
        Return Supabase-side freshness/backlog snapshot for trust monitoring.
        """
        failure_counts = self.get_processing_failure_counts()
        active_channels = self.get_active_channels()
        scraped_times = []
        active_never_scraped = 0

        for channel in active_channels:
            parsed = _parse_iso_datetime(channel.get("last_scraped_at"))
            if parsed:
                scraped_times.append(parsed)
            else:
                active_never_scraped += 1

        last_scrape_at = max(scraped_times).isoformat() if scraped_times else None
        last_post_at = self._latest_timestamp("telegram_posts", "posted_at")
        last_analysis_created_at = self._latest_timestamp("ai_analysis", "created_at")
        last_synced_post_content_at = self._latest_timestamp("telegram_posts", "posted_at", {"neo4j_synced": True})
        last_unsynced_post_content_at = self._latest_timestamp("telegram_posts", "posted_at", {"neo4j_synced": False})
        last_synced_analysis_created_at = self._latest_timestamp("ai_analysis", "created_at", {"neo4j_synced": True})

        return {
            "active_channels": len(active_channels),
            "active_channels_never_scraped": active_never_scraped,
            "last_scrape_at": last_scrape_at,
            "last_post_at": last_post_at,
            "last_process_at": last_analysis_created_at,
            "last_graph_sync_at": last_synced_post_content_at or last_synced_analysis_created_at,
            "last_graph_sync_post_content_at": last_synced_post_content_at,
            "last_graph_sync_analysis_created_at": last_synced_analysis_created_at,
            "last_unsynced_post_content_at": last_unsynced_post_content_at,
            "unprocessed_posts": self._count_rows("telegram_posts", {"is_processed": False}),
            "unprocessed_comments": self._count_rows("telegram_comments", {"is_processed": False}),
            "unsynced_posts": self._count_rows("telegram_posts", {"neo4j_synced": False}),
            "unsynced_analysis": self._count_rows("ai_analysis", {"neo4j_synced": False}),
            "dead_letter_scopes": failure_counts.get("dead_letter_scopes", 0),
            "retry_blocked_scopes": failure_counts.get("retry_blocked_scopes", 0),
            "total_posts": self._count_rows("telegram_posts"),
            "total_comments": self._count_rows("telegram_comments"),
            "total_analysis": self._count_rows("ai_analysis"),
        }

    def get_recent_pipeline_snapshot(self, retention_days: int | None = None) -> dict:
        """Return retention-window counts/timestamps for graph analytics monitoring."""
        since_iso = self._recent_cutoff_iso(retention_days)
        return {
            "window_days": max(1, int(retention_days or config.GRAPH_ANALYTICS_RETENTION_DAYS)),
            "window_start_at": since_iso,
            "recent_posts": self._count_rows_since("telegram_posts", "posted_at", since_iso),
            "recent_comments": self._count_rows_since("telegram_comments", "posted_at", since_iso),
            "recent_unsynced_posts": self._count_rows_since(
                "telegram_posts",
                "posted_at",
                since_iso,
                {"neo4j_synced": False},
            ),
            "recent_last_post_at": self._latest_timestamp_since("telegram_posts", "posted_at", since_iso),
            "recent_last_graph_sync_post_at": self._latest_timestamp_since(
                "telegram_posts",
                "posted_at",
                since_iso,
                {"neo4j_synced": True},
            ),
        }

    def get_unprocessed_posts_since(self, since_iso: str, limit: int = 100) -> list[dict]:
        """Fetch recent posts not yet sent to AI."""
        res = self.client.table("telegram_posts") \
            .select("id, channel_id, telegram_message_id, text, posted_at") \
            .eq("is_processed", False) \
            .gte("posted_at", since_iso) \
            .not_.is_("text", "null") \
            .order("posted_at", desc=False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def get_unprocessed_comments_since(self, since_iso: str, limit: int = 200) -> list[dict]:
        """Fetch recent comments not yet sent to AI."""
        res = self.client.table("telegram_comments") \
            .select("id, post_id, channel_id, telegram_user_id, text, posted_at") \
            .eq("is_processed", False) \
            .gte("posted_at", since_iso) \
            .not_.is_("text", "null") \
            .order("posted_at", desc=False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def get_unsynced_posts_since(self, since_iso: str, limit: int = 100) -> list[dict]:
        """Fetch recent posts not yet fully synced to Neo4j."""
        res = self.client.table("telegram_posts") \
            .select("*") \
            .eq("neo4j_synced", False) \
            .gte("posted_at", since_iso) \
            .order("posted_at", desc=False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def reset_recent_graph_window(self, retention_days: int | None = None) -> dict:
        """
        Mark the recent retention window for reprocessing/re-sync without touching old history.
        """
        since_iso = self._recent_cutoff_iso(retention_days)
        self._update_rows_since(
            "telegram_posts",
            time_column="posted_at",
            since_iso=since_iso,
            payload={"is_processed": False, "neo4j_synced": False},
        )
        self._update_rows_since(
            "telegram_comments",
            time_column="posted_at",
            since_iso=since_iso,
            payload={"is_processed": False},
        )
        self.client.table("ai_analysis") \
            .update({"neo4j_synced": False}) \
            .gte("created_at", since_iso) \
            .execute()

        snapshot = self.get_recent_pipeline_snapshot(retention_days)
        return {
            "window_days": snapshot.get("window_days"),
            "window_start_at": since_iso,
            "recent_posts": int(snapshot.get("recent_posts") or 0),
            "recent_comments": int(snapshot.get("recent_comments") or 0),
        }

    # ── Neo4j Bundle Assembly ────────────────────────────────────────────────

    @staticmethod
    def _analysis_is_newer(candidate: dict | None, existing: dict | None) -> bool:
        if not isinstance(candidate, dict):
            return False
        if not isinstance(existing, dict):
            return True
        return str(candidate.get("created_at") or "") > str(existing.get("created_at") or "")

    def get_unsynced_posts(self, limit: int = 100) -> list[dict]:
        """Fetch posts not yet fully synced to Neo4j graph."""
        res = self.client.table("telegram_posts") \
            .select("*") \
            .eq("neo4j_synced", False) \
            .order("posted_at", desc=False) \
            .limit(limit) \
            .execute()
        return res.data or []

    def get_post_bundles_batch(self, posts: list[dict]) -> list[dict]:
        """Assemble Neo4j bundles for many posts using set-based Supabase queries."""
        ordered_posts = [dict(post) for post in (posts or []) if isinstance(post, dict) and post.get("id")]
        if not ordered_posts:
            return []

        post_ids = [str(post["id"]) for post in ordered_posts]
        channel_ids = sorted({
            str(post.get("channel_id"))
            for post in ordered_posts
            if post.get("channel_id")
        })

        channels_by_id: dict[str, dict] = {}
        if channel_ids:
            ch_res = self.client.table("telegram_channels") \
                .select("*") \
                .in_("id", channel_ids) \
                .execute()
            channels_by_id = {
                str(item.get("id")): item
                for item in (ch_res.data or [])
                if item.get("id")
            }

        comments_by_post: dict[str, list[dict]] = defaultdict(list)
        cmt_res = self.client.table("telegram_comments") \
            .select("*") \
            .in_("post_id", post_ids) \
            .execute()
        for comment in (cmt_res.data or []):
            post_id = str(comment.get("post_id") or "").strip()
            if post_id:
                comments_by_post[post_id].append(comment)

        all_user_ids = sorted({
            int(comment["telegram_user_id"])
            for comments in comments_by_post.values()
            for comment in comments
            if comment.get("telegram_user_id") is not None
        })

        scoped_analyses: dict[tuple[str, str], dict] = {}
        fallback_analyses: dict[tuple[str, str], dict] = {}
        if all_user_ids and channel_ids:
            scoped_res = self.client.table("ai_analysis") \
                .select("*") \
                .eq("content_type", "batch") \
                .in_("channel_id", channel_ids) \
                .in_("content_id", post_ids) \
                .in_("telegram_user_id", all_user_ids) \
                .execute()
            for analysis in (scoped_res.data or []):
                post_id = str(analysis.get("content_id") or "").strip()
                uid = str(analysis.get("telegram_user_id") or "").strip()
                if not post_id or not uid:
                    continue
                key = (post_id, uid)
                if self._analysis_is_newer(analysis, scoped_analyses.get(key)):
                    scoped_analyses[key] = analysis

            fallback_res = self.client.table("ai_analysis") \
                .select("*") \
                .eq("content_type", "batch") \
                .in_("channel_id", channel_ids) \
                .is_("content_id", "null") \
                .in_("telegram_user_id", all_user_ids) \
                .execute()
            for analysis in (fallback_res.data or []):
                channel_id = str(analysis.get("channel_id") or "").strip()
                uid = str(analysis.get("telegram_user_id") or "").strip()
                if not channel_id or not uid:
                    continue
                key = (channel_id, uid)
                if self._analysis_is_newer(analysis, fallback_analyses.get(key)):
                    fallback_analyses[key] = analysis

        post_analyses: dict[str, dict] = {}
        post_analysis_res = self.client.table("ai_analysis") \
            .select("*") \
            .eq("content_type", "post") \
            .in_("content_id", post_ids) \
            .order("created_at", desc=True) \
            .execute()
        for analysis in (post_analysis_res.data or []):
            post_id = str(analysis.get("content_id") or "").strip()
            if post_id and post_id not in post_analyses:
                post_analyses[post_id] = analysis

        bundles: list[dict] = []
        for post in ordered_posts:
            post_id = str(post.get("id") or "").strip()
            channel_id = str(post.get("channel_id") or "").strip()
            comments = list(comments_by_post.get(post_id, []))
            analyses: dict[str, dict] = {}
            for comment in comments:
                uid_value = comment.get("telegram_user_id")
                if uid_value is None:
                    continue
                uid = str(uid_value)
                analysis = scoped_analyses.get((post_id, uid)) or fallback_analyses.get((channel_id, uid))
                if analysis is not None:
                    analyses[uid] = analysis

            post_analysis = post_analyses.get(post_id)
            analysis_records: list[dict] = list(analyses.values())
            if post_analysis and post_analysis.get("id"):
                included_ids = {row.get("id") for row in analysis_records if row.get("id")}
                if post_analysis.get("id") not in included_ids:
                    analysis_records.append(post_analysis)

            bundles.append(
                {
                    "post": post,
                    "channel": channels_by_id.get(channel_id, {}),
                    "comments": comments,
                    "analyses": analyses,
                    "post_analysis": post_analysis,
                    "analysis_records": analysis_records,
                    "reply_user_map": {
                        int(comment["telegram_message_id"]): int(comment["telegram_user_id"])
                        for comment in comments
                        if comment.get("telegram_message_id") and comment.get("telegram_user_id") is not None
                    },
                }
            )

        return bundles

    def get_post_bundle(self, post: dict) -> dict:
        """
        Assemble everything needed to build the Neo4j graph for one post.

        Returns:
          {
            "post":     post dict,
            "channel":  channel dict,
            "comments": [comment dict, ...],
            "analyses": { str(telegram_user_id): analysis_dict, ... },
            "post_analysis": analysis_dict | None,
            "analysis_records": [analysis_dict, ...],
          }
        """
        # Channel
        ch_res = self.client.table("telegram_channels") \
            .select("*") \
            .eq("id", post["channel_id"]) \
            .limit(1) \
            .execute()
        channel = (ch_res.data or [{}])[0]

        # Comments for this post
        cmt_res = self.client.table("telegram_comments") \
            .select("*") \
            .eq("post_id", post["id"]) \
            .execute()
        comments = cmt_res.data or []

        # Build set of telegram_user_ids appearing in comments
        user_ids = list({
            int(c["telegram_user_id"])
            for c in comments
            if c.get("telegram_user_id")
        })

        # Fetch scoped AI analyses for those users in this post.
        # Fallback to legacy channel-scoped `batch` rows only when needed.
        analyses: dict[str, dict] = {}
        if user_ids:
            scoped_res = self.client.table("ai_analysis") \
                .select("*") \
                .eq("channel_id", post["channel_id"]) \
                .eq("content_type", "batch") \
                .eq("content_id", post["id"]) \
                .in_("telegram_user_id", user_ids) \
                .execute()
            for a in (scoped_res.data or []):
                uid = str(a["telegram_user_id"])
                # Keep the MOST RECENT analysis per user (by created_at)
                if uid not in analyses or (
                    (a.get("created_at") or "") > (analyses[uid].get("created_at") or "")
                ):
                    analyses[uid] = a

            missing_user_ids = [uid for uid in user_ids if str(uid) not in analyses]
            if missing_user_ids:
                fallback_res = self.client.table("ai_analysis") \
                    .select("*") \
                    .eq("channel_id", post["channel_id"]) \
                    .eq("content_type", "batch") \
                    .is_("content_id", "null") \
                    .in_("telegram_user_id", missing_user_ids) \
                    .execute()

                for a in (fallback_res.data or []):
                    uid = str(a["telegram_user_id"])
                    if uid in analyses:
                        continue
                    if uid not in analyses or (
                        (a.get("created_at") or "") > (analyses[uid].get("created_at") or "")
                    ):
                        analyses[uid] = a

        post_analysis = self.get_latest_post_analysis(post["id"])

        analysis_records: list[dict] = list(analyses.values())
        if post_analysis and post_analysis.get("id"):
            included_ids = {row.get("id") for row in analysis_records if row.get("id")}
            if post_analysis.get("id") not in included_ids:
                analysis_records.append(post_analysis)

        return {
            "post":           post,
            "channel":        channel,
            "comments":       comments,
            "analyses":       analyses,
            "post_analysis":  post_analysis,
            "analysis_records": analysis_records,
            # Maps telegram_message_id → telegram_user_id for User→User network
            # Used by neo4j_writer to resolve who was replied to
            "reply_user_map": {
                int(c["telegram_message_id"]): int(c["telegram_user_id"])
                for c in comments
                if c.get("telegram_message_id") and c.get("telegram_user_id")
            },
        }
