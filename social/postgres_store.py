from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

from loguru import logger

import config

try:  # pragma: no cover - optional dependency in local dev until requirements are installed
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


class SocialPostgresStore:
    """Direct Postgres access for lease-based worker operations."""

    def __init__(self, database_url: str | None = None) -> None:
        self.database_url = (database_url or config.SOCIAL_DATABASE_URL).strip()
        if self.database_url and psycopg is None:
            logger.warning("SOCIAL_DATABASE_URL is configured but psycopg is not installed; falling back to Supabase worker path")

    @property
    def enabled(self) -> bool:
        return bool(self.database_url and psycopg is not None)

    @contextmanager
    def _connection(self) -> Iterator[Any]:
        if not self.enabled:
            raise RuntimeError("Direct social Postgres path is not available")
        conn = psycopg.connect(self.database_url, row_factory=dict_row)
        try:
            yield conn
        finally:
            conn.close()

    def claim_collect_accounts(
        self,
        *,
        worker_id: str,
        platforms: list[str],
        limit: int,
        lease_seconds: int,
    ) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        query = """
        WITH candidates AS (
          SELECT sea.id
          FROM public.social_entity_accounts AS sea
          JOIN public.social_entities AS se
            ON se.id = sea.entity_id
          WHERE sea.is_active = TRUE
            AND se.is_active = TRUE
            AND sea.platform = ANY(%s)
            AND COALESCE(
              NULLIF(BTRIM(sea.account_external_id), ''),
              NULLIF(BTRIM(sea.account_handle), ''),
              NULLIF(BTRIM(sea.domain), ''),
              NULLIF(BTRIM(sea.metadata ->> 'page_url'), ''),
              NULLIF(BTRIM(sea.metadata ->> 'source_url'), '')
            ) IS NOT NULL
            AND (
              sea.collect_claimed_at IS NULL
              OR sea.collect_claimed_at < NOW() - (%s * INTERVAL '1 second')
            )
            AND NOT EXISTS (
              SELECT 1
              FROM public.social_processing_failures AS failure
              WHERE failure.stage = 'ingest'
                AND failure.scope_key = CONCAT(sea.entity_id::text, ':', sea.platform, ':', sea.source_kind)
                AND failure.resolved_at IS NULL
                AND (
                  failure.is_dead_letter = TRUE
                  OR failure.next_retry_at IS NULL
                  OR failure.next_retry_at > NOW()
                )
            )
          ORDER BY COALESCE(sea.last_collected_at, to_timestamp(0)) ASC, sea.updated_at ASC
          LIMIT %s
          FOR UPDATE SKIP LOCKED
        )
        UPDATE public.social_entity_accounts AS sea
        SET collect_claimed_at = NOW(),
            collect_claimed_by = %s
        FROM candidates
        WHERE sea.id = candidates.id
        RETURNING
          sea.*,
          (
            SELECT row_to_json(se)
            FROM public.social_entities AS se
            WHERE se.id = sea.entity_id
          ) AS entity
        """
        with self._connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(query, (platforms, lease_seconds, limit, worker_id))
                    rows = list(cur.fetchall())
        return rows

    def claim_analysis_activities(
        self,
        *,
        worker_id: str,
        limit: int,
        lease_seconds: int,
        analysis_version: str,
    ) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        query = """
        WITH candidates AS (
          SELECT sa.id
          FROM public.social_activities AS sa
          WHERE sa.ingest_status = 'normalized'
            AND (
              sa.analysis_status IN ('pending', 'failed')
              OR COALESCE(sa.analysis_version, '') <> %s
            )
            AND sa.analysis_status <> 'dead_letter'
            AND (
              sa.analysis_claimed_at IS NULL
              OR sa.analysis_claimed_at < NOW() - (%s * INTERVAL '1 second')
            )
            AND NOT EXISTS (
              SELECT 1
              FROM public.social_processing_failures AS failure
              WHERE failure.stage = 'analysis'
                AND failure.scope_key = sa.activity_uid
                AND failure.resolved_at IS NULL
                AND (
                  failure.is_dead_letter = TRUE
                  OR failure.next_retry_at IS NULL
                  OR failure.next_retry_at > NOW()
                )
            )
          ORDER BY COALESCE(sa.last_seen_at, sa.created_at) ASC
          LIMIT %s
          FOR UPDATE SKIP LOCKED
        )
        UPDATE public.social_activities AS sa
        SET analysis_claimed_at = NOW(),
            analysis_claimed_by = %s
        FROM candidates
        WHERE sa.id = candidates.id
        RETURNING
          sa.*,
          (
            SELECT row_to_json(se)
            FROM public.social_entities AS se
            WHERE se.id = sa.entity_id
          ) AS entity
        """
        with self._connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(query, (analysis_version, lease_seconds, limit, worker_id))
                    rows = list(cur.fetchall())
        return rows

    def claim_graph_activities(
        self,
        *,
        worker_id: str,
        limit: int,
        lease_seconds: int,
        projection_version: str,
    ) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        query = """
        WITH candidates AS (
          SELECT sa.id
          FROM public.social_activities AS sa
          WHERE sa.ingest_status = 'normalized'
            AND sa.analysis_status = 'analyzed'
            AND (
              sa.graph_status IN ('pending', 'failed')
              OR COALESCE(sa.graph_projection_version, '') <> %s
            )
            AND sa.graph_status <> 'dead_letter'
            AND (
              sa.graph_claimed_at IS NULL
              OR sa.graph_claimed_at < NOW() - (%s * INTERVAL '1 second')
            )
            AND NOT EXISTS (
              SELECT 1
              FROM public.social_processing_failures AS failure
              WHERE failure.stage = 'graph'
                AND failure.scope_key = sa.activity_uid
                AND failure.resolved_at IS NULL
                AND (
                  failure.is_dead_letter = TRUE
                  OR failure.next_retry_at IS NULL
                  OR failure.next_retry_at > NOW()
                )
            )
          ORDER BY COALESCE(sa.last_seen_at, sa.created_at) ASC
          LIMIT %s
          FOR UPDATE SKIP LOCKED
        )
        UPDATE public.social_activities AS sa
        SET graph_claimed_at = NOW(),
            graph_claimed_by = %s
        FROM candidates
        WHERE sa.id = candidates.id
        RETURNING
          sa.*,
          (
            SELECT row_to_json(se)
            FROM public.social_entities AS se
            WHERE se.id = sa.entity_id
          ) AS entity,
          (
            SELECT row_to_json(analysis)
            FROM public.social_activity_analysis AS analysis
            WHERE analysis.activity_id = sa.id
          ) AS analysis
        """
        with self._connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(query, (projection_version, lease_seconds, limit, worker_id))
                    rows = [row for row in cur.fetchall() if row.get("analysis")]
        return rows

    def cleanup(self, *, lease_seconds: int, payload_retention_days: int) -> dict[str, int]:
        if not self.enabled:
            return {
                "payloads_redacted": 0,
                "collect_claims_released": 0,
                "analysis_claims_released": 0,
                "graph_claims_released": 0,
            }
        statements = {
            "payloads_redacted": """
                UPDATE public.social_activities
                SET provider_payload = '{}'::jsonb
                WHERE provider_payload <> '{}'::jsonb
                  AND first_seen_at < NOW() - (%s * INTERVAL '1 day')
            """,
            "collect_claims_released": """
                UPDATE public.social_entity_accounts
                SET collect_claimed_at = NULL,
                    collect_claimed_by = NULL
                WHERE collect_claimed_at IS NOT NULL
                  AND collect_claimed_at < NOW() - (%s * INTERVAL '1 second')
            """,
            "analysis_claims_released": """
                UPDATE public.social_activities
                SET analysis_claimed_at = NULL,
                    analysis_claimed_by = NULL
                WHERE analysis_claimed_at IS NOT NULL
                  AND analysis_claimed_at < NOW() - (%s * INTERVAL '1 second')
            """,
            "graph_claims_released": """
                UPDATE public.social_activities
                SET graph_claimed_at = NULL,
                    graph_claimed_by = NULL
                WHERE graph_claimed_at IS NOT NULL
                  AND graph_claimed_at < NOW() - (%s * INTERVAL '1 second')
            """,
        }
        result: dict[str, int] = {}
        with self._connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(statements["payloads_redacted"], (payload_retention_days,))
                    result["payloads_redacted"] = cur.rowcount
                    cur.execute(statements["collect_claims_released"], (lease_seconds,))
                    result["collect_claims_released"] = cur.rowcount
                    cur.execute(statements["analysis_claims_released"], (lease_seconds,))
                    result["analysis_claims_released"] = cur.rowcount
                    cur.execute(statements["graph_claims_released"], (lease_seconds,))
                    result["graph_claims_released"] = cur.rowcount
        return result
