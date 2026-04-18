from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from loguru import logger

from api.dashboard_v2_registry import FACT_TABLE_BY_FAMILY, SECONDARY_MATERIALIZATION_TABLES
from buffer.supabase_writer import SupabaseWriter


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _as_iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value else None


def _normalize_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _normalize_str_list(values: list[str] | tuple[str, ...] | None) -> list[str]:
    return [str(value).strip() for value in (values or []) if str(value).strip()]


def compute_max_fact_watermark(dependency_watermarks: dict[str, Any]) -> datetime | None:
    candidates = [_parse_dt(value) for value in _normalize_json(dependency_watermarks).values()]
    candidates = [item for item in candidates if item is not None]
    return max(candidates) if candidates else None


def compute_stale_fact_families(
    artifact_dependency_watermarks: dict[str, Any],
    latest_dependency_watermarks: dict[str, Any],
) -> list[str]:
    stale: list[str] = []
    artifact_map = _normalize_json(artifact_dependency_watermarks)
    latest_map = _normalize_json(latest_dependency_watermarks)
    for family, latest_raw in latest_map.items():
        latest_dt = _parse_dt(latest_raw)
        artifact_dt = _parse_dt(artifact_map.get(family))
        if latest_dt is None:
            continue
        if artifact_dt is None or latest_dt > artifact_dt:
            stale.append(str(family))
    return sorted(set(stale))


def same_key_last_known_good_allowed(
    *,
    request_from: date,
    request_to: date,
    artifact_from: date,
    artifact_to: date,
    artifact_is_stale: bool,
    newer_exact_exists: bool,
) -> bool:
    return bool(
        artifact_is_stale
        and not newer_exact_exists
        and request_from == artifact_from
        and request_to == artifact_to
    )


@dataclass(frozen=True)
class DashboardV2FactRow:
    row_key: str
    payload_json: dict[str, Any]
    source_event_at: datetime | None = None
    topic_key: str | None = None
    channel_id: str | None = None
    user_id: str | None = None
    content_type: str | None = None
    cohort_key: str | None = None


class DashboardV2Store:
    def __init__(self, writer: SupabaseWriter):
        self.writer = writer

    def create_fact_run(
        self,
        *,
        fact_family: str,
        fact_version: int,
        coverage_start: date,
        coverage_end: date,
        meta_json: dict[str, Any] | None = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        self.writer.pg_execute(
            """
            INSERT INTO public.dashboard_fact_runs (
              run_id,
              fact_family,
              fact_version,
              coverage_start,
              coverage_end,
              meta_json
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                run_id,
                fact_family,
                int(fact_version),
                coverage_start,
                coverage_end,
                json.dumps(meta_json or {}, ensure_ascii=True, separators=(",", ":")),
            ),
        )
        return run_id

    def complete_fact_run(
        self,
        run_id: str,
        *,
        status: str,
        source_watermark: datetime | None = None,
        error: str | None = None,
        meta_json: dict[str, Any] | None = None,
    ) -> None:
        self.writer.pg_execute(
            """
            UPDATE public.dashboard_fact_runs
            SET status = %s,
                source_watermark = %s,
                error = %s,
                materialized_at = timezone('utc', now()),
                meta_json = COALESCE(meta_json, '{}'::jsonb) || %s::jsonb
            WHERE run_id = %s::uuid
            """,
            (
                status,
                source_watermark,
                error,
                json.dumps(meta_json or {}, ensure_ascii=True, separators=(",", ":")),
                run_id,
            ),
        )

    def replace_daily_fact_rows(
        self,
        *,
        fact_family: str,
        fact_date: date,
        run_id: str,
        fact_version: int,
        rows: list[DashboardV2FactRow],
        source_watermark: datetime | None = None,
    ) -> int:
        table_name = FACT_TABLE_BY_FAMILY[fact_family]
        with self.writer._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(f"DELETE FROM public.{table_name} WHERE fact_date = %s", (fact_date,))
            if not rows:
                return 0
            if fact_family == "content":
                cur.executemany(
                    f"""
                    INSERT INTO public.{table_name} (
                      fact_date,
                      row_key,
                      run_id,
                      fact_version,
                      source_watermark,
                      source_event_at,
                      topic_key,
                      channel_id,
                      user_id,
                      content_type,
                      payload_json
                    )
                    VALUES (%s, %s, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    [
                        (
                            fact_date,
                            row.row_key,
                            run_id,
                            int(fact_version),
                            source_watermark,
                            row.source_event_at,
                            row.topic_key,
                            row.channel_id,
                            row.user_id,
                            row.content_type,
                            json.dumps(row.payload_json, ensure_ascii=True, separators=(",", ":")),
                        )
                        for row in rows
                    ],
                )
            elif fact_family == "topics":
                cur.executemany(
                    f"""
                    INSERT INTO public.{table_name} (
                      fact_date,
                      row_key,
                      run_id,
                      fact_version,
                      source_watermark,
                      topic_key,
                      payload_json
                    )
                    VALUES (%s, %s, %s::uuid, %s, %s, %s, %s::jsonb)
                    """,
                    [
                        (
                            fact_date,
                            row.row_key,
                            run_id,
                            int(fact_version),
                            source_watermark,
                            row.topic_key,
                            json.dumps(row.payload_json, ensure_ascii=True, separators=(",", ":")),
                        )
                        for row in rows
                    ],
                )
            elif fact_family == "channels":
                cur.executemany(
                    f"""
                    INSERT INTO public.{table_name} (
                      fact_date,
                      row_key,
                      run_id,
                      fact_version,
                      source_watermark,
                      channel_id,
                      payload_json
                    )
                    VALUES (%s, %s, %s::uuid, %s, %s, %s, %s::jsonb)
                    """,
                    [
                        (
                            fact_date,
                            row.row_key,
                            run_id,
                            int(fact_version),
                            source_watermark,
                            row.channel_id,
                            json.dumps(row.payload_json, ensure_ascii=True, separators=(",", ":")),
                        )
                        for row in rows
                    ],
                )
            elif fact_family == "users":
                cur.executemany(
                    f"""
                    INSERT INTO public.{table_name} (
                      fact_date,
                      row_key,
                      run_id,
                      fact_version,
                      source_watermark,
                      user_id,
                      cohort_key,
                      payload_json
                    )
                    VALUES (%s, %s, %s::uuid, %s, %s, %s, %s, %s::jsonb)
                    """,
                    [
                        (
                            fact_date,
                            row.row_key,
                            run_id,
                            int(fact_version),
                            source_watermark,
                            row.user_id,
                            row.cohort_key,
                            json.dumps(row.payload_json, ensure_ascii=True, separators=(",", ":")),
                        )
                        for row in rows
                    ],
                )
            else:
                cur.executemany(
                    f"""
                    INSERT INTO public.{table_name} (
                      fact_date,
                      row_key,
                      run_id,
                      fact_version,
                      source_watermark,
                      payload_json
                    )
                    VALUES (%s, %s, %s::uuid, %s, %s, %s::jsonb)
                    """,
                    [
                        (
                            fact_date,
                            row.row_key,
                            run_id,
                            int(fact_version),
                            source_watermark,
                            json.dumps(row.payload_json, ensure_ascii=True, separators=(",", ":")),
                        )
                        for row in rows
                    ],
                )
            return len(rows)

    def upsert_secondary_materialization(
        self,
        *,
        storage_key: str,
        widget_id: str,
        window_start: date,
        window_end: date,
        payload_json: dict[str, Any],
        status: str = "ready",
        meta_json: dict[str, Any] | None = None,
        source_watermark: datetime | None = None,
    ) -> None:
        table_name = SECONDARY_MATERIALIZATION_TABLES[storage_key]
        self.writer.pg_execute(
            f"""
            INSERT INTO public.{table_name} (
              widget_id,
              window_start,
              window_end,
              source_watermark,
              status,
              payload_json,
              meta_json
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
            ON CONFLICT (widget_id, window_start, window_end) DO UPDATE
            SET source_watermark = EXCLUDED.source_watermark,
                materialized_at = timezone('utc', now()),
                status = EXCLUDED.status,
                payload_json = EXCLUDED.payload_json,
                meta_json = EXCLUDED.meta_json
            """,
            (
                widget_id,
                window_start,
                window_end,
                source_watermark,
                status,
                json.dumps(payload_json or {}, ensure_ascii=True, separators=(",", ":")),
                json.dumps(meta_json or {}, ensure_ascii=True, separators=(",", ":")),
            ),
        )

    def upsert_range_artifact(
        self,
        *,
        cache_key: str,
        from_date: date,
        to_date: date,
        range_mode: str,
        payload_json: dict[str, Any],
        dependency_watermarks: dict[str, Any],
        summary_granularity: str | None = None,
        artifact_version: int = 1,
        is_stale: bool = False,
        stale_fact_families: list[str] | None = None,
        stale_reason: str | None = None,
    ) -> None:
        fact_watermark = compute_max_fact_watermark(dependency_watermarks)
        normalized_watermarks = {
            key: _as_iso(_parse_dt(value))
            for key, value in _normalize_json(dependency_watermarks).items()
        }
        self.writer.pg_execute(
            """
            INSERT INTO public.dashboard_range_artifacts_v2 (
              cache_key,
              from_date,
              to_date,
              range_mode,
              summary_granularity,
              fact_watermark,
              artifact_version,
              payload_json,
              dependency_watermarks,
              stale_fact_families,
              stale_reason,
              is_stale
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::text[], %s, %s)
            ON CONFLICT (cache_key) DO UPDATE
            SET from_date = EXCLUDED.from_date,
                to_date = EXCLUDED.to_date,
                range_mode = EXCLUDED.range_mode,
                summary_granularity = EXCLUDED.summary_granularity,
                fact_watermark = EXCLUDED.fact_watermark,
                materialized_at = timezone('utc', now()),
                built_at = timezone('utc', now()),
                artifact_version = EXCLUDED.artifact_version,
                payload_json = EXCLUDED.payload_json,
                dependency_watermarks = EXCLUDED.dependency_watermarks,
                stale_fact_families = EXCLUDED.stale_fact_families,
                stale_reason = EXCLUDED.stale_reason,
                is_stale = EXCLUDED.is_stale
            """,
            (
                cache_key,
                from_date,
                to_date,
                range_mode,
                summary_granularity,
                fact_watermark,
                int(artifact_version),
                json.dumps(payload_json or {}, ensure_ascii=True, separators=(",", ":")),
                json.dumps(normalized_watermarks, ensure_ascii=True, separators=(",", ":")),
                _normalize_str_list(stale_fact_families),
                stale_reason,
                bool(is_stale),
            ),
        )

    def mark_overlapping_artifacts_stale(
        self,
        *,
        fact_family: str,
        changed_date: date,
        new_watermark: datetime | None = None,
        reason: str | None = None,
    ) -> int:
        normalized_reason = reason or f"{fact_family}:{changed_date.isoformat()}"
        return self.writer.pg_execute(
            """
            UPDATE public.dashboard_range_artifacts_v2
            SET is_stale = TRUE,
                stale_reason = %s,
                stale_fact_families = (
                  SELECT ARRAY(
                    SELECT DISTINCT item
                    FROM unnest(
                      COALESCE(dashboard_range_artifacts_v2.stale_fact_families, '{}'::text[])
                      || ARRAY[%s]::text[]
                    ) AS item
                  )
                )
            WHERE from_date <= %s
              AND to_date >= %s
              AND (
                fact_watermark IS NULL
                OR %s IS NULL
                OR fact_watermark < %s
              )
            """,
            (
                normalized_reason,
                fact_family,
                changed_date,
                changed_date,
                new_watermark,
                new_watermark,
            ),
        )

    def get_range_artifact(self, cache_key: str) -> dict[str, Any] | None:
        return self.writer.pg_fetchone(
            """
            SELECT
              cache_key,
              from_date,
              to_date,
              range_mode,
              summary_granularity,
              fact_watermark,
              materialized_at,
              built_at,
              artifact_version,
              payload_json,
              dependency_watermarks,
              stale_fact_families,
              stale_reason,
              is_stale
            FROM public.dashboard_range_artifacts_v2
            WHERE cache_key = %s
            """,
            (cache_key,),
        )

    def create_compare_run(
        self,
        *,
        cache_key: str,
        from_date: date,
        to_date: date,
        old_path_meta: dict[str, Any],
        v2_meta: dict[str, Any],
        direct_truth_meta: dict[str, Any],
        diff_json: dict[str, Any],
    ) -> str:
        compare_id = str(uuid.uuid4())
        self.writer.pg_execute(
            """
            INSERT INTO public.dashboard_compare_runs (
              compare_id,
              cache_key,
              from_date,
              to_date,
              old_path_meta,
              v2_meta,
              direct_truth_meta,
              diff_json
            )
            VALUES (%s::uuid, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb)
            """,
            (
                compare_id,
                cache_key,
                from_date,
                to_date,
                json.dumps(old_path_meta or {}, ensure_ascii=True, separators=(",", ":")),
                json.dumps(v2_meta or {}, ensure_ascii=True, separators=(",", ":")),
                json.dumps(direct_truth_meta or {}, ensure_ascii=True, separators=(",", ":")),
                json.dumps(diff_json or {}, ensure_ascii=True, separators=(",", ":")),
            ),
        )
        return compare_id

    def list_recent_fact_runs(self, *, limit: int = 25) -> list[dict[str, Any]]:
        return self.writer.pg_fetchall(
            """
            SELECT
              run_id,
              fact_family,
              fact_version,
              coverage_start,
              coverage_end,
              source_watermark,
              materialized_at,
              status,
              error,
              meta_json
            FROM public.dashboard_fact_runs
            ORDER BY materialized_at DESC
            LIMIT %s
            """,
            (max(1, int(limit)),),
        )

    def list_recent_compare_runs(self, *, limit: int = 10) -> list[dict[str, Any]]:
        return self.writer.pg_fetchall(
            """
            SELECT
              compare_id,
              cache_key,
              from_date,
              to_date,
              old_path_meta,
              v2_meta,
              direct_truth_meta,
              diff_json,
              created_at
            FROM public.dashboard_compare_runs
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (max(1, int(limit)),),
        )

    def summarize_recent_artifacts(self, *, limit: int = 25) -> list[dict[str, Any]]:
        return self.writer.pg_fetchall(
            """
            SELECT
              cache_key,
              from_date,
              to_date,
              range_mode,
              summary_granularity,
              fact_watermark,
              materialized_at,
              built_at,
              is_stale,
              stale_fact_families,
              stale_reason
            FROM public.dashboard_range_artifacts_v2
            ORDER BY built_at DESC
            LIMIT %s
            """,
            (max(1, int(limit)),),
        )

    def status_snapshot(self, *, run_limit: int = 20, artifact_limit: int = 20) -> dict[str, Any]:
        try:
            return {
                "factRuns": self.list_recent_fact_runs(limit=run_limit),
                "artifacts": self.summarize_recent_artifacts(limit=artifact_limit),
                "compareRuns": self.list_recent_compare_runs(limit=min(run_limit, 10)),
            }
        except Exception as exc:
            logger.warning(f"Dashboard V2 status snapshot failed: {exc}")
            raise
