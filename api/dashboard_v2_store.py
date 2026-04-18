from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger

from api.dashboard_v2_keys import build_dashboard_v2_coverage_row_key
from api.dashboard_v2_registry import (
    FACT_FAMILIES,
    FACT_TABLE_BY_FAMILY,
    FULL_DASHBOARD_REQUIRED_FACT_FAMILIES,
    SECONDARY_MATERIALIZATION_TABLES,
)
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


_COVERAGE_ROW_KEY = build_dashboard_v2_coverage_row_key()


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

    def get_exact_secondary_materialization(
        self,
        *,
        storage_key: str,
        widget_id: str,
        window_start: date,
        window_end: date,
    ) -> dict[str, Any] | None:
        table_name = SECONDARY_MATERIALIZATION_TABLES[storage_key]
        return self.writer.pg_fetchone(
            f"""
            SELECT
              widget_id,
              window_start,
              window_end,
              source_watermark,
              materialized_at,
              status,
              payload_json,
              meta_json
            FROM public.{table_name}
            WHERE widget_id = %s
              AND window_start = %s
              AND window_end = %s
            """,
            (widget_id, window_start, window_end),
        )

    def fetch_fact_rows_for_range(
        self,
        *,
        fact_family: str,
        from_date: date,
        to_date: date,
        min_fact_version: int = 1,
        row_keys: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        table_name = FACT_TABLE_BY_FAMILY[fact_family]
        filters: list[str] = ["fact_date >= %s", "fact_date <= %s", "fact_version >= %s"]
        params: list[Any] = [from_date, to_date, int(min_fact_version)]
        if row_keys:
            filters.append("row_key = ANY(%s)")
            params.append(list(row_keys))
        if fact_family == "content":
            sql = f"""
            SELECT
              fact_date,
              row_key,
              run_id,
              fact_version,
              source_watermark,
              materialized_at,
              payload_json,
              source_event_at,
              topic_key,
              channel_id,
              user_id,
              content_type,
              NULL::text AS cohort_key
            FROM public.{table_name}
            WHERE {" AND ".join(filters)}
            ORDER BY fact_date ASC, row_key ASC
            """
        elif fact_family == "topics":
            sql = f"""
            SELECT
              fact_date,
              row_key,
              run_id,
              fact_version,
              source_watermark,
              materialized_at,
              payload_json,
              NULL::timestamptz AS source_event_at,
              topic_key,
              NULL::text AS channel_id,
              NULL::text AS user_id,
              NULL::text AS content_type,
              NULL::text AS cohort_key
            FROM public.{table_name}
            WHERE {" AND ".join(filters)}
            ORDER BY fact_date ASC, row_key ASC
            """
        elif fact_family == "channels":
            sql = f"""
            SELECT
              fact_date,
              row_key,
              run_id,
              fact_version,
              source_watermark,
              materialized_at,
              payload_json,
              NULL::timestamptz AS source_event_at,
              NULL::text AS topic_key,
              channel_id,
              NULL::text AS user_id,
              NULL::text AS content_type,
              NULL::text AS cohort_key
            FROM public.{table_name}
            WHERE {" AND ".join(filters)}
            ORDER BY fact_date ASC, row_key ASC
            """
        elif fact_family == "users":
            sql = f"""
            SELECT
              fact_date,
              row_key,
              run_id,
              fact_version,
              source_watermark,
              materialized_at,
              payload_json,
              NULL::timestamptz AS source_event_at,
              NULL::text AS topic_key,
              NULL::text AS channel_id,
              user_id,
              NULL::text AS content_type,
              cohort_key
            FROM public.{table_name}
            WHERE {" AND ".join(filters)}
            ORDER BY fact_date ASC, row_key ASC
            """
        else:
            sql = f"""
            SELECT
              fact_date,
              row_key,
              run_id,
              fact_version,
              source_watermark,
              materialized_at,
              payload_json,
              NULL::timestamptz AS source_event_at,
              NULL::text AS topic_key,
              NULL::text AS channel_id,
              NULL::text AS user_id,
              NULL::text AS content_type,
              NULL::text AS cohort_key
            FROM public.{table_name}
            WHERE {" AND ".join(filters)}
            ORDER BY fact_date ASC, row_key ASC
            """
        return self.writer.pg_fetchall(sql, tuple(params))

    def get_max_fact_watermark_for_range(
        self,
        *,
        fact_family: str,
        from_date: date,
        to_date: date,
        min_fact_version: int = 1,
    ) -> datetime | None:
        table_name = FACT_TABLE_BY_FAMILY[fact_family]
        row = self.writer.pg_fetchone(
            f"""
            SELECT MAX(source_watermark) AS fact_watermark
            FROM public.{table_name}
            WHERE fact_date >= %s
              AND fact_date <= %s
              AND fact_version >= %s
            """,
            (from_date, to_date, int(min_fact_version)),
        )
        return _parse_dt((row or {}).get("fact_watermark"))

    def latest_dependency_watermarks_for_range(
        self,
        *,
        from_date: date,
        to_date: date,
        fact_families: list[str] | tuple[str, ...] | None = None,
        secondary_dependencies: dict[str, tuple[str, str]] | None = None,
        min_fact_version: int = 1,
    ) -> dict[str, str | None]:
        watermarks: dict[str, str | None] = {}
        for fact_family in (fact_families or FULL_DASHBOARD_REQUIRED_FACT_FAMILIES):
            watermarks[fact_family] = _as_iso(
                self.get_max_fact_watermark_for_range(
                    fact_family=fact_family,
                    from_date=from_date,
                    to_date=to_date,
                    min_fact_version=min_fact_version,
                )
            )
        for dependency_name, (storage_key, widget_id) in (secondary_dependencies or {}).items():
            row = self.get_exact_secondary_materialization(
                storage_key=storage_key,
                widget_id=widget_id,
                window_start=from_date,
                window_end=to_date,
            )
            watermarks[dependency_name] = _as_iso(_parse_dt((row or {}).get("source_watermark")))
        return watermarks

    def mark_secondary_materialization_stale(
        self,
        *,
        dependency_name: str,
        window_start: date,
        window_end: date,
        new_watermark: datetime | None = None,
        reason: str | None = None,
    ) -> int:
        normalized_reason = reason or f"{dependency_name}:{window_start.isoformat()}:{window_end.isoformat()}"
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
              AND from_date <= %s
              AND to_date >= %s
              AND (
                fact_watermark IS NULL
                OR %s IS NULL
                OR fact_watermark < %s
              )
            """,
            (
                normalized_reason,
                dependency_name,
                window_end,
                window_start,
                window_start,
                window_end,
                new_watermark,
                new_watermark,
            ),
        )

    def exact_artifact_has_newer_same_key(self, *, cache_key: str, materialized_at: datetime | None) -> bool:
        if materialized_at is None:
            return False
        row = self.writer.pg_fetchone(
            """
            SELECT 1 AS newer
            FROM public.dashboard_range_artifacts_v2
            WHERE cache_key = %s
              AND materialized_at > %s
            LIMIT 1
            """,
            (cache_key, materialized_at),
        )
        return bool(row)

    def _coverage_rows_for_family(
        self,
        *,
        fact_family: str,
        from_date: date,
        to_date: date,
        min_fact_version: int,
    ) -> list[dict[str, Any]]:
        return self.fetch_fact_rows_for_range(
            fact_family=fact_family,
            from_date=from_date,
            to_date=to_date,
            min_fact_version=min_fact_version,
            row_keys=[_COVERAGE_ROW_KEY],
        )

    def _coverage_summary_from_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        requested_start: date,
        requested_end: date,
        min_fact_version: int,
    ) -> dict[str, Any]:
        degraded_dates: list[str] = []
        failed_widgets: set[str] = set()
        covered_dates = sorted(
            {
                row.get("fact_date")
                for row in rows
                if isinstance(row.get("payload_json"), dict)
                and bool(row["payload_json"].get("coverageReady"))
                and int(row.get("fact_version") or 0) >= int(min_fact_version)
            }
        )
        for row in rows:
            payload = row.get("payload_json")
            fact_date = row.get("fact_date")
            if not isinstance(payload, dict) or not isinstance(fact_date, date):
                continue
            if int(row.get("fact_version") or 0) < int(min_fact_version):
                continue
            if bool(payload.get("coverageDegraded")) or list(payload.get("failedWidgets") or []):
                degraded_dates.append(fact_date.isoformat())
                failed_widgets.update(
                    str(widget_id)
                    for widget_id in (payload.get("failedWidgets") or [])
                    if str(widget_id).strip()
                )
        date_set = {item for item in covered_dates if isinstance(item, date)}
        latest_fact_version = max([int(row.get("fact_version") or 0) for row in rows], default=0)
        last_materialized = max([_parse_dt(row.get("materialized_at")) for row in rows if row.get("materialized_at")], default=None)
        last_source_watermark = max([_parse_dt(row.get("source_watermark")) for row in rows if row.get("source_watermark")], default=None)
        coverage_start = min(date_set) if date_set else None
        coverage_end = max(date_set) if date_set else None
        gap_count = 0
        if coverage_start and coverage_end:
            current = coverage_start
            while current <= coverage_end:
                if current not in date_set:
                    gap_count += 1
                current += timedelta(days=1)
        contiguous_end = coverage_end
        contiguous_start = coverage_end
        if coverage_end:
            probe = coverage_end
            while probe in date_set:
                contiguous_start = probe
                probe -= timedelta(days=1)
        missing_dates: list[str] = []
        current = requested_start
        while current <= requested_end:
            if current not in date_set:
                missing_dates.append(current.isoformat())
            current += timedelta(days=1)
        return {
            "latestFactVersion": latest_fact_version,
            "coverageStart": coverage_start.isoformat() if coverage_start else None,
            "coverageEnd": coverage_end.isoformat() if coverage_end else None,
            "continuousCoverageStart": contiguous_start.isoformat() if contiguous_start else None,
            "continuousCoverageEnd": contiguous_end.isoformat() if contiguous_end else None,
            "lastMaterializedAt": _as_iso(last_materialized),
            "lastSourceWatermark": _as_iso(last_source_watermark),
            "gapCount": gap_count,
            "missingDates": missing_dates,
            "degradedDates": sorted(set(degraded_dates)),
            "failedWidgets": sorted(failed_widgets),
        }

    def get_range_readiness(
        self,
        *,
        from_date: date,
        to_date: date,
        fact_families: list[str] | tuple[str, ...] | None = None,
        min_fact_version: int = 1,
    ) -> dict[str, Any]:
        family_summaries: dict[str, Any] = {}
        missing_families: list[str] = []
        missing_dates: set[str] = set()
        degraded_families: list[str] = []
        degraded_dates: set[str] = set()
        for fact_family in (fact_families or FULL_DASHBOARD_REQUIRED_FACT_FAMILIES):
            rows = self._coverage_rows_for_family(
                fact_family=fact_family,
                from_date=from_date,
                to_date=to_date,
                min_fact_version=min_fact_version,
            )
            summary = self._coverage_summary_from_rows(
                rows,
                requested_start=from_date,
                requested_end=to_date,
                min_fact_version=min_fact_version,
            )
            family_summaries[fact_family] = summary
            if summary["missingDates"]:
                missing_families.append(fact_family)
                missing_dates.update(summary["missingDates"])
            if summary["degradedDates"]:
                degraded_families.append(fact_family)
                degraded_dates.update(summary["degradedDates"])
        available_starts = [summary["coverageStart"] for summary in family_summaries.values() if summary.get("coverageStart")]
        available_ends = [summary["coverageEnd"] for summary in family_summaries.values() if summary.get("coverageEnd")]
        return {
            "availabilityStart": min(available_starts) if available_starts else None,
            "availabilityEnd": max(available_ends) if available_ends else None,
            "missingFactFamilies": sorted(set(missing_families)),
            "missingDates": sorted(missing_dates),
            "degradedFactFamilies": sorted(set(degraded_families)),
            "degradedDates": sorted(degraded_dates),
            "factFamilies": family_summaries,
            "ready": not missing_families and not missing_dates and not degraded_families and not degraded_dates,
            "minRequiredFactVersion": int(min_fact_version),
        }

    def summarize_v2_route_readiness(
        self,
        *,
        min_fact_version: int = 1,
        lookback_days: int = 400,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        resolved_end = end_date or (_utc_now().date() - timedelta(days=1))
        resolved_lookback = max(1, int(lookback_days))
        requested_start = resolved_end - timedelta(days=resolved_lookback - 1)
        family_summaries: dict[str, Any] = {}
        for fact_family in FACT_FAMILIES:
            rows = self._coverage_rows_for_family(
                fact_family=fact_family,
                from_date=requested_start,
                to_date=resolved_end,
                min_fact_version=min_fact_version,
            )
            family_summaries[fact_family] = self._coverage_summary_from_rows(
                rows,
                requested_start=requested_start,
                requested_end=resolved_end,
                min_fact_version=min_fact_version,
            )
        route_starts = [summary.get("continuousCoverageStart") for summary in family_summaries.values() if summary.get("continuousCoverageStart")]
        route_ends = [summary.get("continuousCoverageEnd") for summary in family_summaries.values() if summary.get("continuousCoverageEnd")]
        route_ready_window_start = max(route_starts) if route_starts else None
        route_ready_window_end = min(route_ends) if route_ends else None
        missing_families = [
            family
            for family, summary in family_summaries.items()
            if summary.get("continuousCoverageStart") is None or summary.get("continuousCoverageEnd") is None
        ]
        degraded_families = [
            family
            for family, summary in family_summaries.items()
            if summary.get("degradedDates")
        ]
        v2_route_ready = False
        if route_ready_window_start and route_ready_window_end:
            start_dt = date.fromisoformat(str(route_ready_window_start))
            end_dt = date.fromisoformat(str(route_ready_window_end))
            v2_route_ready = (end_dt - start_dt).days + 1 >= resolved_lookback and not degraded_families
        return {
            "coverageStart": min([summary["coverageStart"] for summary in family_summaries.values() if summary.get("coverageStart")], default=None),
            "coverageEnd": max([summary["coverageEnd"] for summary in family_summaries.values() if summary.get("coverageEnd")], default=None),
            "routeReadyWindowStart": route_ready_window_start,
            "routeReadyWindowEnd": route_ready_window_end,
            "minRequiredFactVersion": int(min_fact_version),
            "v2RouteReady": bool(v2_route_ready),
            "missingFamilies": sorted(set(missing_families)),
            "degradedFamilies": sorted(set(degraded_families)),
            "factFamilies": family_summaries,
        }

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

    def status_snapshot(
        self,
        *,
        run_limit: int = 20,
        artifact_limit: int = 20,
        min_fact_version: int = 1,
        lookback_days: int = 400,
    ) -> dict[str, Any]:
        try:
            return {
                "factRuns": self.list_recent_fact_runs(limit=run_limit),
                "artifacts": self.summarize_recent_artifacts(limit=artifact_limit),
                "compareRuns": self.list_recent_compare_runs(limit=min(run_limit, 10)),
                "readiness": self.summarize_v2_route_readiness(
                    min_fact_version=min_fact_version,
                    lookback_days=lookback_days,
                ),
            }
        except Exception as exc:
            logger.warning(f"Dashboard V2 status snapshot failed: {exc}")
            raise
