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


MATERIALIZE_JOB_ACTIVE_STATUSES = ("queued", "running", "paused")
MATERIALIZE_JOB_RESUMABLE_STATUSES = ("queued", "running", "paused", "failed")
MATERIALIZE_SLICE_ACTIVE_STATUSES = ("pending", "running")
MATERIALIZE_JOB_HEAVY_MODES = ("backfill", "reconciliation")


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


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except Exception:
        return None


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

    def _materialize_job_summary_from_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        return {
            "jobId": str(row.get("job_id") or ""),
            "mode": str(row.get("mode") or ""),
            "requestedStart": row.get("requested_start").isoformat() if isinstance(row.get("requested_start"), date) else str(row.get("requested_start") or ""),
            "requestedEnd": row.get("requested_end").isoformat() if isinstance(row.get("requested_end"), date) else str(row.get("requested_end") or ""),
            "factVersion": int(row.get("fact_version") or 0),
            "status": str(row.get("status") or ""),
            "requestedByRole": row.get("requested_by_role"),
            "requestedByActor": row.get("requested_by_actor"),
            "jobOwner": row.get("job_owner"),
            "createdAt": _as_iso(_parse_dt(row.get("created_at"))),
            "startedAt": _as_iso(_parse_dt(row.get("started_at"))),
            "finishedAt": _as_iso(_parse_dt(row.get("finished_at"))),
            "updatedAt": _as_iso(_parse_dt(row.get("updated_at"))),
            "activeWorkerId": row.get("active_worker_id"),
            "lastHeartbeatAt": _as_iso(_parse_dt(row.get("last_heartbeat_at"))),
            "lastError": row.get("last_error"),
            "totalSlices": int(row.get("total_slices") or 0),
            "completedSlices": int(row.get("completed_slices") or 0),
            "failedSlices": int(row.get("failed_slices") or 0),
            "totalDays": int(row.get("total_days") or 0),
            "completedDays": int(row.get("completed_days") or 0),
        }

    def _materialize_slice_summary_from_row(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        return {
            "sliceId": str(row.get("slice_id") or ""),
            "jobId": str(row.get("job_id") or ""),
            "factFamily": str(row.get("fact_family") or ""),
            "sliceOrder": int(row.get("slice_order") or 0),
            "sliceStart": row.get("slice_start").isoformat() if isinstance(row.get("slice_start"), date) else str(row.get("slice_start") or ""),
            "sliceEnd": row.get("slice_end").isoformat() if isinstance(row.get("slice_end"), date) else str(row.get("slice_end") or ""),
            "status": str(row.get("status") or ""),
            "attemptCount": int(row.get("attempt_count") or 0),
            "leaseOwner": row.get("lease_owner"),
            "leaseExpiresAt": _as_iso(_parse_dt(row.get("lease_expires_at"))),
            "startedAt": _as_iso(_parse_dt(row.get("started_at"))),
            "finishedAt": _as_iso(_parse_dt(row.get("finished_at"))),
            "rowsInserted": int(row.get("rows_inserted") or 0),
            "daysProcessed": int(row.get("days_processed") or 0),
            "degradedDays": _normalize_str_list(row.get("degraded_days")),
            "failedWidgets": _normalize_str_list(row.get("failed_widgets")),
            "factRunId": str(row.get("fact_run_id") or "") or None,
            "lastError": row.get("last_error"),
        }

    def _refresh_materialize_job_progress_cur(self, cur, *, job_id: str) -> dict[str, Any]:
        cur.execute(
            """
            SELECT
              COUNT(*)::int AS total_slices,
              COUNT(*) FILTER (WHERE status = 'completed')::int AS completed_slices,
              COUNT(*) FILTER (WHERE status = 'failed')::int AS failed_slices,
              COUNT(*) FILTER (WHERE status = 'running')::int AS running_slices,
              COUNT(*) FILTER (WHERE status = 'pending')::int AS pending_slices,
              COALESCE(SUM((slice_end - slice_start) + 1), 0)::int AS total_days,
              COALESCE(SUM(CASE WHEN status = 'completed' THEN days_processed ELSE 0 END), 0)::int AS completed_days
            FROM public.dashboard_materialize_slices_v2
            WHERE job_id = %s::uuid
            """,
            (job_id,),
        )
        aggregate = cur.fetchone() or {}
        running_slices = int(aggregate.get("running_slices") or 0)
        pending_slices = int(aggregate.get("pending_slices") or 0)
        failed_slices = int(aggregate.get("failed_slices") or 0)
        if running_slices > 0:
            status = "running"
        elif pending_slices > 0:
            status = "queued"
        elif failed_slices > 0:
            status = "failed"
        else:
            status = "completed"
        finished_at = _utc_now() if status in {"completed", "failed", "cancelled"} else None
        cur.execute(
            """
            UPDATE public.dashboard_materialize_jobs_v2
            SET status = %s,
                updated_at = timezone('utc', now()),
                finished_at = COALESCE(%s, finished_at),
                total_slices = %s,
                completed_slices = %s,
                failed_slices = %s,
                total_days = %s,
                completed_days = %s
            WHERE job_id = %s::uuid
            RETURNING *
            """,
            (
                status,
                finished_at,
                int(aggregate.get("total_slices") or 0),
                int(aggregate.get("completed_slices") or 0),
                failed_slices,
                int(aggregate.get("total_days") or 0),
                int(aggregate.get("completed_days") or 0),
                job_id,
            ),
        )
        return self._materialize_job_summary_from_row(cur.fetchone()) or {}

    def _fetch_materialize_job_cur(
        self,
        cur,
        *,
        job_id: str,
        include_slices: bool = False,
        slice_limit: int = 200,
    ) -> dict[str, Any] | None:
        cur.execute(
            """
            SELECT *
            FROM public.dashboard_materialize_jobs_v2
            WHERE job_id = %s::uuid
            LIMIT 1
            """,
            (job_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        summary = self._materialize_job_summary_from_row(row) or {}
        cur.execute(
            """
            SELECT *
            FROM public.dashboard_materialize_slices_v2
            WHERE job_id = %s::uuid
              AND status = 'running'
            ORDER BY slice_order ASC
            LIMIT 1
            """,
            (job_id,),
        )
        current_slice = self._materialize_slice_summary_from_row(cur.fetchone())
        summary["currentSlice"] = current_slice
        if include_slices:
            cur.execute(
                """
                SELECT *
                FROM public.dashboard_materialize_slices_v2
                WHERE job_id = %s::uuid
                ORDER BY slice_order ASC
                LIMIT %s
                """,
                (job_id, max(1, int(slice_limit))),
            )
            summary["slices"] = [
                self._materialize_slice_summary_from_row(item)
                for item in (cur.fetchall() or [])
            ]
        return summary

    def enqueue_materialize_job(
        self,
        *,
        mode: str,
        requested_start: date,
        requested_end: date,
        fact_version: int,
        requested_by_role: str | None = None,
        requested_by_actor: str | None = None,
        job_owner: str = "worker",
        slices: list[dict[str, Any]],
    ) -> dict[str, Any]:
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in MATERIALIZE_JOB_HEAVY_MODES:
            raise ValueError(f"Unsupported Dashboard V2 materialize mode: {mode}")
        with self.writer._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM public.dashboard_materialize_jobs_v2
                WHERE mode = %s
                  AND requested_start = %s
                  AND requested_end = %s
                  AND fact_version = %s
                  AND status = ANY(%s)
                ORDER BY created_at DESC
                LIMIT 1
                FOR UPDATE
                """,
                (
                    normalized_mode,
                    requested_start,
                    requested_end,
                    int(fact_version),
                    list(MATERIALIZE_JOB_RESUMABLE_STATUSES),
                ),
            )
            existing = cur.fetchone()
            if existing:
                job_id = str(existing["job_id"])
                if str(existing.get("status") or "").strip().lower() in {"failed", "paused"}:
                    cur.execute(
                        """
                        UPDATE public.dashboard_materialize_slices_v2
                        SET status = 'pending',
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            updated_at = timezone('utc', now()),
                            last_error = NULL
                        WHERE job_id = %s::uuid
                          AND (
                            status = 'failed'
                            OR (status = 'running' AND lease_expires_at < timezone('utc', now()))
                          )
                        """,
                        (job_id,),
                    )
                    cur.execute(
                        """
                        UPDATE public.dashboard_materialize_jobs_v2
                        SET status = 'queued',
                            updated_at = timezone('utc', now()),
                            finished_at = NULL,
                            last_error = NULL,
                            active_worker_id = NULL
                        WHERE job_id = %s::uuid
                        """,
                        (job_id,),
                    )
                    self._refresh_materialize_job_progress_cur(cur, job_id=job_id)
                return self._fetch_materialize_job_cur(cur, job_id=job_id) or {}

            job_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO public.dashboard_materialize_jobs_v2 (
                  job_id,
                  mode,
                  requested_start,
                  requested_end,
                  fact_version,
                  status,
                  requested_by_role,
                  requested_by_actor,
                  job_owner,
                  total_slices,
                  total_days
                )
                VALUES (%s::uuid, %s, %s, %s, %s, 'queued', %s, %s, %s, %s, %s)
                """,
                (
                    job_id,
                    normalized_mode,
                    requested_start,
                    requested_end,
                    int(fact_version),
                    requested_by_role,
                    requested_by_actor,
                    job_owner,
                    len(slices),
                    sum(((item["slice_end"] - item["slice_start"]).days + 1) for item in slices),
                ),
            )
            if slices:
                cur.executemany(
                    """
                    INSERT INTO public.dashboard_materialize_slices_v2 (
                      slice_id,
                      job_id,
                      fact_family,
                      slice_order,
                      slice_start,
                      slice_end,
                      status
                    )
                    VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, 'pending')
                    """,
                    [
                        (
                            str(uuid.uuid4()),
                            job_id,
                            str(item["fact_family"]),
                            int(item["slice_order"]),
                            item["slice_start"],
                            item["slice_end"],
                        )
                        for item in slices
                    ],
                )
            self._refresh_materialize_job_progress_cur(cur, job_id=job_id)
            return self._fetch_materialize_job_cur(cur, job_id=job_id) or {}

    def list_recent_materialize_jobs(self, *, limit: int = 10) -> list[dict[str, Any]]:
        rows = self.writer.pg_fetchall(
            """
            SELECT *
            FROM public.dashboard_materialize_jobs_v2
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (max(1, int(limit)),),
        )
        return [self._materialize_job_summary_from_row(row) for row in rows]

    def get_materialize_job(self, job_id: str, *, include_slices: bool = False, slice_limit: int = 200) -> dict[str, Any] | None:
        with self.writer._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
            return self._fetch_materialize_job_cur(cur, job_id=job_id, include_slices=include_slices, slice_limit=slice_limit)

    def get_active_materialize_job(self) -> dict[str, Any] | None:
        row = self.writer.pg_fetchone(
            """
            SELECT *
            FROM public.dashboard_materialize_jobs_v2
            WHERE status = ANY(%s)
            ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, created_at ASC
            LIMIT 1
            """,
            (list(MATERIALIZE_JOB_ACTIVE_STATUSES),),
        )
        if not row:
            return None
        return self.get_materialize_job(str(row["job_id"]), include_slices=False)

    def has_active_materialize_job(self) -> bool:
        return self.get_active_materialize_job() is not None

    def claim_next_materialize_slice(self, *, worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
        with self.writer._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM public.dashboard_materialize_jobs_v2
                WHERE status = ANY(%s)
                  AND job_owner = 'worker'
                ORDER BY CASE status WHEN 'running' THEN 0 ELSE 1 END, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
                (list(("running", "queued")),),
            )
            job_row = cur.fetchone()
            if not job_row:
                return None
            job_id = str(job_row["job_id"])
            cur.execute(
                """
                SELECT *
                FROM public.dashboard_materialize_slices_v2
                WHERE job_id = %s::uuid
                  AND (
                    status = 'pending'
                    OR (status = 'running' AND lease_expires_at < timezone('utc', now()))
                  )
                ORDER BY slice_order ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """,
                (job_id,),
            )
            slice_row = cur.fetchone()
            if not slice_row:
                self._refresh_materialize_job_progress_cur(cur, job_id=job_id)
                return None
            cur.execute(
                """
                UPDATE public.dashboard_materialize_slices_v2
                SET status = 'running',
                    attempt_count = attempt_count + 1,
                    lease_owner = %s,
                    lease_expires_at = timezone('utc', now()) + (%s * interval '1 second'),
                    started_at = COALESCE(started_at, timezone('utc', now())),
                    updated_at = timezone('utc', now()),
                    last_error = NULL
                WHERE slice_id = %s::uuid
                RETURNING *
                """,
                (
                    worker_id,
                    max(30, int(lease_seconds)),
                    str(slice_row["slice_id"]),
                ),
            )
            claimed_slice = cur.fetchone()
            cur.execute(
                """
                UPDATE public.dashboard_materialize_jobs_v2
                SET status = 'running',
                    active_worker_id = %s,
                    started_at = COALESCE(started_at, timezone('utc', now())),
                    last_heartbeat_at = timezone('utc', now()),
                    updated_at = timezone('utc', now())
                WHERE job_id = %s::uuid
                """,
                (worker_id, job_id),
            )
            return {
                "job": self._fetch_materialize_job_cur(cur, job_id=job_id, include_slices=False),
                "slice": self._materialize_slice_summary_from_row(claimed_slice),
            }

    def heartbeat_materialize_slice(
        self,
        *,
        job_id: str,
        slice_id: str,
        worker_id: str,
        lease_seconds: int,
    ) -> None:
        self.writer.pg_execute(
            """
            UPDATE public.dashboard_materialize_slices_v2
            SET lease_owner = %s,
                lease_expires_at = timezone('utc', now()) + (%s * interval '1 second'),
                updated_at = timezone('utc', now())
            WHERE slice_id = %s::uuid
              AND job_id = %s::uuid
            """,
            (worker_id, max(30, int(lease_seconds)), slice_id, job_id),
        )
        self.writer.pg_execute(
            """
            UPDATE public.dashboard_materialize_jobs_v2
            SET active_worker_id = %s,
                last_heartbeat_at = timezone('utc', now()),
                updated_at = timezone('utc', now())
            WHERE job_id = %s::uuid
            """,
            (worker_id, job_id),
        )

    def complete_materialize_slice(
        self,
        *,
        job_id: str,
        slice_id: str,
        worker_id: str,
        rows_inserted: int,
        days_processed: int,
        degraded_days: list[str] | tuple[str, ...] | None,
        failed_widgets: list[str] | tuple[str, ...] | None,
        fact_run_id: str | None,
    ) -> dict[str, Any]:
        with self.writer._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.dashboard_materialize_slices_v2
                SET status = 'completed',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    finished_at = timezone('utc', now()),
                    updated_at = timezone('utc', now()),
                    rows_inserted = %s,
                    days_processed = %s,
                    degraded_days = %s::text[],
                    failed_widgets = %s::text[],
                    fact_run_id = %s::uuid,
                    last_error = NULL
                WHERE slice_id = %s::uuid
                  AND job_id = %s::uuid
                """,
                (
                    int(rows_inserted),
                    int(days_processed),
                    _normalize_str_list(degraded_days),
                    _normalize_str_list(failed_widgets),
                    fact_run_id,
                    slice_id,
                    job_id,
                ),
            )
            cur.execute(
                """
                UPDATE public.dashboard_materialize_jobs_v2
                SET active_worker_id = %s,
                    last_heartbeat_at = timezone('utc', now()),
                    updated_at = timezone('utc', now()),
                    last_error = NULL
                WHERE job_id = %s::uuid
                """,
                (worker_id, job_id),
            )
            return self._refresh_materialize_job_progress_cur(cur, job_id=job_id)

    def fail_materialize_slice(
        self,
        *,
        job_id: str,
        slice_id: str,
        worker_id: str,
        error: str,
        rows_inserted: int = 0,
        days_processed: int = 0,
        degraded_days: list[str] | tuple[str, ...] | None = None,
        failed_widgets: list[str] | tuple[str, ...] | None = None,
        fact_run_id: str | None = None,
    ) -> dict[str, Any]:
        with self.writer._pipeline_connection() as conn, conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                UPDATE public.dashboard_materialize_slices_v2
                SET status = 'failed',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    finished_at = timezone('utc', now()),
                    updated_at = timezone('utc', now()),
                    rows_inserted = %s,
                    days_processed = %s,
                    degraded_days = %s::text[],
                    failed_widgets = %s::text[],
                    fact_run_id = %s::uuid,
                    last_error = %s
                WHERE slice_id = %s::uuid
                  AND job_id = %s::uuid
                """,
                (
                    int(rows_inserted),
                    int(days_processed),
                    _normalize_str_list(degraded_days),
                    _normalize_str_list(failed_widgets),
                    fact_run_id,
                    error,
                    slice_id,
                    job_id,
                ),
            )
            cur.execute(
                """
                UPDATE public.dashboard_materialize_jobs_v2
                SET active_worker_id = %s,
                    last_heartbeat_at = timezone('utc', now()),
                    updated_at = timezone('utc', now()),
                    last_error = %s
                WHERE job_id = %s::uuid
                """,
                (worker_id, error, job_id),
            )
            return self._refresh_materialize_job_progress_cur(cur, job_id=job_id)

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
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, Any]:
        if from_date is not None or to_date is not None:
            if from_date is None or to_date is None:
                raise ValueError("Both from_date and to_date are required for exact Dashboard V2 readiness checks.")
            exact_readiness = self.get_range_readiness(
                from_date=from_date,
                to_date=to_date,
                fact_families=FULL_DASHBOARD_REQUIRED_FACT_FAMILIES,
                min_fact_version=min_fact_version,
            )
            return {
                "coverageStart": exact_readiness["availabilityStart"],
                "coverageEnd": exact_readiness["availabilityEnd"],
                "routeReadyWindowStart": from_date.isoformat() if exact_readiness["ready"] else None,
                "routeReadyWindowEnd": to_date.isoformat() if exact_readiness["ready"] else None,
                "requestedFrom": from_date.isoformat(),
                "requestedTo": to_date.isoformat(),
                "minRequiredFactVersion": int(min_fact_version),
                "v2RouteReady": bool(exact_readiness["ready"]),
                "missingFamilies": exact_readiness["missingFactFamilies"],
                "missingDates": exact_readiness["missingDates"],
                "degradedFamilies": exact_readiness["degradedFactFamilies"],
                "degradedDates": exact_readiness["degradedDates"],
                "factFamilies": exact_readiness["factFamilies"],
            }
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
        job_limit: int = 10,
        min_fact_version: int = 1,
        lookback_days: int = 400,
        from_date: date | None = None,
        to_date: date | None = None,
    ) -> dict[str, Any]:
        try:
            return {
                "factRuns": self.list_recent_fact_runs(limit=run_limit),
                "artifacts": self.summarize_recent_artifacts(limit=artifact_limit),
                "compareRuns": self.list_recent_compare_runs(limit=min(run_limit, 10)),
                "materializeJobs": self.list_recent_materialize_jobs(limit=job_limit),
                "activeJob": self.get_active_materialize_job(),
                "readiness": self.summarize_v2_route_readiness(
                    min_fact_version=min_fact_version,
                    lookback_days=lookback_days,
                    from_date=from_date,
                    to_date=to_date,
                ),
            }
        except Exception as exc:
            logger.warning(f"Dashboard V2 status snapshot failed: {exc}")
            raise
