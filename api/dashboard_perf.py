from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import time
from typing import Any, Callable


@dataclass
class DashboardQueryRecord:
    label: str
    elapsed_ms: float
    row_count: int | None = None
    success: bool = True
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DashboardProfile:
    label: str
    started_at: float = field(default_factory=time.perf_counter)
    finished_at: float | None = None
    neo4j_queries: list[DashboardQueryRecord] = field(default_factory=list)
    supabase_queries: list[DashboardQueryRecord] = field(default_factory=list)

    @property
    def elapsed_ms(self) -> float:
        finished_at = self.finished_at if self.finished_at is not None else time.perf_counter()
        return round((finished_at - self.started_at) * 1000, 2)


_CURRENT_DASHBOARD_PROFILE: ContextVar[DashboardProfile | None] = ContextVar(
    "current_dashboard_profile",
    default=None,
)


def current_dashboard_profile() -> DashboardProfile | None:
    return _CURRENT_DASHBOARD_PROFILE.get()


@contextmanager
def capture_dashboard_profile(label: str = "dashboard") -> DashboardProfile:
    profile = DashboardProfile(label=label)
    token = _CURRENT_DASHBOARD_PROFILE.set(profile)
    try:
        yield profile
    finally:
        profile.finished_at = time.perf_counter()
        _CURRENT_DASHBOARD_PROFILE.reset(token)


def _record_query(
    target: list[DashboardQueryRecord],
    *,
    label: str,
    elapsed_ms: float,
    row_count: int | None = None,
    success: bool = True,
    error: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    target.append(
        DashboardQueryRecord(
            label=label,
            elapsed_ms=round(float(elapsed_ms), 2),
            row_count=row_count,
            success=success,
            error=error,
            metadata=dict(metadata or {}),
        )
    )


def record_neo4j_query(
    *,
    label: str,
    elapsed_ms: float,
    row_count: int | None = None,
    success: bool = True,
    error: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    profile = current_dashboard_profile()
    if profile is None:
        return
    _record_query(
        profile.neo4j_queries,
        label=label,
        elapsed_ms=elapsed_ms,
        row_count=row_count,
        success=success,
        error=error,
        metadata=metadata,
    )


def record_supabase_query(
    *,
    label: str,
    elapsed_ms: float,
    row_count: int | None = None,
    success: bool = True,
    error: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    profile = current_dashboard_profile()
    if profile is None:
        return
    _record_query(
        profile.supabase_queries,
        label=label,
        elapsed_ms=elapsed_ms,
        row_count=row_count,
        success=success,
        error=error,
        metadata=metadata,
    )


def _row_count_for_response(response: Any) -> int | None:
    data = getattr(response, "data", None)
    if isinstance(data, list):
        return len(data)
    if data is None:
        return 0
    return 1


def execute_supabase_query(query: Any, *, label: str) -> Any:
    started_at = time.perf_counter()
    response = None
    error: str | None = None
    success = False
    try:
        response = query.execute()
        success = True
        return response
    except Exception as exc:
        error = str(exc)
        raise
    finally:
        record_supabase_query(
            label=label,
            elapsed_ms=(time.perf_counter() - started_at) * 1000,
            row_count=_row_count_for_response(response),
            success=success,
            error=error,
        )


def paginate_supabase_query(
    query_factory: Callable[[int, int], Any],
    *,
    label: str,
    page_size: int,
) -> list[dict]:
    rows: list[dict] = []
    offset = 0
    total_count: int | None = None
    page_index = 0

    while True:
        response = execute_supabase_query(
            query_factory(offset, offset + page_size - 1),
            label=f"{label}[page={page_index}]",
        )
        batch = response.data or []
        if total_count is None:
            raw_count = getattr(response, "count", None)
            total_count = int(raw_count) if raw_count is not None else None
        if not batch:
            break
        rows.extend(batch)
        if total_count is not None and len(rows) >= total_count:
            break
        offset += len(batch)
        page_index += 1

    return rows


def _serialize_records(records: list[DashboardQueryRecord], *, top_n: int) -> dict[str, Any]:
    sorted_records = sorted(records, key=lambda record: record.elapsed_ms, reverse=True)
    slowest = [
        {
            "label": record.label,
            "elapsedMs": record.elapsed_ms,
            "rowCount": record.row_count,
            "success": record.success,
            "error": record.error,
            **record.metadata,
        }
        for record in sorted_records[: max(1, int(top_n))]
    ]
    return {
        "queryCount": len(records),
        "totalMs": round(sum(record.elapsed_ms for record in records), 2),
        "failureCount": sum(1 for record in records if not record.success),
        "slowest": slowest,
    }


def summarize_dashboard_profile(
    profile: DashboardProfile | None,
    *,
    top_n: int = 10,
) -> dict[str, Any] | None:
    if profile is None:
        return None
    return {
        "label": profile.label,
        "elapsedMs": profile.elapsed_ms,
        "neo4j": _serialize_records(profile.neo4j_queries, top_n=top_n),
        "supabase": _serialize_records(profile.supabase_queries, top_n=top_n),
    }
