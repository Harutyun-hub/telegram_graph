from __future__ import annotations

import json
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Optional

from loguru import logger


@dataclass
class DefaultProducerBuildContext:
    build_id: str
    cache_key: str
    reason: str
    trigger_request_id: str | None = None
    scheduled_at: float | None = None
    query_records: list[dict[str, Any]] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add_query_record(self, record: dict[str, Any]) -> None:
        with self._lock:
            self.query_records.append(dict(record))

    def query_summary(self) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        with self._lock:
            records = list(self.query_records)

        for record in records:
            family = str(record.get("query_family") or "").strip()
            if not family:
                continue
            bucket = grouped.setdefault(
                family,
                {
                    "tier": record.get("tier"),
                    "query_family": family,
                    "backend": record.get("backend"),
                    "attempts": 0,
                    "total_duration_ms": 0.0,
                    "max_duration_ms": 0.0,
                    "status": "ok",
                    "error_class": None,
                    "error_message": None,
                },
            )
            bucket["attempts"] += 1
            duration_ms = float(record.get("duration_ms") or 0.0)
            bucket["total_duration_ms"] = round(float(bucket["total_duration_ms"]) + duration_ms, 2)
            bucket["max_duration_ms"] = round(max(float(bucket["max_duration_ms"]), duration_ms), 2)
            status = str(record.get("status") or "ok")
            if status != "ok":
                bucket["status"] = status
                bucket["error_class"] = record.get("error_class")
                bucket["error_message"] = record.get("error_message")

        ordered = sorted(
            grouped.values(),
            key=lambda item: (float(item.get("total_duration_ms") or 0.0), str(item.get("query_family") or "")),
            reverse=True,
        )
        return ordered


_thread_state = threading.local()


def new_build_id() -> str:
    return uuid.uuid4().hex[:12]


def emit_dashboard_event(
    event: str,
    *,
    level: str = "info",
    **fields: Any,
) -> None:
    payload = {
        "level": str(level or "info"),
        "message": str(event or "").strip(),
        "event": str(event or "").strip(),
    }
    for key, value in fields.items():
        if value is None:
            continue
        payload[key] = value
    line = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=False)
    log_fn = getattr(logger, level if level in {"debug", "info", "warning", "error"} else "info")
    log_fn(line)


def current_build_context() -> DefaultProducerBuildContext | None:
    build = getattr(_thread_state, "default_dashboard_build", None)
    if isinstance(build, DefaultProducerBuildContext):
        return build
    return None


def current_tier() -> str | None:
    tier = getattr(_thread_state, "default_dashboard_tier", None)
    if isinstance(tier, str) and tier.strip():
        return tier
    return None


def current_query_family() -> str | None:
    query_family = getattr(_thread_state, "default_dashboard_query_family", None)
    if isinstance(query_family, str) and query_family.strip():
        return query_family
    return None


def current_neo4j_log_fields() -> dict[str, Any]:
    build = current_build_context()
    if build is None:
        return {}
    return {
        "build_id": build.build_id,
        "cache_key": build.cache_key,
        "trigger_request_id": build.trigger_request_id,
        "reason": build.reason,
        "tier": current_tier(),
        "query_family": current_query_family(),
    }


@contextmanager
def bind_build_context(build: DefaultProducerBuildContext | None) -> Iterator[None]:
    previous = getattr(_thread_state, "default_dashboard_build", None)
    _thread_state.default_dashboard_build = build
    try:
        yield
    finally:
        _thread_state.default_dashboard_build = previous


@contextmanager
def bind_tier_context(tier: str | None) -> Iterator[None]:
    previous = getattr(_thread_state, "default_dashboard_tier", None)
    _thread_state.default_dashboard_tier = tier
    try:
        yield
    finally:
        _thread_state.default_dashboard_tier = previous


@contextmanager
def bind_query_family_context(query_family: str | None) -> Iterator[None]:
    previous = getattr(_thread_state, "default_dashboard_query_family", None)
    _thread_state.default_dashboard_query_family = query_family
    try:
        yield
    finally:
        _thread_state.default_dashboard_query_family = previous


def observe_query_family(
    query_family: str,
    backend: str,
    fn: Callable[[], Any],
) -> Any:
    build = current_build_context()
    if build is None:
        return fn()

    tier = current_tier() or str(query_family or "").split(".", 1)[0]
    started_at = time.perf_counter()
    status = "ok"
    error_class: str | None = None
    error_message: str | None = None

    with bind_query_family_context(query_family):
        try:
            return fn()
        except TimeoutError as exc:
            status = "timeout"
            error_class = exc.__class__.__name__
            error_message = str(exc)
            raise
        except Exception as exc:
            status = "failed"
            error_class = exc.__class__.__name__
            error_message = str(exc)
            raise
        finally:
            duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
            record = {
                "build_id": build.build_id,
                "trigger_request_id": build.trigger_request_id,
                "cache_key": build.cache_key,
                "reason": build.reason,
                "tier": tier,
                "query_family": query_family,
                "backend": backend,
                "duration_ms": duration_ms,
                "status": status,
                "error_class": error_class,
                "error_message": error_message,
            }
            build.add_query_record(record)
            emit_dashboard_event(
                "dashboard_default_query_family",
                level="warning" if status != "ok" else "info",
                **record,
            )
