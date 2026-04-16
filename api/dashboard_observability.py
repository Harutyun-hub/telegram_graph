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

    def producer_query_context(
        self,
        *,
        tier: str | None,
        query_family: str | None,
    ) -> ProducerQueryContext | None:
        family = str(query_family or "").strip()
        if not family:
            return None
        tier_name = str(tier or family.split(".", 1)[0] or "").strip()
        return ProducerQueryContext(
            build_id=self.build_id,
            cache_key=self.cache_key,
            trigger_request_id=self.trigger_request_id,
            reason=self.reason,
            tier=tier_name,
            query_family=family,
        )

    def add_query_record(self, record: dict[str, Any]) -> None:
        with self._lock:
            self.query_records.append(dict(record))

    def query_summary(self) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}
        with self._lock:
            records = list(self.query_records)

        if not records:
            return []

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
                    "_first_started_at": None,
                    "_last_ended_at": None,
                    "status": "ok",
                    "error_class": None,
                    "error_message": None,
                },
            )
            bucket["attempts"] += 1
            duration_ms = float(record.get("duration_ms") or 0.0)
            bucket["total_duration_ms"] = round(float(bucket["total_duration_ms"]) + duration_ms, 2)
            bucket["max_duration_ms"] = round(max(float(bucket["max_duration_ms"]), duration_ms), 2)
            started_at = float(record.get("started_at") or 0.0)
            ended_at = float(record.get("ended_at") or 0.0)
            previous_started = bucket.get("_first_started_at")
            previous_ended = bucket.get("_last_ended_at")
            if previous_started is None or (started_at and started_at < float(previous_started)):
                bucket["_first_started_at"] = started_at
            if previous_ended is None or ended_at > float(previous_ended):
                bucket["_last_ended_at"] = ended_at
            status = str(record.get("status") or "ok")
            if status != "ok":
                bucket["status"] = status
                bucket["error_class"] = record.get("error_class")
                bucket["error_message"] = record.get("error_message")

        first_started_values = [
            float(item.get("_first_started_at") or 0.0)
            for item in grouped.values()
            if float(item.get("_first_started_at") or 0.0) > 0.0
        ]
        baseline_started_at = min(first_started_values, default=0.0)

        ordered_items: list[dict[str, Any]] = []
        for bucket in grouped.values():
            first_started_at = float(bucket.pop("_first_started_at") or 0.0)
            last_ended_at = float(bucket.pop("_last_ended_at") or 0.0)
            first_start_ms = 0.0
            last_end_ms = 0.0
            wall_clock_span_ms = 0.0
            if baseline_started_at > 0.0 and first_started_at > 0.0:
                first_start_ms = round((first_started_at - baseline_started_at) * 1000, 2)
            if baseline_started_at > 0.0 and last_ended_at > 0.0:
                last_end_ms = round((last_ended_at - baseline_started_at) * 1000, 2)
            if first_started_at > 0.0 and last_ended_at >= first_started_at:
                wall_clock_span_ms = round((last_ended_at - first_started_at) * 1000, 2)
            bucket["first_start_ms"] = first_start_ms
            bucket["last_end_ms"] = last_end_ms
            bucket["wall_clock_span_ms"] = wall_clock_span_ms
            ordered_items.append(bucket)

        ordered = sorted(
            ordered_items,
            key=lambda item: (
                float(item.get("wall_clock_span_ms") or 0.0),
                float(item.get("total_duration_ms") or 0.0),
                str(item.get("query_family") or ""),
            ),
            reverse=True,
        )
        return ordered


@dataclass(frozen=True)
class ProducerQueryContext:
    build_id: str
    cache_key: str
    trigger_request_id: str | None
    reason: str
    tier: str
    query_family: str


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


def producer_query_context(
    *,
    build_context: DefaultProducerBuildContext | None = None,
    tier: str | None,
    query_family: str | None,
) -> ProducerQueryContext | None:
    build = build_context or current_build_context()
    if build is None:
        return None
    return build.producer_query_context(tier=tier, query_family=query_family)


def current_neo4j_log_fields(
    producer_context: ProducerQueryContext | None = None,
) -> dict[str, Any]:
    if producer_context is not None:
        return {
            "build_id": producer_context.build_id,
            "cache_key": producer_context.cache_key,
            "trigger_request_id": producer_context.trigger_request_id,
            "reason": producer_context.reason,
            "tier": producer_context.tier,
            "query_family": producer_context.query_family,
        }

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
    *,
    build_context: DefaultProducerBuildContext | None = None,
    tier: str | None = None,
) -> Any:
    build = build_context or current_build_context()
    if build is None:
        return fn()

    producer_context = build.producer_query_context(
        tier=tier or current_tier(),
        query_family=query_family,
    )
    tier_name = producer_context.tier if producer_context is not None else (tier or current_tier() or str(query_family or "").split(".", 1)[0])
    started_at = time.perf_counter()
    status = "ok"
    error_class: str | None = None
    error_message: str | None = None

    with bind_build_context(build), bind_tier_context(tier_name), bind_query_family_context(query_family):
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
                "tier": tier_name,
                "query_family": query_family,
                "backend": backend,
                "started_at": started_at,
                "ended_at": time.perf_counter(),
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
