"""
server.py — FastAPI application serving the dashboard API.

Endpoints:
  GET  /api/dashboard     → Full AppData JSON (cached 5 min)
  GET  /api/topics        → Topics detail page (paginated)
  GET  /api/channels      → Channels detail page
  GET  /api/audience      → Audience detail page (paginated)
  POST /api/cache/clear   → Invalidate cache
  GET  /api/health        → Health check

Run:
  cd /Users/harutnahapetyan/Documents/Gemini/Telegram
  python -m uvicorn api.server:app --reload --port 8000
"""
from __future__ import annotations
import sys, os
import gzip
import json
import hashlib
import asyncio
import hmac
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
config.validate()

from fastapi import FastAPI, Query, HTTPException, Depends, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from loguru import logger

try:
    import orjson
except ImportError:  # pragma: no cover - optional dependency in local dev
    orjson = None

try:
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration
except ImportError:  # pragma: no cover - optional dependency in local dev
    sentry_sdk = None
    FastApiIntegration = None

if orjson is not None:
    from fastapi.responses import ORJSONResponse
else:  # pragma: no cover - exercised when orjson isn't installed locally
    ORJSONResponse = None

from api.aggregator import (
    CACHE_TTL_SECONDS as DASHBOARD_CACHE_TTL_SECONDS,
    MAX_STALE_SECONDS as DASHBOARD_MAX_STALE_SECONDS,
    CRITICAL_TIERS as DASHBOARD_CRITICAL_TIERS,
    DetailRefreshUnavailableError,
    build_dashboard_snapshot_once,
    get_dashboard_data, get_dashboard_snapshot, get_topics_page, get_channels_page,
    get_audience_page, get_topic_detail, get_channel_detail, get_audience_detail,
    get_topic_evidence_page, get_channel_posts_page, get_audience_messages_page,
    invalidate_cache, peek_dashboard_snapshot, prime_dashboard_snapshot
)
from api.dashboard_dates import build_dashboard_date_context
from api.queries import graph_dashboard, pulse
from api.freshness import (
    clear_cached_freshness_snapshot,
    freshness_cache_ttl_seconds,
    get_cached_freshness_snapshot,
    get_latest_cached_freshness_snapshot,
    get_freshness_snapshot,
    freshness_max_stale_seconds,
    prime_freshness_snapshot,
)
from api import insights
from api import behavioral_briefs
from api import opportunity_briefs
from api import question_briefs
from api import recommendation_briefs
from api import topic_overviews
from api.admin_runtime import (
    get_admin_config_runtime_warning,
    load_admin_config_raw,
    save_admin_config_raw,
)
from api import db
from api.runtime_executors import (
    background_executor_workers,
    is_draining as runtime_is_draining,
    log_executor_configuration,
    mark_draining as mark_runtime_draining,
    request_executor_workers,
    run_background,
    run_request,
    submit_background,
    shutdown_background_executor,
    shutdown_request_executor,
)
from buffer.supabase_writer import SupabaseWriter
from api.scraper_scheduler import ScraperSchedulerService
from processor import intent_extractor
from scraper.channel_metadata import get_full_channel_metadata
from utils.taxonomy import TAXONOMY_DOMAINS

# ── App setup ────────────────────────────────────────────────────────────────

def _normalize_app_role(value: str | None) -> str:
    role = str(value or "").strip().lower()
    if role in {"web", "worker", "all"}:
        return role
    # Preserve the historical single-service deployment shape by default.
    return "all"


def _should_run_background_jobs(role: str | None = None) -> bool:
    return _normalize_app_role(APP_ROLE if role is None else role) in {"worker", "all"}


APP_ROLE = _normalize_app_role(os.getenv("APP_ROLE"))
RUN_STARTUP_WARMERS = str(os.getenv("RUN_STARTUP_WARMERS", "true")).strip().lower() in {"1", "true", "yes", "on"}
SENTRY_DSN = str(os.getenv("SENTRY_DSN", "")).strip()
SENTRY_TRACES_SAMPLE_RATE = max(0.0, min(float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.05")), 1.0))
_DASHBOARD_RESPONSE_DROP_KEYS = {"trendData", "voiceData", "integrationLevels", "housingHotTopics"}
_DEFAULT_ALIAS_FALLBACK_LOOKBACK_DAYS = max(1, int(os.getenv("DASH_DEFAULT_ALIAS_FALLBACK_DAYS", "3")))
_DEFAULT_TOPICS_PREWARM_SIZE = max(50, min(int(os.getenv("TOPICS_PREWARM_PAGE_SIZE", "100")), 500))
_SERVER_TIMING_PATHS = {
    "/api/dashboard",
    "/api/topics",
    "/api/topics/detail",
    "/api/topics/evidence",
}
_DASHBOARD_PERSISTED_SCHEMA_VERSION = 1
_DASHBOARD_PERSISTED_PREFIX = "dashboard-cache/v1"
_DASHBOARD_DEFAULT_ALIAS_PATH = f"{_DASHBOARD_PERSISTED_PREFIX}/default-latest.json.gz"
_FRESHNESS_PERSISTED_SCHEMA_VERSION = 1
_FRESHNESS_PERSISTED_PATH = "freshness-cache/v1/latest.json.gz"
_SUPABASE_CACHE_READ_TIMEOUT_SECONDS = max(1.0, float(os.getenv("SUPABASE_CACHE_READ_TIMEOUT_SECONDS", "5")))
_SUPABASE_CACHE_WRITE_TIMEOUT_SECONDS = max(1.0, float(os.getenv("SUPABASE_CACHE_WRITE_TIMEOUT_SECONDS", "5")))
_DEFAULT_DASHBOARD_LOOKBACK_DAYS = 14
_HISTORICAL_FASTPATH_ENABLED = str(
    os.getenv("DASH_HISTORICAL_FASTPATH_ENABLED", "true")
).strip().lower() in {"1", "true", "yes", "on"}
_HISTORICAL_FASTPATH_SKIP_TIERS = {
    item.strip().lower()
    for item in os.getenv("DASH_HISTORICAL_FASTPATH_SKIP_TIERS", "network,comparative,predictive").split(",")
    if item.strip()
}
_orjson_dashboard_enabled = ORJSONResponse is not None
_orjson_dashboard_verified = ORJSONResponse is None
_dashboard_refresh_control_lock = threading.Lock()
_dashboard_refresh_inflight: set[str] = set()
_freshness_refresh_control_lock = threading.Lock()
_freshness_refresh_inflight = False


def _init_sentry() -> None:
    if not SENTRY_DSN:
        logger.info("Sentry disabled | SENTRY_DSN missing")
        return
    if sentry_sdk is None or FastApiIntegration is None:
        logger.warning("Sentry requested but sentry-sdk is not installed")
        return
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        environment=os.getenv("SENTRY_ENVIRONMENT", os.getenv("RAILWAY_ENVIRONMENT", "production")),
        traces_sample_rate=SENTRY_TRACES_SAMPLE_RATE,
        send_default_pii=False,
        integrations=[FastApiIntegration(transaction_style="endpoint")],
    )
    logger.info(f"Sentry enabled | traces_sample_rate={SENTRY_TRACES_SAMPLE_RATE}")


def _should_run_scraper_scheduler() -> bool:
    return _should_run_background_jobs() and bool(config.ENABLE_SCRAPER_SCHEDULER)


def _should_run_any_card_materializers() -> bool:
    return _should_run_background_jobs() and bool(config.ENABLE_CARD_MATERIALIZERS)


def _should_run_question_card_materializer() -> bool:
    return _should_run_any_card_materializers() and bool(config.ENABLE_QUESTION_CARD_MATERIALIZER)


def _should_run_behavioral_card_materializer() -> bool:
    return _should_run_any_card_materializers() and bool(config.ENABLE_BEHAVIORAL_CARD_MATERIALIZER)


def _should_run_opportunity_card_materializer() -> bool:
    return _should_run_any_card_materializers() and bool(config.ENABLE_OPPORTUNITY_CARD_MATERIALIZER)


def _should_run_topic_overviews_materializer() -> bool:
    return _should_run_background_jobs() and bool(config.FEATURE_TOPIC_OVERVIEWS_AI)


def _trim_dashboard_payload(snapshot: dict) -> dict:
    return {key: value for key, value in snapshot.items() if key not in _DASHBOARD_RESPONSE_DROP_KEYS}


def _dashboard_response(payload: dict) -> JSONResponse:
    global _orjson_dashboard_enabled, _orjson_dashboard_verified
    if _orjson_dashboard_enabled and ORJSONResponse is not None and orjson is not None:
        if not _orjson_dashboard_verified:
            try:
                orjson.dumps(payload)
            except Exception as exc:
                _orjson_dashboard_enabled = False
                logger.warning(f"ORJSON dashboard response disabled due to serialization mismatch: {exc}")
                return JSONResponse(content=payload)
            _orjson_dashboard_verified = True
        return ORJSONResponse(content=payload)
    return JSONResponse(content=payload)


def _json_dumps_bytes(payload: dict) -> bytes:
    if orjson is not None:
        return orjson.dumps(payload)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def _json_loads_bytes(raw: bytes) -> dict:
    if orjson is not None:
        return orjson.loads(raw)
    return json.loads(raw.decode("utf-8"))


def _dashboard_snapshot_storage_path(cache_key: str) -> str:
    return f"{_DASHBOARD_PERSISTED_PREFIX}/{quote(str(cache_key or '').strip(), safe='')}.json.gz"


def _dashboard_context_from_trusted_end(trusted_end: date) -> Any:
    from_date = (trusted_end - timedelta(days=_DEFAULT_DASHBOARD_LOOKBACK_DAYS)).isoformat()
    return build_dashboard_date_context(from_date, trusted_end.isoformat())


def _serialize_runtime_envelope(envelope: dict) -> bytes:
    return gzip.compress(_json_dumps_bytes(envelope))


def _deserialize_runtime_envelope(raw: bytes) -> dict:
    return _json_loads_bytes(gzip.decompress(raw))


def _load_persisted_envelope(path: str, *, schema_version: int) -> dict[str, Any]:
    blob = get_supabase_writer().read_runtime_blob(path, timeout_seconds=_SUPABASE_CACHE_READ_TIMEOUT_SECONDS)
    status = str(blob.get("status") or "error")
    read_ms = float(blob.get("elapsed_ms") or 0.0)
    if status != "ok":
        mapped = {
            "missing": "miss",
            "timeout": "timeout",
        }.get(status, "error")
        return {"status": mapped, "readMs": read_ms, "envelope": None, "error": blob.get("error") or ""}

    try:
        envelope = _deserialize_runtime_envelope(blob.get("body") or b"")
    except Exception as exc:
        logger.warning("Persisted runtime payload decode failed | path={} error={}", path, exc)
        return {"status": "error", "readMs": read_ms, "envelope": None, "error": str(exc)}

    if not isinstance(envelope, dict):
        return {"status": "error", "readMs": read_ms, "envelope": None, "error": "Persisted payload must be an object"}

    try:
        stored_schema = int(envelope.get("schemaVersion") or 0)
    except Exception:
        stored_schema = 0
    if stored_schema != schema_version:
        return {"status": "schema_mismatch", "readMs": read_ms, "envelope": None, "error": ""}

    return {"status": "hit", "readMs": read_ms, "envelope": envelope, "error": ""}


def _load_persisted_freshness_snapshot() -> dict[str, Any]:
    loaded = _load_persisted_envelope(
        _FRESHNESS_PERSISTED_PATH,
        schema_version=_FRESHNESS_PERSISTED_SCHEMA_VERSION,
    )
    if loaded["status"] != "hit":
        return loaded

    envelope = loaded["envelope"] or {}
    snapshot = envelope.get("data")
    snapshot_built_at = _parse_snapshot_date(envelope.get("snapshotBuiltAt"))
    if not isinstance(snapshot, dict) or snapshot_built_at is None:
        return {
            "status": "error",
            "readMs": loaded["readMs"],
            "envelope": None,
            "error": "Persisted freshness payload is malformed",
        }
    loaded.update(
        {
            "snapshot": snapshot,
            "snapshotBuiltAt": snapshot_built_at,
        }
    )
    return loaded


def _load_persisted_dashboard_snapshot(path: str) -> dict[str, Any]:
    loaded = _load_persisted_envelope(
        path,
        schema_version=_DASHBOARD_PERSISTED_SCHEMA_VERSION,
    )
    if loaded["status"] != "hit":
        return loaded

    envelope = loaded["envelope"] or {}
    snapshot = envelope.get("data")
    meta = envelope.get("meta")
    snapshot_built_at = _parse_snapshot_date(envelope.get("snapshotBuiltAt"))
    trusted_end_date = str(envelope.get("trustedEndDate") or "").strip()
    from_date = str(envelope.get("fromDate") or "").strip()
    to_date = str(envelope.get("toDate") or "").strip()

    if not isinstance(snapshot, dict) or not isinstance(meta, dict) or snapshot_built_at is None:
        return {
            "status": "error",
            "readMs": loaded["readMs"],
            "envelope": None,
            "error": "Persisted dashboard payload is malformed",
        }

    try:
        ctx = build_dashboard_date_context(from_date, to_date)
    except Exception as exc:
        return {
            "status": "error",
            "readMs": loaded["readMs"],
            "envelope": None,
            "error": f"Invalid persisted dashboard context: {exc}",
        }

    loaded.update(
        {
            "snapshot": snapshot,
            "meta": meta,
            "ctx": ctx,
            "snapshotBuiltAt": snapshot_built_at,
            "trustedEndDate": trusted_end_date or ctx.to_date.isoformat(),
        }
    )
    return loaded


def _snapshot_age_seconds(snapshot_built_at: datetime | None) -> float | None:
    if snapshot_built_at is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - snapshot_built_at).total_seconds())


def _is_persisted_snapshot_fresh(snapshot_built_at: datetime | None) -> bool:
    age_seconds = _snapshot_age_seconds(snapshot_built_at)
    return age_seconds is not None and age_seconds < DASHBOARD_CACHE_TTL_SECONDS


def _is_persisted_snapshot_usable(snapshot_built_at: datetime | None) -> bool:
    age_seconds = _snapshot_age_seconds(snapshot_built_at)
    return age_seconds is not None and age_seconds < DASHBOARD_MAX_STALE_SECONDS


def _is_persisted_freshness_usable(snapshot_built_at: datetime | None) -> bool:
    age_seconds = _snapshot_age_seconds(snapshot_built_at)
    return age_seconds is not None and age_seconds < freshness_max_stale_seconds()


def _cached_freshness_resolution(*, allow_live: bool) -> dict[str, Any]:
    cached_snapshot, cached_at = get_cached_freshness_snapshot()
    if cached_snapshot is not None:
        return {
            "snapshot": cached_snapshot,
            "source": "memory",
            "snapshotBuiltAt": cached_at or _parse_snapshot_date(cached_snapshot.get("generated_at")),
            "persistedReadStatus": None,
            "persistedReadMs": None,
        }

    latest_cached_snapshot, latest_cached_at = get_latest_cached_freshness_snapshot()
    if latest_cached_snapshot is not None:
        return {
            "snapshot": latest_cached_snapshot,
            "source": "memory_stale",
            "snapshotBuiltAt": latest_cached_at or _parse_snapshot_date(latest_cached_snapshot.get("generated_at")),
            "persistedReadStatus": None,
            "persistedReadMs": None,
        }

    persisted = _load_persisted_freshness_snapshot()
    if persisted["status"] == "hit":
        snapshot_built_at = persisted.get("snapshotBuiltAt")
        if _snapshot_age_seconds(snapshot_built_at) is not None and _snapshot_age_seconds(snapshot_built_at) < freshness_cache_ttl_seconds():
            snapshot = persisted.get("snapshot") or {}
            prime_freshness_snapshot(snapshot, cached_at=snapshot_built_at)
            return {
                "snapshot": snapshot,
                "source": "persisted",
                "snapshotBuiltAt": snapshot_built_at,
                "persistedReadStatus": persisted["status"],
                "persistedReadMs": persisted["readMs"],
            }
        if _is_persisted_freshness_usable(snapshot_built_at):
            snapshot = persisted.get("snapshot") or {}
            prime_freshness_snapshot(snapshot, cached_at=snapshot_built_at)
            return {
                "snapshot": snapshot,
                "source": "persisted_stale",
                "snapshotBuiltAt": snapshot_built_at,
                "persistedReadStatus": persisted["status"],
                "persistedReadMs": persisted["readMs"],
            }

    if not allow_live:
        return {
            "snapshot": None,
            "source": None,
            "snapshotBuiltAt": None,
            "persistedReadStatus": persisted.get("status"),
            "persistedReadMs": persisted.get("readMs"),
        }

    snapshot = _dashboard_freshness_snapshot(force_refresh=False)
    snapshot_built_at = _parse_snapshot_date(snapshot.get("generated_at")) or datetime.now(timezone.utc)
    prime_freshness_snapshot(snapshot, cached_at=snapshot_built_at)
    _persist_freshness_snapshot_async(snapshot)
    return {
        "snapshot": snapshot,
        "source": "live",
        "snapshotBuiltAt": snapshot_built_at,
        "persistedReadStatus": persisted.get("status"),
        "persistedReadMs": persisted.get("readMs"),
    }


def _persist_freshness_snapshot_sync(snapshot: dict) -> dict[str, Any]:
    envelope = {
        "schemaVersion": _FRESHNESS_PERSISTED_SCHEMA_VERSION,
        "snapshotBuiltAt": snapshot.get("generated_at") or datetime.now(timezone.utc).isoformat(),
        "data": snapshot,
    }
    payload = _serialize_runtime_envelope(envelope)
    started_at = time.perf_counter()
    ok = get_supabase_writer().save_runtime_blob(
        _FRESHNESS_PERSISTED_PATH,
        payload,
        content_type="application/gzip",
    )
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    level = "INFO" if ok else "WARNING"
    logger.log(
        level,
        json.dumps(
            {
                "level": level.lower(),
                "message": "freshness_snapshot_persisted",
                "ok": ok,
                "persistedWriteMs": elapsed_ms,
                "path": _FRESHNESS_PERSISTED_PATH,
            },
            ensure_ascii=True,
            separators=(",", ":"),
        ),
    )
    return {"ok": ok, "persistedWriteMs": elapsed_ms}


def _persist_freshness_snapshot_async(snapshot: dict) -> None:
    thread = threading.Thread(
        target=_persist_freshness_snapshot_sync,
        args=(dict(snapshot),),
        daemon=True,
        name="freshness-persist",
    )
    thread.start()


def _ensure_background_freshness_refresh() -> bool:
    global _freshness_refresh_inflight
    with _freshness_refresh_control_lock:
        if _freshness_refresh_inflight:
            return False
        _freshness_refresh_inflight = True

    def _worker() -> None:
        global _freshness_refresh_inflight
        try:
            snapshot = _dashboard_freshness_snapshot(force_refresh=True)
            _persist_freshness_snapshot_sync(snapshot)
        except Exception as exc:
            logger.error(f"Background freshness refresh failed: {exc}")
        finally:
            with _freshness_refresh_control_lock:
                _freshness_refresh_inflight = False

    thread = threading.Thread(target=_worker, daemon=True, name="freshness-refresh")
    thread.start()
    return True


def _load_recent_default_dashboard_snapshot(
    trusted_end: date,
    *,
    lookback_days: int = _DEFAULT_ALIAS_FALLBACK_LOOKBACK_DAYS,
) -> dict[str, Any]:
    total_read_ms = 0.0
    for offset in range(1, max(1, lookback_days) + 1):
        candidate_ctx = _dashboard_context_from_trusted_end(trusted_end - timedelta(days=offset))
        loaded = _load_persisted_dashboard_snapshot(_dashboard_snapshot_storage_path(candidate_ctx.cache_key))
        total_read_ms += float(loaded.get("readMs") or 0.0)
        if loaded.get("status") != "hit":
            continue
        if not _is_persisted_snapshot_usable(loaded.get("snapshotBuiltAt")):
            continue
        loaded["fallbackOffsetDays"] = offset
        loaded["readMs"] = round(total_read_ms, 2)
        return loaded
    return {"status": "miss", "readMs": round(total_read_ms, 2)}


def _schedule_default_topics_prewarm(ctx) -> bool:
    def _worker() -> None:
        try:
            get_topics_page(0, _DEFAULT_TOPICS_PREWARM_SIZE, ctx)
            logger.info(
                "Default topics cache warmed | key={} page=0 size={}",
                ctx.cache_key,
                _DEFAULT_TOPICS_PREWARM_SIZE,
            )
        except Exception as exc:
            logger.warning(
                "Default topics cache warm failed | key={} page=0 size={} error={}",
                ctx.cache_key,
                _DEFAULT_TOPICS_PREWARM_SIZE,
                exc,
            )

    try:
        submit_background(_worker)
        return True
    except Exception as exc:
        logger.warning(
            "Default topics cache warm could not be scheduled | key={} page=0 size={} error={}",
            ctx.cache_key,
            _DEFAULT_TOPICS_PREWARM_SIZE,
            exc,
        )
        return False


def _should_persist_dashboard_snapshot(meta: dict[str, Any]) -> bool:
    if bool(meta.get("isStale")):
        return False
    degraded = {
        str(name).strip()
        for name in (meta.get("degradedTiers") or [])
        if str(name).strip()
    }
    if degraded.intersection(DASHBOARD_CRITICAL_TIERS):
        return False
    if str(meta.get("cacheStatus") or "") in {
        "stale_on_error",
        "preserved_previous_on_fallback",
        "refresh_success_uncached_degraded",
    }:
        return False
    return True


def _build_dashboard_persisted_envelope(
    ctx,
    snapshot: dict,
    meta: dict[str, Any],
    *,
    trusted_end_date: str,
) -> tuple[dict[str, Any], datetime]:
    snapshot_built_at = _parse_snapshot_date(meta.get("snapshotBuiltAt")) or datetime.now(timezone.utc)
    envelope = {
        "schemaVersion": _DASHBOARD_PERSISTED_SCHEMA_VERSION,
        "snapshotBuiltAt": snapshot_built_at.isoformat(),
        "trustedEndDate": trusted_end_date,
        "fromDate": ctx.from_date.isoformat(),
        "toDate": ctx.to_date.isoformat(),
        "cacheKey": ctx.cache_key,
        "data": snapshot,
        "meta": dict(meta),
    }
    return envelope, snapshot_built_at


def _persist_dashboard_snapshot_sync(
    ctx,
    snapshot: dict,
    meta: dict[str, Any],
    *,
    trusted_end_date: str,
    write_default_alias: bool,
) -> dict[str, Any]:
    if not _should_persist_dashboard_snapshot(meta):
        return {"ok": False, "persistedWriteMs": 0.0, "skipped": True}

    envelope, snapshot_built_at = _build_dashboard_persisted_envelope(
        ctx,
        snapshot,
        meta,
        trusted_end_date=trusted_end_date,
    )
    payload = _serialize_runtime_envelope(envelope)
    started_at = time.perf_counter()
    primary_path = _dashboard_snapshot_storage_path(ctx.cache_key)
    writer = get_supabase_writer()
    primary_ok = writer.save_runtime_blob(primary_path, payload, content_type="application/gzip")
    alias_ok = True
    if write_default_alias and primary_ok:
        alias_ok = writer.save_runtime_blob(
            _DASHBOARD_DEFAULT_ALIAS_PATH,
            payload,
            content_type="application/gzip",
        )
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    updated_meta = dict(meta)
    updated_meta["persistedWriteMs"] = elapsed_ms
    if primary_ok:
        prime_dashboard_snapshot(
            ctx,
            snapshot,
            updated_meta,
            cached_at_ts=snapshot_built_at.timestamp(),
        )
        if write_default_alias:
            _schedule_default_topics_prewarm(ctx)
    logger.info(
        json.dumps(
            {
                "level": "info" if primary_ok else "warning",
                "message": "dashboard_snapshot_persisted",
                "cache_key": ctx.cache_key,
                "ok": bool(primary_ok and alias_ok),
                "primaryPath": primary_path,
                "defaultAliasWritten": bool(write_default_alias and alias_ok),
                "persistedWriteMs": elapsed_ms,
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
    )
    return {"ok": bool(primary_ok and alias_ok), "persistedWriteMs": elapsed_ms}


def _persist_dashboard_snapshot_async(
    ctx,
    snapshot: dict,
    meta: dict[str, Any],
    *,
    trusted_end_date: str,
    write_default_alias: bool,
) -> None:
    thread = threading.Thread(
        target=_persist_dashboard_snapshot_sync,
        args=(ctx, dict(snapshot), dict(meta)),
        kwargs={
            "trusted_end_date": trusted_end_date,
            "write_default_alias": write_default_alias,
        },
        daemon=True,
        name=f"dashboard-persist-{ctx.cache_key}",
    )
    thread.start()


def _ensure_background_dashboard_refresh(
    ctx,
    *,
    trusted_end_date: str,
    write_default_alias: bool,
) -> bool:
    with _dashboard_refresh_control_lock:
        if ctx.cache_key in _dashboard_refresh_inflight:
            return False
        _dashboard_refresh_inflight.add(ctx.cache_key)

    def _worker() -> None:
        try:
            snapshot, runtime_meta = get_dashboard_snapshot(ctx, force_refresh=True)
            _persist_dashboard_snapshot_sync(
                ctx,
                snapshot,
                runtime_meta,
                trusted_end_date=trusted_end_date,
                write_default_alias=write_default_alias,
            )
        except Exception as exc:
            logger.error(f"Background dashboard refresh failed | key={ctx.cache_key} error={exc}")
        finally:
            with _dashboard_refresh_control_lock:
                _dashboard_refresh_inflight.discard(ctx.cache_key)

    thread = threading.Thread(
        target=_worker,
        daemon=True,
        name=f"dashboard-refresh-{ctx.cache_key}",
    )
    thread.start()
    return True


def _build_dashboard_api_payload(
    *,
    ctx,
    trusted_end_date: str,
    dashboard_data: dict,
    dashboard_runtime_meta: dict[str, Any],
    requested_from: str,
    requested_to: str,
    cache_source: str,
    freshness_snapshot: dict | None,
    freshness_source: str | None,
    persisted_read_status: str | None = None,
    persisted_read_ms: float | None = None,
    cache_status_override: str | None = None,
    default_resolution_path: str | None = None,
) -> dict[str, Any]:
    trimmed_dashboard_data = _trim_dashboard_payload(dashboard_data)
    origin_cache_status = dashboard_runtime_meta.get("cacheStatus")
    response_meta = {
        "from": ctx.from_date.isoformat(),
        "to": ctx.to_date.isoformat(),
        "requestedFrom": requested_from,
        "requestedTo": requested_to,
        "days": ctx.days,
        "mode": "operational" if ctx.is_operational else "intelligence",
        "rangeLabel": ctx.range_label,
        "trustedEndDate": trusted_end_date,
        "degradedTiers": dashboard_runtime_meta.get("degradedTiers", []),
        "suppressedDegradedTiers": dashboard_runtime_meta.get("suppressedDegradedTiers", []),
        "tierTimes": dashboard_runtime_meta.get("tierTimes", {}),
        "snapshotBuiltAt": dashboard_runtime_meta.get("snapshotBuiltAt"),
        "cacheStatus": cache_status_override or origin_cache_status,
        "isStale": dashboard_runtime_meta.get("isStale", False),
        "buildElapsedSeconds": dashboard_runtime_meta.get("buildElapsedSeconds"),
        "buildMode": dashboard_runtime_meta.get("buildMode"),
        "refreshFailureCount": dashboard_runtime_meta.get("refreshFailureCount", 0),
        "cacheSource": cache_source,
        "freshnessSource": freshness_source,
        "persistedReadStatus": persisted_read_status,
        "persistedReadMs": persisted_read_ms,
        "persistedWriteMs": dashboard_runtime_meta.get("persistedWriteMs"),
        "freshness": {
            "status": freshness_snapshot.get("health", {}).get("status") if isinstance(freshness_snapshot, dict) else None,
            "generatedAt": freshness_snapshot.get("generated_at") if isinstance(freshness_snapshot, dict) else None,
        },
    }
    if default_resolution_path:
        response_meta["defaultResolutionPath"] = default_resolution_path
    if origin_cache_status and cache_status_override and cache_status_override != origin_cache_status:
        response_meta["originCacheStatus"] = origin_cache_status
    response_meta["responseBytes"] = -1
    response_meta["responseSerializeMs"] = 0
    return {
        "data": trimmed_dashboard_data,
        "meta": response_meta,
    }


def _should_use_historical_fastpath(*, default_request: bool) -> bool:
    return bool(_HISTORICAL_FASTPATH_ENABLED and not default_request and _HISTORICAL_FASTPATH_SKIP_TIERS)


def _newer_snapshot_choice(
    first: tuple[str, dict, dict[str, Any]] | None,
    second: tuple[str, dict, dict[str, Any]] | None,
) -> tuple[str, dict, dict[str, Any]] | None:
    if first is None:
        return second
    if second is None:
        return first
    first_built = _parse_snapshot_date(first[2].get("snapshotBuiltAt"))
    second_built = _parse_snapshot_date(second[2].get("snapshotBuiltAt"))
    if first_built is None:
        return second
    if second_built is None:
        return first
    return first if first_built >= second_built else second


def _clear_persisted_dashboard_cache() -> int:
    writer = get_supabase_writer()
    paths = {_DASHBOARD_DEFAULT_ALIAS_PATH, _FRESHNESS_PERSISTED_PATH}
    for row in writer.list_runtime_files(_DASHBOARD_PERSISTED_PREFIX):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        if "/" in name:
            paths.add(name)
        else:
            paths.add(f"{_DASHBOARD_PERSISTED_PREFIX}/{name}")
    deleted = writer.delete_runtime_files(sorted(paths))
    return deleted


_init_sentry()


@asynccontextmanager
async def app_lifespan(_app: FastAPI):
    startup_started_at = time.perf_counter()
    startup_phases: dict[str, float] = {}
    key_fp = hashlib.sha256(config.OPENAI_API_KEY.encode("utf-8")).hexdigest()[:12] if config.OPENAI_API_KEY else "missing"
    mark_runtime_draining(False)
    log_executor_configuration()
    logger.info(f"AI runtime configured | model={config.OPENAI_MODEL} key_fp={key_fp} role={APP_ROLE}")

    if _should_run_scraper_scheduler():
        scheduler_started_at = time.perf_counter()
        scheduler = get_scraper_scheduler()
        startup_phases["schedulerInitMs"] = round((time.perf_counter() - scheduler_started_at) * 1000, 2)
        scheduler_boot_at = time.perf_counter()
        await scheduler.startup()
        startup_phases["schedulerStartupMs"] = round((time.perf_counter() - scheduler_boot_at) * 1000, 2)
    else:
        logger.info("Scraper scheduler disabled for this runtime")

    cards_scheduler_started_at = time.perf_counter()
    started_cards_scheduler = False
    if _should_run_any_card_materializers():
        _start_question_cards_scheduler()
        _start_behavioral_cards_scheduler()
        _start_opportunity_cards_scheduler()
        started_cards_scheduler = True
    else:
        logger.info("Recurring card materializers disabled for this runtime")
    if _should_run_topic_overviews_materializer():
        _start_topic_overviews_scheduler()
        started_cards_scheduler = True
    else:
        logger.info("Topic overview materializer disabled for this runtime")
    if started_cards_scheduler:
        startup_phases["cardsSchedulerStartupMs"] = round((time.perf_counter() - cards_scheduler_started_at) * 1000, 2)

    if RUN_STARTUP_WARMERS:
        warmers_started_at = time.perf_counter()
        asyncio.create_task(_warm_dashboard_cache())
        if _should_run_question_card_materializer() and config.QUESTION_BRIEFS_REFRESH_ON_STARTUP:
            asyncio.create_task(_materialize_question_cards_once(force=False))
        if _should_run_behavioral_card_materializer() and config.BEHAVIORAL_BRIEFS_REFRESH_ON_STARTUP:
            asyncio.create_task(_materialize_behavioral_cards_once(force=False))
        if _should_run_opportunity_card_materializer() and config.OPPORTUNITY_BRIEFS_REFRESH_ON_STARTUP:
            asyncio.create_task(_materialize_opportunity_cards_once(force=False))
        if _should_run_topic_overviews_materializer() and config.TOPIC_OVERVIEWS_REFRESH_ON_STARTUP:
            asyncio.create_task(_materialize_topic_overviews_once(force=False))
        startup_phases["warmersEnqueuedMs"] = round((time.perf_counter() - warmers_started_at) * 1000, 2)

    startup_phases["totalStartupMs"] = round((time.perf_counter() - startup_started_at) * 1000, 2)
    logger.info(
        json.dumps(
            {
                "level": "info",
                "message": "startup_completed",
                "role": APP_ROLE,
                "background_jobs_enabled": _should_run_background_jobs(),
                "scraper_scheduler_enabled": _should_run_scraper_scheduler(),
                "card_materializers_enabled": _should_run_any_card_materializers(),
                "topic_overviews_materializer_enabled": _should_run_topic_overviews_materializer(),
                "run_startup_warmers": RUN_STARTUP_WARMERS,
                "requestExecutorWorkers": request_executor_workers(),
                "backgroundExecutorWorkers": background_executor_workers(),
                "phases": startup_phases,
            },
            ensure_ascii=True,
            separators=(",", ":"),
        )
    )

    try:
        yield
    finally:
        global question_cards_scheduler, behavioral_cards_scheduler, opportunity_cards_scheduler, topic_overviews_scheduler
        global scraper_scheduler, supabase_writer
        mark_runtime_draining(True)
        if question_cards_scheduler is not None:
            try:
                question_cards_scheduler.shutdown(wait=False)
            except Exception:
                pass
            question_cards_scheduler = None
        if behavioral_cards_scheduler is not None:
            try:
                behavioral_cards_scheduler.shutdown(wait=False)
            except Exception:
                pass
            behavioral_cards_scheduler = None
        if opportunity_cards_scheduler is not None:
            try:
                opportunity_cards_scheduler.shutdown(wait=False)
            except Exception:
                pass
            opportunity_cards_scheduler = None
        if topic_overviews_scheduler is not None:
            try:
                topic_overviews_scheduler.shutdown(wait=False)
            except Exception:
                pass
            topic_overviews_scheduler = None
        if scraper_scheduler is not None:
            await scraper_scheduler.shutdown(wait_for_cycle_seconds=30.0)
            scraper_scheduler = None
        shutdown_background_executor(wait=True)
        shutdown_request_executor(wait=True)
        supabase_writer = None
        db.close()
        logger.info("API server shut down — Neo4j driver closed")

app = FastAPI(
    title="Radar Obshchiny API",
    description="Dashboard data API for the Radar Obshchiny community intelligence platform",
    version="1.0.0",
    lifespan=app_lifespan,
)

_cors_allow_origins = config.CORS_ALLOW_ORIGINS or ["*"]
_cors_allow_credentials = "*" not in _cors_allow_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_allow_origins,
    allow_credentials=_cors_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _format_server_timing(request: Request, total_ms: float) -> str:
    metrics = [f"app;dur={total_ms:.2f}"]
    query_ms = getattr(request.state, "query_ms", None)
    if isinstance(query_ms, (int, float)):
        metrics.append(f"query;dur={float(query_ms):.2f}")
    return ", ".join(metrics)


def _record_query_timing(request: Request, started_at: float, *, cache_status: Optional[str] = None) -> None:
    elapsed_ms = (time.perf_counter() - started_at) * 1000
    request.state.query_ms = round(elapsed_ms, 2)
    request.state.executor_class = "request"
    if cache_status:
        request.state.cache_status = cache_status


@app.middleware("http")
async def request_metrics_middleware(request: Request, call_next):
    request_id = uuid.uuid4().hex[:12]
    request.state.request_id = request_id
    started_at = time.perf_counter()
    response = None
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
        if response is not None:
            response.headers["X-Request-ID"] = request_id
            if request.url.path in _SERVER_TIMING_PATHS:
                response.headers["Server-Timing"] = _format_server_timing(request, elapsed_ms)
        payload: dict[str, Any] = {
            "level": "info",
            "message": "request_completed",
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": status_code,
            "elapsed_ms": elapsed_ms,
        }
        cache_status = getattr(request.state, "cache_status", None)
        if cache_status:
            payload["cache_status"] = cache_status
        query_ms = getattr(request.state, "query_ms", None)
        if isinstance(query_ms, (int, float)):
            payload["query_ms"] = round(float(query_ms), 2)
        executor_class = getattr(request.state, "executor_class", None)
        if executor_class:
            payload["executorClass"] = executor_class
        dashboard_meta = getattr(request.state, "dashboard_meta", None)
        if isinstance(dashboard_meta, dict):
            for src_key, dst_key in (
                ("cacheStatus", "dashboard_cache_status"),
                ("cacheSource", "dashboard_cache_source"),
                ("freshnessSource", "dashboard_freshness_source"),
                ("buildElapsedSeconds", "dashboard_build_elapsed_seconds"),
                ("buildMode", "dashboard_build_mode"),
                ("tierTimes", "dashboard_tier_times"),
                ("refreshFailureCount", "dashboard_refresh_failure_count"),
                ("persistedReadMs", "dashboard_persisted_read_ms"),
                ("persistedWriteMs", "dashboard_persisted_write_ms"),
                ("persistedReadStatus", "dashboard_persisted_read_status"),
            ):
                value = dashboard_meta.get(src_key)
                if value not in (None, "", []):
                    payload[dst_key] = value
        logger.info(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))


class AIQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)


class ChannelSourceCreateRequest(BaseModel):
    channel_username: str = Field(..., min_length=1, max_length=256)
    channel_title: Optional[str] = Field(default=None, max_length=256)
    scrape_depth_days: int = Field(default=7, ge=1, le=3650)
    scrape_comments: bool = True


class ChannelSourceUpdateRequest(BaseModel):
    is_active: Optional[bool] = None
    scrape_depth_days: Optional[int] = Field(default=None, ge=1, le=3650)
    scrape_comments: Optional[bool] = None


class ScraperSchedulerUpdateRequest(BaseModel):
    interval_minutes: int = Field(..., ge=1, le=1440)


class AdminConfigPatchRequest(BaseModel):
    widgets: Optional[Dict[str, Dict[str, Any]]] = None
    prompts: Optional[Dict[str, Any]] = None
    runtime: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"


class GraphRequest(BaseModel):
    mode: Optional[str] = Field(default=None, max_length=64)
    timeframe: Optional[str] = Field(default="Last 7 Days", max_length=64)
    channels: Optional[List[str]] = None
    brandSource: Optional[List[str]] = None
    sentiment: Optional[List[str]] = None
    sentiments: Optional[List[str]] = None
    topics: Optional[List[str]] = None
    layers: Optional[List[str]] = None
    insightMode: Optional[str] = Field(default=None, max_length=64)
    sourceProfile: Optional[str] = Field(default=None, max_length=64)
    connectionStrength: Optional[int] = Field(default=None, ge=1, le=5)
    confidenceThreshold: Optional[int] = Field(default=None, ge=1, le=100)
    min_weight: Optional[int] = Field(default=None, ge=1, le=10000)
    max_nodes: Optional[int] = Field(default=None, ge=10, le=5000)
    max_edges: Optional[int] = Field(default=None, ge=10, le=10000)


class InsightCardsRequest(BaseModel):
    filters: dict = Field(default_factory=dict)
    audience: str = Field(default="analyst", max_length=32)


class FailureRetryRequest(BaseModel):
    scope_type: str = Field(..., min_length=1, max_length=32)
    scope_keys: List[str] = Field(default_factory=list)


class TopicProposalReviewRequest(BaseModel):
    topic_name: str = Field(..., min_length=1, max_length=200)
    decision: str = Field(..., min_length=1, max_length=16)
    canonical_topic: Optional[str] = Field(default=None, max_length=200)
    aliases: List[str] = Field(default_factory=list)
    notes: Optional[str] = Field(default=None, max_length=1000)
    reviewed_by: Optional[str] = Field(default=None, max_length=120)


supabase_writer: SupabaseWriter | None = None
scraper_scheduler: ScraperSchedulerService | None = None
question_cards_scheduler: AsyncIOScheduler | None = None
behavioral_cards_scheduler: AsyncIOScheduler | None = None
opportunity_cards_scheduler: AsyncIOScheduler | None = None
topic_overviews_scheduler: AsyncIOScheduler | None = None
USERNAME_RE = re.compile(r"^[a-z][a-z0-9_]{4,31}$")
_analytics_rate_limit_lock = threading.Lock()
_analytics_rate_limit_buckets: dict[tuple[str, str], list[float]] = {}
ADMIN_WIDGET_IDS = (
    "community_brief",
    "community_health_score",
    "trending_topics_feed",
    "topic_landscape",
    "conversation_trends",
    "question_cloud",
    "topic_lifecycle",
    "problem_tracker",
    "service_gap_detector",
    "satisfaction_by_area",
    "mood_over_time",
    "emotional_urgency_index",
    "top_channels",
    "key_voices",
    "recommendation_tracker",
    "information_velocity",
    "persona_gallery",
    "interest_radar",
    "community_growth_funnel",
    "retention_risk_gauge",
    "decision_stage_tracker",
    "emerging_interests",
    "new_vs_returning_voice",
    "business_opportunity_tracker",
    "job_market_pulse",
    "week_over_week_shifts",
    "sentiment_by_topic",
    "content_performance",
)
ADMIN_RUNTIME_STRING_KEYS = {
    "openaiModel",
    "questionBriefsModel",
    "behavioralBriefsModel",
    "opportunityBriefsModel",
    "topicOverviewsModel",
    "questionBriefsPromptVersion",
    "behavioralBriefsPromptVersion",
    "opportunityBriefsPromptVersion",
    "topicOverviewsPromptVersion",
    "topicOverviewsRefreshMinutes",
    "aiPostPromptStyle",
}
ADMIN_RUNTIME_BOOL_KEYS = {
    "featureQuestionBriefsAi",
    "featureBehavioralBriefsAi",
    "featureOpportunityBriefsAi",
    "featureTopicOverviewsAi",
}


def _admin_prompt_defaults() -> dict[str, str]:
    defaults: dict[str, str] = {}
    for provider in (
        intent_extractor.get_admin_prompt_defaults,
        question_briefs.get_admin_prompt_defaults,
        behavioral_briefs.get_admin_prompt_defaults,
        opportunity_briefs.get_admin_prompt_defaults,
        topic_overviews.get_admin_prompt_defaults,
        recommendation_briefs.get_admin_prompt_defaults,
    ):
        defaults.update(provider())
    return defaults


def get_supabase_writer() -> SupabaseWriter:
    global supabase_writer
    if supabase_writer is None:
        supabase_writer = SupabaseWriter()
    return supabase_writer


def get_scraper_scheduler() -> ScraperSchedulerService:
    global scraper_scheduler
    if scraper_scheduler is None:
        scraper_scheduler = ScraperSchedulerService(get_supabase_writer())
    return scraper_scheduler


def get_current_scraper_scheduler_status() -> dict[str, Any]:
    if scraper_scheduler is None:
        return {
            "status": "stopped",
            "is_active": False,
            "interval_minutes": 15,
            "running_now": False,
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_success_at": None,
            "next_run_at": None,
            "last_error": None,
            "last_result": None,
            "last_mode": "normal",
            "catchup_limits": {
                "comment_limit": config.AI_CATCHUP_COMMENT_LIMIT,
                "post_limit": config.AI_CATCHUP_POST_LIMIT,
                "sync_limit": config.AI_CATCHUP_SYNC_LIMIT,
            },
            "normal_limits": {
                "comment_limit": config.AI_NORMAL_COMMENT_LIMIT,
                "post_limit": config.AI_NORMAL_POST_LIMIT,
                "sync_limit": config.AI_NORMAL_SYNC_LIMIT,
            },
            "run_history": [],
            "persisted": None,
        }
    return scraper_scheduler.status()


def _default_admin_config() -> dict[str, Any]:
    prompt_defaults = _admin_prompt_defaults()
    return {
        "widgets": {widget_id: {"enabled": True} for widget_id in ADMIN_WIDGET_IDS},
        "prompts": dict(prompt_defaults),
        "promptDefaults": dict(prompt_defaults),
        "runtime": {
            "openaiModel": config.OPENAI_MODEL,
            "questionBriefsModel": config.QUESTION_BRIEFS_MODEL,
            "behavioralBriefsModel": config.BEHAVIORAL_BRIEFS_MODEL,
            "opportunityBriefsModel": config.OPPORTUNITY_BRIEFS_MODEL,
            "topicOverviewsModel": config.TOPIC_OVERVIEWS_MODEL,
            "questionBriefsPromptVersion": config.QUESTION_BRIEFS_PROMPT_VERSION,
            "behavioralBriefsPromptVersion": config.BEHAVIORAL_BRIEFS_PROMPT_VERSION,
            "opportunityBriefsPromptVersion": config.OPPORTUNITY_BRIEFS_PROMPT_VERSION,
            "topicOverviewsPromptVersion": config.TOPIC_OVERVIEWS_PROMPT_VERSION,
            "topicOverviewsRefreshMinutes": str(config.TOPIC_OVERVIEWS_REFRESH_MINUTES),
            "aiPostPromptStyle": config.AI_POST_PROMPT_STYLE,
            "featureQuestionBriefsAi": bool(config.FEATURE_QUESTION_BRIEFS_AI),
            "featureBehavioralBriefsAi": bool(config.FEATURE_BEHAVIORAL_BRIEFS_AI),
            "featureOpportunityBriefsAi": bool(config.FEATURE_OPPORTUNITY_BRIEFS_AI),
            "featureTopicOverviewsAi": bool(config.FEATURE_TOPIC_OVERVIEWS_AI),
        },
    }


def _active_ai_runtime_summary() -> dict[str, str | bool]:
    raw_config = load_admin_config_raw()
    runtime = raw_config.get("runtime") if isinstance(raw_config, dict) and isinstance(raw_config.get("runtime"), dict) else {}

    def _runtime_value(key: str, fallback: Any) -> Any:
        value = runtime.get(key)
        if value is None:
            return fallback
        if isinstance(value, str):
            text = value.strip()
            return text or fallback
        return value

    return {
        "openaiModel": str(_runtime_value("openaiModel", config.OPENAI_MODEL)),
        "questionBriefsModel": str(_runtime_value("questionBriefsModel", config.QUESTION_BRIEFS_MODEL)),
        "behavioralBriefsModel": str(_runtime_value("behavioralBriefsModel", config.BEHAVIORAL_BRIEFS_MODEL)),
        "opportunityBriefsModel": str(_runtime_value("opportunityBriefsModel", config.OPPORTUNITY_BRIEFS_MODEL)),
        "topicOverviewsModel": str(_runtime_value("topicOverviewsModel", config.TOPIC_OVERVIEWS_MODEL)),
        "questionBriefsPromptVersion": str(_runtime_value("questionBriefsPromptVersion", config.QUESTION_BRIEFS_PROMPT_VERSION)),
        "behavioralBriefsPromptVersion": str(_runtime_value("behavioralBriefsPromptVersion", config.BEHAVIORAL_BRIEFS_PROMPT_VERSION)),
        "opportunityBriefsPromptVersion": str(_runtime_value("opportunityBriefsPromptVersion", config.OPPORTUNITY_BRIEFS_PROMPT_VERSION)),
        "topicOverviewsPromptVersion": str(_runtime_value("topicOverviewsPromptVersion", config.TOPIC_OVERVIEWS_PROMPT_VERSION)),
        "topicOverviewsRefreshMinutes": str(_runtime_value("topicOverviewsRefreshMinutes", config.TOPIC_OVERVIEWS_REFRESH_MINUTES)),
        "aiPostPromptStyle": str(_runtime_value("aiPostPromptStyle", config.AI_POST_PROMPT_STYLE)),
        "featureExtractionV2": bool(config.FEATURE_EXTRACTION_V2),
        "featureTopicOverviewsAi": bool(_runtime_value("featureTopicOverviewsAi", config.FEATURE_TOPIC_OVERVIEWS_AI)),
    }


def _validate_admin_widgets(raw_widgets: Any) -> dict[str, dict[str, bool]]:
    if raw_widgets is None:
        return {}
    if not isinstance(raw_widgets, dict):
        raise HTTPException(status_code=400, detail="widgets must be an object")

    validated: dict[str, dict[str, bool]] = {}
    for widget_id, value in raw_widgets.items():
        if widget_id not in ADMIN_WIDGET_IDS:
            raise HTTPException(status_code=400, detail=f"Unknown widget id: {widget_id}")
        if not isinstance(value, dict):
            raise HTTPException(status_code=400, detail=f"Widget config for {widget_id} must be an object")
        enabled = value.get("enabled")
        if not isinstance(enabled, bool):
            raise HTTPException(status_code=400, detail=f"Widget {widget_id} requires boolean enabled")
        validated[widget_id] = {"enabled": enabled}
    return validated


def _validate_admin_prompts(raw_prompts: Any) -> dict[str, str]:
    if raw_prompts is None:
        return {}
    if not isinstance(raw_prompts, dict):
        raise HTTPException(status_code=400, detail="prompts must be an object")

    defaults = _admin_prompt_defaults()
    validated: dict[str, str] = {}
    for key, value in raw_prompts.items():
        if key not in defaults:
            raise HTTPException(status_code=400, detail=f"Unknown prompt key: {key}")
        if not isinstance(value, str):
            raise HTTPException(status_code=400, detail=f"Prompt {key} must be a string")
        text = value.strip()
        if not text:
            raise HTTPException(status_code=400, detail=f"Prompt {key} cannot be empty")
        validated[key] = text
    return validated


def _validate_admin_runtime(raw_runtime: Any) -> dict[str, Any]:
    if raw_runtime is None:
        return {}
    if not isinstance(raw_runtime, dict):
        raise HTTPException(status_code=400, detail="runtime must be an object")

    validated: dict[str, Any] = {}
    allowed_keys = ADMIN_RUNTIME_STRING_KEYS.union(ADMIN_RUNTIME_BOOL_KEYS)
    for key, value in raw_runtime.items():
        if key not in allowed_keys:
            raise HTTPException(status_code=400, detail=f"Unknown runtime key: {key}")
        if key in ADMIN_RUNTIME_BOOL_KEYS:
            if not isinstance(value, bool):
                raise HTTPException(status_code=400, detail=f"Runtime field {key} must be boolean")
            validated[key] = value
            continue
        if not isinstance(value, str):
            raise HTTPException(status_code=400, detail=f"Runtime field {key} must be a string")
        text = value.strip()
        if not text:
            raise HTTPException(status_code=400, detail=f"Runtime field {key} cannot be empty")
        if key == "aiPostPromptStyle" and text.lower() not in {"compact", "full"}:
            raise HTTPException(status_code=400, detail="aiPostPromptStyle must be compact or full")
        validated[key] = text.lower() if key == "aiPostPromptStyle" else text
    return validated


def _configured_analytics_tokens() -> list[str]:
    return [
        token
        for token in (
            config.ANALYTICS_API_KEY_FRONTEND,
            config.ANALYTICS_API_KEY_OPENCLAW,
        )
        if token
    ]


def _token_fingerprint(token: str | None) -> str:
    text = (token or "").strip()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def _analytics_client_ip(request: Request) -> str:
    forwarded_for = (request.headers.get("x-forwarded-for") or "").strip()
    if config.ANALYTICS_RATE_LIMIT_TRUST_PROXY and forwarded_for:
        return forwarded_for.split(",")[0].strip() or "unknown"
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _enforce_analytics_rate_limit(request: Request) -> None:
    if not config.ANALYTICS_RATE_LIMIT_ENABLED:
        return

    now = time.monotonic()
    window_seconds = max(1, int(config.ANALYTICS_RATE_LIMIT_WINDOW_SECONDS))
    max_requests = max(1, int(config.ANALYTICS_RATE_LIMIT_MAX_REQUESTS))
    bucket_key = (_analytics_client_ip(request), request.url.path)

    with _analytics_rate_limit_lock:
        timestamps = _analytics_rate_limit_buckets.get(bucket_key, [])
        cutoff = now - window_seconds
        timestamps = [ts for ts in timestamps if ts >= cutoff]
        if len(timestamps) >= max_requests:
            _analytics_rate_limit_buckets[bucket_key] = timestamps
            logger.warning(
                "Analytics rate limit exceeded | endpoint={} client_ip={} count={}".format(
                    request.url.path,
                    bucket_key[0],
                    len(timestamps),
                )
            )
            raise HTTPException(status_code=429, detail="Rate limit exceeded for analytics API.")
        timestamps.append(now)
        _analytics_rate_limit_buckets[bucket_key] = timestamps


def require_analytics_access(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> None:
    _enforce_analytics_rate_limit(request)

    if not config.ANALYTICS_API_REQUIRE_AUTH:
        return

    valid_tokens = _configured_analytics_tokens()
    if not valid_tokens:
        logger.error(
            "Analytics auth enabled without configured tokens | endpoint={}".format(request.url.path)
        )
        raise HTTPException(status_code=503, detail="Analytics API auth is enabled but no server token is configured.")

    if not authorization:
        logger.warning(
            "Analytics auth failure | endpoint={} reason=missing_header client_ip={}".format(
                request.url.path,
                _analytics_client_ip(request),
            )
        )
        raise HTTPException(status_code=401, detail="Missing Authorization header.")

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        logger.warning(
            "Analytics auth failure | endpoint={} reason=bad_scheme client_ip={}".format(
                request.url.path,
                _analytics_client_ip(request),
            )
        )
        raise HTTPException(status_code=401, detail="Authorization header must use Bearer token.")

    clean_token = token.strip()
    if any(hmac.compare_digest(clean_token, valid) for valid in valid_tokens):
        return

    logger.warning(
        "Analytics auth failure | endpoint={} reason=invalid_token token_fp={} client_ip={}".format(
            request.url.path,
            _token_fingerprint(clean_token),
            _analytics_client_ip(request),
        )
    )
    raise HTTPException(status_code=401, detail="Invalid analytics API token.")


def _sanitize_admin_config(raw_config: Any) -> dict[str, Any]:
    defaults = _default_admin_config()
    payload = raw_config if isinstance(raw_config, dict) else {}

    widgets = payload.get("widgets") if isinstance(payload.get("widgets"), dict) else {}
    prompts = payload.get("prompts") if isinstance(payload.get("prompts"), dict) else {}
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}

    defaults["widgets"].update(_validate_admin_widgets(widgets))
    defaults["prompts"].update(_validate_admin_prompts(prompts))
    defaults["runtime"].update(_validate_admin_runtime(runtime))
    return defaults


def _load_admin_config() -> dict[str, Any]:
    try:
        return _sanitize_admin_config(load_admin_config_raw())
    except HTTPException:
        logger.warning("Admin config contains invalid values; falling back to defaults")
        return _default_admin_config()


def _admin_config_response() -> dict[str, Any]:
    payload = _load_admin_config()
    warning = get_admin_config_runtime_warning()
    if warning:
        payload["warning"] = warning
    return payload


def _parse_snapshot_date(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _trusted_end_date_from_freshness(snapshot: dict) -> datetime.date:
    now = datetime.now(timezone.utc)
    sync = snapshot.get("pipeline", {}).get("sync", {})
    process = snapshot.get("pipeline", {}).get("process", {})
    drift = snapshot.get("drift", {})

    candidate = _parse_snapshot_date(
        sync.get("last_graph_sync_at")
        or drift.get("neo4j_last_post_at")
        or drift.get("supabase_last_post_at")
        or snapshot.get("generated_at")
    )
    trusted_end = (candidate or (now - timedelta(days=1))).date()

    sync_status = str(sync.get("status") or "unknown").lower()
    process_status = str(process.get("status") or "unknown").lower()
    sync_age_minutes = sync.get("age_minutes")
    process_age_minutes = process.get("age_minutes")

    if (
        trusted_end == now.date() and (
            sync_status != "healthy"
            or process_status in {"warning", "stale"}
            or (isinstance(sync_age_minutes, (int, float)) and sync_age_minutes > 180)
            or (isinstance(process_age_minutes, (int, float)) and process_age_minutes > 180)
        )
    ):
        trusted_end = trusted_end - timedelta(days=1)

    return trusted_end


def _dashboard_freshness_snapshot(force_refresh: bool = False) -> dict:
    return get_freshness_snapshot(
        get_supabase_writer(),
        scheduler_status=get_current_scraper_scheduler_status(),
        force_refresh=force_refresh,
    )


def _default_dashboard_context(snapshot: Optional[dict] = None):
    freshness_snapshot = snapshot or _dashboard_freshness_snapshot(force_refresh=False)
    trusted_end = _trusted_end_date_from_freshness(freshness_snapshot)
    from_date = (trusted_end - timedelta(days=14)).isoformat()
    return build_dashboard_date_context(from_date, trusted_end.isoformat())


def _build_dashboard_response_payload(
    from_date: Optional[str],
    to_date: Optional[str],
) -> dict[str, Any]:
    default_request = not from_date or not to_date
    freshness_snapshot: dict | None = None
    freshness_source: str | None = None
    persisted_read_status: str | None = None
    persisted_read_ms: float | None = None

    if default_request:
        freshness_resolution = _cached_freshness_resolution(allow_live=False)
        freshness_snapshot = freshness_resolution.get("snapshot")
        freshness_source = freshness_resolution.get("source")

        if freshness_snapshot is not None:
            trusted_end = _trusted_end_date_from_freshness(freshness_snapshot)
            ctx = _dashboard_context_from_trusted_end(trusted_end)
            trusted_end_iso = trusted_end.isoformat()
        else:
            default_snapshot = _load_persisted_dashboard_snapshot(_DASHBOARD_DEFAULT_ALIAS_PATH)
            persisted_read_status = default_snapshot.get("status")
            persisted_read_ms = default_snapshot.get("readMs")
            if default_snapshot.get("status") == "hit" and _is_persisted_snapshot_usable(default_snapshot.get("snapshotBuiltAt")):
                ctx = default_snapshot["ctx"]
                trusted_end_iso = str(default_snapshot.get("trustedEndDate") or ctx.to_date.isoformat())
                dashboard_meta = dict(default_snapshot.get("meta") or {})
                dashboard_meta["isStale"] = not _is_persisted_snapshot_fresh(default_snapshot.get("snapshotBuiltAt"))
                prime_dashboard_snapshot(
                    ctx,
                    default_snapshot["snapshot"],
                    dashboard_meta,
                    cached_at_ts=default_snapshot["snapshotBuiltAt"].timestamp(),
                )
                refresh_started = False
                if dashboard_meta.get("isStale"):
                    refresh_started = _ensure_background_dashboard_refresh(
                        ctx,
                        trusted_end_date=trusted_end_iso,
                        write_default_alias=True,
                    )
                _ensure_background_freshness_refresh()
                return _build_dashboard_api_payload(
                    ctx=ctx,
                    trusted_end_date=trusted_end_iso,
                    dashboard_data=default_snapshot["snapshot"],
                    dashboard_runtime_meta=dashboard_meta,
                    requested_from=ctx.from_date.isoformat(),
                    requested_to=ctx.to_date.isoformat(),
                    cache_source="persisted",
                    freshness_snapshot=None,
                    freshness_source=None,
                    persisted_read_status=persisted_read_status,
                    persisted_read_ms=persisted_read_ms,
                    default_resolution_path="persisted_alias",
                    cache_status_override=(
                        "persisted_stale_while_revalidate" if dashboard_meta.get("isStale") and refresh_started
                        else "persisted_stale_refresh_inflight" if dashboard_meta.get("isStale")
                        else "persisted_fresh"
                    ),
                )

            freshness_resolution = _cached_freshness_resolution(allow_live=True)
            freshness_snapshot = freshness_resolution.get("snapshot")
            freshness_source = freshness_resolution.get("source")
            trusted_end = _trusted_end_date_from_freshness(freshness_snapshot or {})
            ctx = _dashboard_context_from_trusted_end(trusted_end)
            trusted_end_iso = trusted_end.isoformat()
    else:
        ctx = build_dashboard_date_context(from_date or "", to_date or "")
        trusted_end_iso = ctx.to_date.isoformat()
        freshness_resolution = _cached_freshness_resolution(allow_live=False)
        freshness_snapshot = freshness_resolution.get("snapshot")
        freshness_source = freshness_resolution.get("source")

    requested_from = from_date or ctx.from_date.isoformat()
    requested_to = to_date or ctx.to_date.isoformat()

    memory_snapshot, memory_meta, memory_state = peek_dashboard_snapshot(ctx)
    if memory_state == "fresh" and memory_snapshot is not None and memory_meta is not None:
        return _build_dashboard_api_payload(
            ctx=ctx,
            trusted_end_date=trusted_end_iso,
            dashboard_data=memory_snapshot,
            dashboard_runtime_meta=memory_meta,
            requested_from=requested_from,
            requested_to=requested_to,
            cache_source="memory",
            freshness_snapshot=freshness_snapshot,
            freshness_source=freshness_source,
            default_resolution_path="memory" if default_request else None,
            cache_status_override="memory_fresh",
        )

    persisted_snapshot = _load_persisted_dashboard_snapshot(_dashboard_snapshot_storage_path(ctx.cache_key))
    persisted_read_status = persisted_snapshot.get("status")
    persisted_read_ms = persisted_snapshot.get("readMs")

    memory_stale_choice: tuple[str, dict, dict[str, Any]] | None = None
    if memory_state == "stale" and memory_snapshot is not None and memory_meta is not None:
        memory_stale_choice = ("memory", memory_snapshot, dict(memory_meta))

    persisted_stale_choice: tuple[str, dict, dict[str, Any]] | None = None
    if persisted_snapshot.get("status") == "hit":
        persisted_meta = dict(persisted_snapshot.get("meta") or {})
        persisted_meta["isStale"] = not _is_persisted_snapshot_fresh(persisted_snapshot.get("snapshotBuiltAt"))
        if not persisted_meta["isStale"]:
            prime_dashboard_snapshot(
                ctx,
                persisted_snapshot["snapshot"],
                persisted_meta,
                cached_at_ts=persisted_snapshot["snapshotBuiltAt"].timestamp(),
            )
            return _build_dashboard_api_payload(
                ctx=ctx,
                trusted_end_date=trusted_end_iso,
                dashboard_data=persisted_snapshot["snapshot"],
                dashboard_runtime_meta=persisted_meta,
                requested_from=requested_from,
                requested_to=requested_to,
                cache_source="persisted",
                freshness_snapshot=freshness_snapshot,
                freshness_source=freshness_source,
                persisted_read_status=persisted_read_status,
                persisted_read_ms=persisted_read_ms,
                default_resolution_path="persisted_exact" if default_request else None,
                cache_status_override="persisted_fresh",
            )

        if _is_persisted_snapshot_usable(persisted_snapshot.get("snapshotBuiltAt")):
            persisted_stale_choice = ("persisted", persisted_snapshot["snapshot"], persisted_meta)

    stale_choice = _newer_snapshot_choice(memory_stale_choice, persisted_stale_choice)
    if stale_choice is not None:
        cache_source, stale_snapshot, stale_meta = stale_choice
        if cache_source == "persisted" and persisted_snapshot.get("status") == "hit":
            prime_dashboard_snapshot(
                ctx,
                stale_snapshot,
                stale_meta,
                cached_at_ts=persisted_snapshot["snapshotBuiltAt"].timestamp(),
            )
        refresh_started = _ensure_background_dashboard_refresh(
            ctx,
            trusted_end_date=trusted_end_iso,
            write_default_alias=default_request,
        )
        if default_request and freshness_snapshot is None:
            _ensure_background_freshness_refresh()
        stale_meta["isStale"] = True
        return _build_dashboard_api_payload(
            ctx=ctx,
            trusted_end_date=trusted_end_iso,
            dashboard_data=stale_snapshot,
            dashboard_runtime_meta=stale_meta,
            requested_from=requested_from,
            requested_to=requested_to,
            cache_source=cache_source,
            freshness_snapshot=freshness_snapshot,
            freshness_source=freshness_source,
            persisted_read_status=persisted_read_status,
            persisted_read_ms=persisted_read_ms,
            default_resolution_path=(
                "persisted_exact" if default_request and cache_source == "persisted"
                else "memory" if default_request and cache_source == "memory"
                else None
            ),
            cache_status_override=(
                f"{cache_source}_stale_while_revalidate"
                if refresh_started
                else f"{cache_source}_stale_refresh_inflight"
            ),
        )

    if default_request:
        recent_default_snapshot = _load_recent_default_dashboard_snapshot(trusted_end)
        if recent_default_snapshot.get("status") == "hit":
            fallback_ctx = recent_default_snapshot["ctx"]
            fallback_meta = dict(recent_default_snapshot.get("meta") or {})
            fallback_meta["isStale"] = True
            prime_dashboard_snapshot(
                fallback_ctx,
                recent_default_snapshot["snapshot"],
                fallback_meta,
                cached_at_ts=recent_default_snapshot["snapshotBuiltAt"].timestamp(),
            )
            refresh_started = _ensure_background_dashboard_refresh(
                ctx,
                trusted_end_date=trusted_end_iso,
                write_default_alias=True,
            )
            if freshness_snapshot is None:
                _ensure_background_freshness_refresh()
            return _build_dashboard_api_payload(
                ctx=fallback_ctx,
                trusted_end_date=str(recent_default_snapshot.get("trustedEndDate") or fallback_ctx.to_date.isoformat()),
                dashboard_data=recent_default_snapshot["snapshot"],
                dashboard_runtime_meta=fallback_meta,
                requested_from=requested_from,
                requested_to=requested_to,
                cache_source="persisted",
                freshness_snapshot=freshness_snapshot,
                freshness_source=freshness_source,
                persisted_read_status="recent_fallback_hit",
                persisted_read_ms=round(float(persisted_read_ms or 0.0) + float(recent_default_snapshot.get("readMs") or 0.0), 2),
                default_resolution_path="persisted_recent_fallback",
                cache_status_override=(
                    "persisted_recent_fallback_while_revalidate"
                    if refresh_started
                    else "persisted_recent_fallback_refresh_inflight"
                ),
            )

    if _should_use_historical_fastpath(default_request=default_request):
        try:
            dashboard_data, dashboard_runtime_meta = build_dashboard_snapshot_once(
                ctx,
                skipped_tiers=set(_HISTORICAL_FASTPATH_SKIP_TIERS),
                cache_status="historical_fastpath_uncached",
            )
            critical_degraded = {
                str(name).strip()
                for name in (dashboard_runtime_meta.get("degradedTiers") or [])
                if str(name).strip()
            }.intersection(DASHBOARD_CRITICAL_TIERS)
            if not critical_degraded:
                refresh_started = _ensure_background_dashboard_refresh(
                    ctx,
                    trusted_end_date=trusted_end_iso,
                    write_default_alias=default_request,
                )
                logger.info(
                    "Historical dashboard fastpath served | key={} skipped_tiers={} refresh_started={}",
                    ctx.cache_key,
                    sorted(_HISTORICAL_FASTPATH_SKIP_TIERS),
                    refresh_started,
                )
                return _build_dashboard_api_payload(
                    ctx=ctx,
                    trusted_end_date=trusted_end_iso,
                    dashboard_data=dashboard_data,
                    dashboard_runtime_meta=dashboard_runtime_meta,
                    requested_from=requested_from,
                    requested_to=requested_to,
                    cache_source="fastpath",
                    freshness_snapshot=freshness_snapshot,
                    freshness_source=freshness_source,
                    persisted_read_status=persisted_read_status,
                    persisted_read_ms=persisted_read_ms,
                    cache_status_override=(
                        "historical_fastpath_while_revalidate"
                        if refresh_started
                        else "historical_fastpath_refresh_inflight"
                    ),
                )
            logger.warning(
                "Historical dashboard fastpath fell back to full rebuild because critical tiers degraded | key={} degraded={}",
                ctx.cache_key,
                sorted(critical_degraded),
            )
        except Exception as exc:
            logger.warning(f"Historical dashboard fastpath failed | key={ctx.cache_key} error={exc}")

    dashboard_data, dashboard_runtime_meta = get_dashboard_snapshot(ctx, force_refresh=True)
    if _should_persist_dashboard_snapshot(dashboard_runtime_meta):
        _persist_dashboard_snapshot_async(
            ctx,
            dashboard_data,
            dashboard_runtime_meta,
            trusted_end_date=trusted_end_iso,
            write_default_alias=default_request,
        )
    return _build_dashboard_api_payload(
        ctx=ctx,
        trusted_end_date=trusted_end_iso,
        dashboard_data=dashboard_data,
        dashboard_runtime_meta=dashboard_runtime_meta,
        requested_from=requested_from,
        requested_to=requested_to,
        cache_source="rebuild",
        freshness_snapshot=freshness_snapshot,
        freshness_source=freshness_source,
        persisted_read_status=persisted_read_status,
        persisted_read_ms=persisted_read_ms,
        default_resolution_path="rebuild" if default_request else None,
    )


async def _warm_dashboard_cache() -> None:
    """Warm dashboard cache in background after startup."""
    try:
        await run_background(lambda: _build_dashboard_response_payload(None, None))
        logger.info("Dashboard cache warm-up completed")
    except Exception as e:
        logger.warning(f"Dashboard cache warm-up failed: {e}")


async def _materialize_question_cards_once(force: bool = False) -> None:
    """Run question-card materialization off the request path."""
    if not _should_run_question_card_materializer():
        logger.info("Question cards materialization skipped | disabled=true")
        return
    try:
        cards = await run_background(lambda: question_briefs.refresh_question_briefs(force=force))
        logger.info(f"Question cards materialization completed | cards={len(cards)}")
    except Exception as e:
        logger.warning(f"Question cards materialization failed: {e}")


async def _materialize_behavioral_cards_once(force: bool = False) -> None:
    """Run behavioral-card materialization off the request path."""
    if not _should_run_behavioral_card_materializer():
        logger.info("Behavioral cards materialization skipped | disabled=true")
        return
    try:
        payload = await run_background(lambda: behavioral_briefs.refresh_behavioral_briefs(force=force))
        problems = len(payload.get("problemBriefs") or []) if isinstance(payload, dict) else 0
        services = len(payload.get("serviceGapBriefs") or []) if isinstance(payload, dict) else 0
        logger.info(f"Behavioral cards materialization completed | problem_cards={problems} service_cards={services}")
    except Exception as e:
        logger.warning(f"Behavioral cards materialization failed: {e}")


async def _materialize_opportunity_cards_once(force: bool = False) -> None:
    """Run opportunity-card materialization off the request path."""
    if not _should_run_opportunity_card_materializer():
        logger.info("Opportunity cards materialization skipped | disabled=true")
        return
    try:
        cards = await run_background(lambda: opportunity_briefs.refresh_opportunity_briefs(force=force))
        logger.info(f"Opportunity cards materialization completed | cards={len(cards)}")
    except Exception as e:
        logger.warning(f"Opportunity cards materialization failed: {e}")


async def _materialize_topic_overviews_once(force: bool = False) -> None:
    """Run topic-overview materialization off the request path."""
    if not _should_run_topic_overviews_materializer():
        logger.info("Topic overviews materialization skipped | disabled=true")
        return
    try:
        freshness_snapshot = _dashboard_freshness_snapshot(force_refresh=False)
        ctx = _default_dashboard_context(freshness_snapshot)
        payload = await run_background(
            lambda: topic_overviews.refresh_topic_overviews(ctx=ctx, force=force),
        )
        items = len(payload.get("items") or []) if isinstance(payload, dict) else 0
        logger.info(f"Topic overviews materialization completed | items={items} window={ctx.cache_key}")
    except Exception as e:
        logger.warning(f"Topic overviews materialization failed: {e}")


def _start_question_cards_scheduler() -> None:
    """Start recurring question-card materialization scheduler."""
    global question_cards_scheduler
    if not _should_run_question_card_materializer():
        return

    interval = max(15, int(config.QUESTION_BRIEFS_REFRESH_MINUTES))
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        _materialize_question_cards_once,
        "interval",
        minutes=interval,
        id="question-cards-materializer",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    scheduler.start()
    question_cards_scheduler = scheduler
    logger.info(f"Question cards scheduler ready | interval={interval}m")


def _start_behavioral_cards_scheduler() -> None:
    """Start recurring W8/W9 card materialization scheduler."""
    global behavioral_cards_scheduler
    if not _should_run_behavioral_card_materializer():
        return

    interval = max(15, int(config.BEHAVIORAL_BRIEFS_REFRESH_MINUTES))
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        _materialize_behavioral_cards_once,
        "interval",
        minutes=interval,
        id="behavioral-cards-materializer",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    scheduler.start()
    behavioral_cards_scheduler = scheduler
    logger.info(f"Behavioral cards scheduler ready | interval={interval}m")


def _start_opportunity_cards_scheduler() -> None:
    """Start recurring business-opportunity card materialization scheduler."""
    global opportunity_cards_scheduler
    if not _should_run_opportunity_card_materializer():
        return

    interval = max(15, int(config.OPPORTUNITY_BRIEFS_REFRESH_MINUTES))
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        _materialize_opportunity_cards_once,
        "interval",
        minutes=interval,
        id="opportunity-cards-materializer",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    scheduler.start()
    opportunity_cards_scheduler = scheduler
    logger.info(f"Opportunity cards scheduler ready | interval={interval}m")


def _start_topic_overviews_scheduler() -> None:
    """Start recurring topic-overview materialization scheduler."""
    global topic_overviews_scheduler
    if not _should_run_topic_overviews_materializer():
        return

    interval = max(15, int(topic_overviews.get_topic_overviews_refresh_minutes()))
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        _materialize_topic_overviews_once,
        "interval",
        minutes=interval,
        id="topic-overviews-materializer",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    scheduler.start()
    topic_overviews_scheduler = scheduler
    logger.info(f"Topic overviews scheduler ready | interval={interval}m")


def _normalize_channel_username(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""

    value = re.sub(r"^https?://", "", value, flags=re.IGNORECASE)
    lowered = value.lower()
    if lowered.startswith("t.me/"):
        value = value[5:]
    elif lowered.startswith("telegram.me/"):
        value = value[12:]

    value = value.split("?", 1)[0].split("#", 1)[0]
    if value.startswith("@"):
        value = value[1:]
    if "/" in value:
        value = value.split("/", 1)[0]

    return value.strip().lower().lstrip("@")


def _canonical_channel_username(handle: str) -> str:
    normalized = (handle or "").strip().lower().lstrip("@")
    return f"@{normalized}" if normalized else ""


async def _try_enrich_channel_metadata(
    channel_uuid: str,
    canonical_username: str,
    fallback_title: Optional[str] = None,
) -> None:
    """
    Best-effort Telegram metadata enrichment for a channel source.

    This fills telegram_channel_id (and title where available) right after source
    creation/activation so users don't have to wait for the scraper cycle.
    """
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        import os
    except Exception as e:
        logger.warning(f"Telethon unavailable for metadata enrichment: {e}")
        return

    username = _canonical_channel_username(canonical_username)
    if not username:
        return

    # Check for session string from environment (Railway deployment)
    session_string = os.getenv("TELEGRAM_SESSION_STRING")

    if session_string:
        client = TelegramClient(
            StringSession(session_string),
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )
    else:
        # Use file-based session for local development
        client = TelegramClient(
            config.TELEGRAM_SESSION_NAME,
            config.TELEGRAM_API_ID,
            config.TELEGRAM_API_HASH,
        )

    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.warning(f"Metadata enrichment skipped for {username}: Telegram session is not authorized")
            return

        metadata = await get_full_channel_metadata(client, username=username)
        if not metadata.get("channel_title") and fallback_title:
            metadata["channel_title"] = fallback_title

        get_supabase_writer().update_channel_metadata(channel_uuid, metadata)
    except Exception as e:
        logger.warning(f"Metadata enrichment failed for {username}: {e}")
    finally:
        client.disconnect()


def _validate_channel_username(username: str) -> None:
    if not USERNAME_RE.match(username):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid Telegram channel username. Use @name, t.me/name, or name; "
                "5-32 chars, letters/digits/underscore, starts with a letter."
            ),
        )


def _is_russian(text: str) -> bool:
    return bool(re.search(r"[\u0400-\u04FF]", text))


def _normalize_taxonomy_label(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("&", "and").split())


def _taxonomy_quality_snapshot() -> dict:
    expected_domains = set(TAXONOMY_DOMAINS.keys())

    domain_rows = db.run_query("MATCH (d:TopicDomain) RETURN d.name AS name ORDER BY name")
    domain_names = [str(row.get("name") or "").strip() for row in domain_rows if row.get("name")]

    normalized_groups: dict[str, list[str]] = {}
    for domain_name in domain_names:
        key = _normalize_taxonomy_label(domain_name)
        normalized_groups.setdefault(key, []).append(domain_name)
    duplicate_domain_groups = [group for group in normalized_groups.values() if len(group) > 1]

    unknown_domains = [
        name for name in domain_names
        if name not in expected_domains and name != "General"
    ]

    general_stats = db.run_single(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(c:TopicCategory)-[:IN_DOMAIN]->(d:TopicDomain)
        WITH count(DISTINCT t) AS total,
             count(DISTINCT CASE WHEN c.name='General' OR d.name='General' THEN t END) AS general_topics
        RETURN total, general_topics,
               CASE WHEN total = 0 THEN 0.0 ELSE toFloat(general_topics) / toFloat(total) END AS ratio
        """
    ) or {}

    general_mention_stats = db.run_single(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(c:TopicCategory)-[:IN_DOMAIN]->(d:TopicDomain)
        OPTIONAL MATCH (n)-[:TAGGED]->(t)
        WITH t,c,d,count(n) AS mentions
        RETURN sum(mentions) AS total_mentions,
               sum(CASE WHEN c.name='General' OR d.name='General' THEN mentions ELSE 0 END) AS general_mentions,
               CASE
                 WHEN sum(mentions)=0 THEN 0.0
                 ELSE toFloat(sum(CASE WHEN c.name='General' OR d.name='General' THEN mentions ELSE 0 END)) / toFloat(sum(mentions))
               END AS ratio
        """
    ) or {}

    multi_rows = db.run_query(
        """
        MATCH (t:Topic)-[:BELONGS_TO_CATEGORY]->(c:TopicCategory)-[:IN_DOMAIN]->(d:TopicDomain)
        WITH t, collect(DISTINCT c.name) AS categories, collect(DISTINCT d.name) AS domains
        WHERE size(categories) > 1 OR size(domains) > 1
        RETURN t.name AS topic, categories, domains
        ORDER BY t.name
        LIMIT 200
        """
    )

    proposed_rows = db.run_single(
        """
        MATCH (t:Topic)
        RETURN count(t) AS total,
               sum(CASE WHEN coalesce(t.proposed,false) THEN 1 ELSE 0 END) AS proposed
        """
    ) or {}

    proposed_mention_stats = db.run_single(
        """
        MATCH (t:Topic)
        OPTIONAL MATCH (n)-[:TAGGED]->(t)
        WITH t, count(n) AS mentions
        RETURN sum(mentions) AS total_mentions,
               sum(CASE WHEN coalesce(t.proposed,false) THEN mentions ELSE 0 END) AS proposed_mentions,
               CASE
                 WHEN sum(mentions)=0 THEN 0.0
                 ELSE toFloat(sum(CASE WHEN coalesce(t.proposed,false) THEN mentions ELSE 0 END)) / toFloat(sum(mentions))
               END AS ratio
        """
    ) or {}

    writer = get_supabase_writer()
    pending_proposals = len(writer.list_topic_proposals(status="pending", limit=500))
    visible_emerging_proposals = len(writer.list_emerging_topic_candidates(status="pending", limit=500))

    total_topics = int(general_stats.get("total") or 0)
    general_topics = int(general_stats.get("general_topics") or 0)
    general_ratio = float(general_stats.get("ratio") or 0.0)
    general_mentions = int(general_mention_stats.get("general_mentions") or 0)
    total_mentions = int(general_mention_stats.get("total_mentions") or 0)
    general_ratio_mentions = float(general_mention_stats.get("ratio") or 0.0)

    proposed_total = int(proposed_rows.get("proposed") or 0)
    proposed_ratio = (proposed_total / total_topics) if total_topics else 0.0
    proposed_mentions = int(proposed_mention_stats.get("proposed_mentions") or 0)
    proposed_total_mentions = int(proposed_mention_stats.get("total_mentions") or 0)
    proposed_ratio_mentions = float(proposed_mention_stats.get("ratio") or 0.0)

    gates = {
        "domain_duplicates_zero": len(duplicate_domain_groups) == 0,
        "unknown_domains_zero": len(unknown_domains) == 0,
        "general_share_le_30pct": general_ratio_mentions <= 0.30,
        "multi_mapped_topics_le_5": len(multi_rows) <= 5,
        "proposed_share_le_20pct": proposed_ratio_mentions <= 0.20,
    }
    advisory = {
        "general_share_topics_le_35pct": general_ratio <= 0.35,
    }

    return {
        "summary": {
            "domains_total": len(domain_names),
            "topics_total": total_topics,
            "general_topics": general_topics,
            "general_ratio": round(general_ratio, 4),
            "general_mentions": general_mentions,
            "general_ratio_mentions": round(general_ratio_mentions, 4),
            "total_mentions": total_mentions,
            "proposed_topics": proposed_total,
            "proposed_ratio": round(proposed_ratio, 4),
            "proposed_mentions": proposed_mentions,
            "proposed_ratio_mentions": round(proposed_ratio_mentions, 4),
            "proposed_total_mentions": proposed_total_mentions,
            "multi_mapped_topics": len(multi_rows),
            "pending_topic_proposals": pending_proposals,
            "visible_emerging_proposals": visible_emerging_proposals,
        },
        "issues": {
            "duplicate_domain_groups": duplicate_domain_groups,
            "unknown_domains": unknown_domains,
            "multi_mapped_samples": multi_rows[:30],
        },
        "gates": gates,
        "advisory": advisory,
        "ready_for_phase2_signoff": all(gates.values()),
    }


def _build_ai_answer(query: str, dashboard: dict) -> str:
    q = query.lower()
    ru = _is_russian(query)

    trending = dashboard.get("trendingTopics") or []
    channels = dashboard.get("communityChannels") or []
    key_voices = dashboard.get("keyVoices") or []
    urgency = dashboard.get("urgencySignals") or []
    weekly = dashboard.get("weeklyShifts") or []
    community = dashboard.get("communityHealth") or {}

    top_topics = [t.get("name") for t in trending[:3] if isinstance(t, dict)]
    top_topics = [t for t in top_topics if t]
    top_channel = channels[0] if channels else {}
    top_voice = key_voices[0] if key_voices else {}
    week = weekly[0] if weekly else {}
    top_urgency = sorted(
        [u for u in urgency if isinstance(u, dict)],
        key=lambda u: int(u.get("urgentUsers", 0)),
        reverse=True,
    )[:3]

    def _weekly_delta(metric_key: str, *legacy_metric_names: str) -> str:
        for item in weekly:
            if not isinstance(item, dict):
                continue
            key = str(item.get("metricKey") or "").strip().lower()
            label = str(item.get("metric") or "").strip().lower()
            if key != metric_key and label not in legacy_metric_names:
                continue

            try:
                current = float(item.get("current") or 0)
                previous = float(item.get("previous") or 0)
            except Exception:
                return "N/A"

            if previous <= 0:
                return "new" if current > 0 else "0.0"
            return f"{round(100.0 * (current - previous) / previous, 1)}"

        legacy_value = week.get("postChange" if metric_key == "posts" else "commentChange")
        return str(legacy_value if legacy_value is not None else "N/A")

    def _format_delta(value: str) -> str:
        return value if value in {"N/A", "new"} else f"{value}%"

    post_delta = _weekly_delta("posts", "posts", "post")
    comment_delta = _weekly_delta("comments", "comments", "comment")

    if ru:
        if "жил" in q or "аренд" in q or "housing" in q:
            return (
                "**Сводка по жилью (живой срез дашборда)**\n\n"
                f"- Основные связанные темы: {', '.join(top_topics) if top_topics else 'недостаточно данных'}\n"
                f"- Самый активный канал: {top_channel.get('title', 'N/A')}\n"
                "- Рекомендация: закрепить FAQ по аренде, депозиту и проверке договоров"
            )
        if "голос" in q or "influenc" in q or "влият" in q:
            return (
                "**Ключевые голоса сообщества**\n\n"
                f"- Самый активный комментатор: user {top_voice.get('userId', 'N/A')}\n"
                f"- Роль: {top_voice.get('role', 'N/A')}\n"
                "- Рекомендация: вовлекать заметных комментаторов в модерацию и weekly digest"
            )
        if "сроч" in q or "urgent" in q or "криз" in q or "problem" in q:
            lines = [f"- {u.get('topic', 'N/A')}: {u.get('urgentUsers', 0)}" for u in top_urgency]
            lines_block = "\n".join(lines) if lines else "- Недостаточно сигналов срочности"
            return (
                "**Темы, требующие срочного внимания**\n\n"
                f"{lines_block}\n"
                "- Рекомендация: запустить быстрые модераторские ответы и закрепить FAQ по этим темам"
            )
        return (
            "**Краткий AI-обзор сообщества**\n\n"
            f"- Health score: {community.get('score', 'N/A')}\n"
            f"- Всего пользователей: {community.get('totalUsers', 'N/A')}\n"
            f"- Активные за 7 дней: {community.get('activeUsers', 'N/A')}\n"
            f"- Изменение постов за неделю: {_format_delta(post_delta)}\n"
            f"- Изменение комментариев за неделю: {_format_delta(comment_delta)}"
        )

    if "hous" in q or "rent" in q:
        return (
            "**Housing Snapshot (live dashboard window)**\n\n"
            f"- Leading related topics: {', '.join(top_topics) if top_topics else 'insufficient data'}\n"
            f"- Most active channel: {top_channel.get('title', 'N/A')}\n"
            "- Recommendation: pin a renter FAQ (contracts, deposits, neighborhood trade-offs)."
        )
    if "voice" in q or "influenc" in q:
        return (
            "**Key Community Voices**\n\n"
            f"- Most active commenter: user {top_voice.get('userId', 'N/A')}\n"
            f"- Role: {top_voice.get('role', 'N/A')}\n"
            "- Recommendation: involve visible commenters in moderation and weekly roundups."
        )
    if "urgent" in q or "problem" in q or "attention" in q:
        lines = [f"- {u.get('topic', 'N/A')}: {u.get('urgentUsers', 0)}" for u in top_urgency]
        lines_block = "\n".join(lines) if lines else "- Not enough urgency signals"
        return (
            "**Topics Requiring Urgent Attention**\n\n"
            f"{lines_block}\n"
            "- Recommendation: add rapid moderator response playbooks and pin practical guidance."
        )

    return (
        "**Community AI Brief**\n\n"
        f"- Health score: {community.get('score', 'N/A')}\n"
        f"- Total users: {community.get('totalUsers', 'N/A')}\n"
        f"- Active users (7d): {community.get('activeUsers', 'N/A')}\n"
        f"- Weekly post delta: {_format_delta(post_delta)}\n"
        f"- Weekly comment delta: {_format_delta(comment_delta)}"
    )


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/readyz")
async def readyz():
    """Cheap readiness probe that avoids database work."""
    if runtime_is_draining():
        raise HTTPException(status_code=503, detail="draining")
    return {"status": "ready", "role": APP_ROLE}


@app.get("/api/health")
async def health():
    """Health check."""
    try:
        db.run_single("RETURN 1 AS ok")
        return {"status": "ok", "neo4j": "connected"}
    except Exception as e:
        return {"status": "degraded", "neo4j": str(e)}


@app.get("/api/dashboard", dependencies=[Depends(require_analytics_access)])
async def dashboard(
    request: Request,
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """
    Full dashboard data — matches the frontend's AppData interface.
    Cached for 15 minutes by default. Call POST /api/cache/clear to refresh.
    """
    try:
        query_started_at = time.perf_counter()
        response = await run_request(lambda: _build_dashboard_response_payload(from_date, to_date))
        _record_query_timing(
            request,
            query_started_at,
            cache_status=str(response.get("meta", {}).get("cacheStatus") or ""),
        )
        request.state.dashboard_meta = response["meta"]
        return _dashboard_response(response)
    except TimeoutError as e:
        logger.warning(f"Dashboard endpoint warming timeout: {e}")
        raise HTTPException(
            status_code=503,
            detail="Dashboard is still warming this date range. Please retry in a few seconds.",
        )
    except Exception as e:
        logger.error(f"Dashboard endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ai/query", dependencies=[Depends(require_analytics_access)])
async def ai_query(request: AIQueryRequest):
    """Lightweight AI endpoint backed by the live dashboard snapshot."""
    try:
        freshness = _dashboard_freshness_snapshot(force_refresh=False)
        dashboard_data = get_dashboard_data(_default_dashboard_context(freshness))
        answer = _build_ai_answer(request.query, dashboard_data)
        return {
            "query": request.query,
            "answer": answer,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"AI query endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topics", dependencies=[Depends(require_analytics_access)])
async def topics(
    request: Request,
    page: int = Query(0, ge=0),
    size: int = Query(500, ge=1, le=1000),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Topics detail page — paginated."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        query_started_at = time.perf_counter()
        payload = await run_request(lambda: get_topics_page(page, size, ctx))
        _record_query_timing(request, query_started_at)
        return payload
    except DetailRefreshUnavailableError as e:
        logger.warning(f"Topics endpoint degraded: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Topics endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topics/detail", dependencies=[Depends(require_analytics_access)])
async def topic_detail(
    request: Request,
    topic: str = Query(..., min_length=1),
    category: Optional[str] = Query(default=None),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Single topic detail payload with evidence and trend series."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        query_started_at = time.perf_counter()
        payload = await run_request(lambda: get_topic_detail(topic, category, ctx))
        _record_query_timing(request, query_started_at)
        if payload is None:
            raise HTTPException(status_code=404, detail="Topic not found for the selected window.")
        overview = topic_overviews.get_topic_overview(
            str(payload.get("sourceTopic") or payload.get("name") or topic),
            str(payload.get("category") or category or ""),
        )
        payload = {**payload, "overview": overview}
        return payload
    except HTTPException:
        raise
    except DetailRefreshUnavailableError as e:
        logger.warning(f"Topic detail endpoint degraded: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Topic detail endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/topics/evidence", dependencies=[Depends(require_analytics_access)])
async def topic_evidence(
    request: Request,
    topic: str = Query(..., min_length=1),
    category: Optional[str] = Query(default=None),
    view: str = Query(default="all"),
    page: int = Query(0, ge=0),
    size: int = Query(20, ge=1, le=50),
    focus_id: Optional[str] = Query(default=None, alias="focusId"),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Paginated topic evidence feed for the selected window."""
    try:
        normalized_view = (view or "all").strip().lower()
        if normalized_view not in {"all", "questions"}:
            raise HTTPException(status_code=422, detail="view must be 'all' or 'questions'.")
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        query_started_at = time.perf_counter()
        payload = await run_request(
            lambda: get_topic_evidence_page(topic, category, normalized_view, page, size, focus_id, ctx),
        )
        _record_query_timing(request, query_started_at)
        if payload is None:
            raise HTTPException(status_code=404, detail="Topic not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except DetailRefreshUnavailableError as e:
        logger.warning(f"Topic evidence endpoint degraded: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Topic evidence endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/channels", dependencies=[Depends(require_analytics_access)])
async def channels(
    request: Request,
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Channels detail page."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        query_started_at = time.perf_counter()
        payload = await run_request(lambda: get_channels_page(ctx))
        _record_query_timing(request, query_started_at)
        return payload
    except DetailRefreshUnavailableError as e:
        logger.warning(f"Channels endpoint degraded: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/channels/detail", dependencies=[Depends(require_analytics_access)])
async def channel_detail(
    request: Request,
    channel: str = Query(..., min_length=1),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Single channel detail payload with recent posts and distributions."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        query_started_at = time.perf_counter()
        payload = await run_request(lambda: get_channel_detail(channel, ctx))
        _record_query_timing(request, query_started_at)
        if payload is None:
            raise HTTPException(status_code=404, detail="Channel not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except DetailRefreshUnavailableError as e:
        logger.warning(f"Channel detail endpoint degraded: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Channel detail endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/channels/posts", dependencies=[Depends(require_analytics_access)])
async def channel_posts(
    request: Request,
    channel: str = Query(..., min_length=1),
    page: int = Query(0, ge=0),
    size: int = Query(20, ge=1, le=50),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Paginated recent posts feed for a selected channel."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        query_started_at = time.perf_counter()
        payload = await run_request(lambda: get_channel_posts_page(channel, page, size, ctx))
        _record_query_timing(request, query_started_at)
        if payload is None:
            raise HTTPException(status_code=404, detail="Channel not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except DetailRefreshUnavailableError as e:
        logger.warning(f"Channel posts endpoint degraded: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Channel posts endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/graph", dependencies=[Depends(require_analytics_access)])
async def graph_data(payload: GraphRequest):
    """Graph dataset for /graph page (server-side Neo4j)."""
    try:
        filters = payload.model_dump(exclude_none=True)
        def _build_graph() -> dict[str, Any]:
            graph = graph_dashboard.get_graph_data(filters)
            freshness = get_freshness_snapshot(
                get_supabase_writer(),
                scheduler_status=get_current_scraper_scheduler_status(),
            )
            if not isinstance(graph, dict):
                return graph
            meta_existing = graph.get("meta")
            meta_dict = dict(meta_existing) if isinstance(meta_existing, dict) else {}
            meta_dict["freshness"] = {
                "status": freshness.get("health", {}).get("status"),
                "score": freshness.get("health", {}).get("score"),
                "generatedAt": freshness.get("generated_at"),
                "lastScrapeAt": freshness.get("pipeline", {}).get("scrape", {}).get("last_scrape_at"),
                "lastProcessAt": freshness.get("pipeline", {}).get("process", {}).get("last_process_at"),
                "lastGraphSyncAt": freshness.get("pipeline", {}).get("sync", {}).get("last_graph_sync_at"),
                "syncEstimated": freshness.get("pipeline", {}).get("sync", {}).get("estimated"),
                "unsyncedPosts": freshness.get("backlog", {}).get("unsynced_posts"),
                "analyticsWindowDays": freshness.get("drift", {}).get("analytics_window_days"),
                "latestPostDeltaMinutes": freshness.get("drift", {}).get("latest_post_delta_minutes"),
            }
            graph["meta"] = meta_dict
            return graph

        return await run_request(_build_graph)
    except Exception as e:
        logger.error(f"Graph data endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/node-details", dependencies=[Depends(require_analytics_access)])
async def node_details(
    nodeId: str = Query(...),
    nodeType: str = Query(...),
    timeframe: Optional[str] = Query(default="Last 7 Days"),
    channels: Optional[str] = Query(default=None),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    sentiments: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    signalFocus: Optional[str] = Query(default=None),
):
    """Detailed panel data for a graph node."""
    try:
        channel_filters = [c.strip() for c in (channels or "").split(",") if c.strip()]
        sentiment_filters = [item.strip() for item in (sentiments or "").split(",") if item.strip()]
        details = await run_request(
            lambda: graph_dashboard.get_node_details(
                nodeId,
                nodeType,
                timeframe=timeframe,
                channels=channel_filters,
                from_date=from_date,
                to_date=to_date,
                sentiments=sentiment_filters,
                category=category,
                signal_focus=signalFocus,
            )
        )
        if not details:
            raise HTTPException(status_code=404, detail="Node not found")
        return details
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Node details endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search", dependencies=[Depends(require_analytics_access)])
async def search_graph(query: str = Query("", min_length=0, max_length=200), limit: int = Query(20, ge=1, le=100)):
    """Graph search across channels/topics/intents."""
    try:
        if not (query or "").strip():
            return []
        return await run_request(lambda: graph_dashboard.search_graph(query, limit))
    except Exception as e:
        logger.error(f"Graph search endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trending-topics", dependencies=[Depends(require_analytics_access)])
async def trending_topics(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Top trending topics for graph filters."""
    try:
        return await run_request(lambda: graph_dashboard.get_trending_topics(limit, timeframe))
    except Exception as e:
        logger.error(f"Trending topics endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-channels", dependencies=[Depends(require_analytics_access)])
async def top_channels(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Top channels by post activity (graph context)."""
    try:
        return await run_request(lambda: graph_dashboard.get_top_channels(limit, timeframe))
    except Exception as e:
        logger.error(f"Top channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/all-channels", dependencies=[Depends(require_analytics_access)])
async def all_channels_graph():
    """All channels list for graph filters."""
    try:
        return await run_request(graph_dashboard.get_all_channels)
    except Exception as e:
        logger.error(f"All channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-brands", dependencies=[Depends(require_analytics_access)])
async def top_brands_compat(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Compatibility endpoint: returns top channels in legacy shape."""
    try:
        return await run_request(lambda: graph_dashboard.get_top_channels(limit, timeframe))
    except Exception as e:
        logger.error(f"Top brands compatibility endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/all-brands", dependencies=[Depends(require_analytics_access)])
async def all_brands_compat():
    """Compatibility endpoint: returns all channels in legacy shape."""
    try:
        return await run_request(graph_dashboard.get_all_channels)
    except Exception as e:
        logger.error(f"All brands compatibility endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sentiment-distribution", dependencies=[Depends(require_analytics_access)])
async def sentiment_distribution(timeframe: str = Query("Last 7 Days")):
    """Sentiment distribution for graph side panels/filters."""
    try:
        return await run_request(lambda: graph_dashboard.get_sentiment_distribution(timeframe))
    except Exception as e:
        logger.error(f"Sentiment distribution endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/graph-insights", dependencies=[Depends(require_analytics_access)])
async def graph_insights(timeframe: str = Query("Last 7 Days")):
    """Narrative summary for graph context."""
    try:
        return await run_request(lambda: graph_dashboard.get_graph_insights(timeframe))
    except Exception as e:
        logger.error(f"Graph insights endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/insights/cards", dependencies=[Depends(require_analytics_access)])
async def insight_cards(payload: InsightCardsRequest):
    """Structured insight cards for analyst/executive surfaces."""
    try:
        audience = (payload.audience or "analyst").strip().lower()
        if audience not in {"analyst", "executive"}:
            audience = "analyst"
        return await run_request(lambda: insights.get_insight_cards(payload.filters or {}, audience))
    except Exception as e:
        logger.error(f"Insight cards endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/config")
async def get_admin_config():
    """Return merged Admin config with defaults and runtime overrides."""
    try:
        return _admin_config_response()
    except Exception as e:
        logger.error(f"Get admin config error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/admin/config")
async def update_admin_config(payload: AdminConfigPatchRequest):
    """Persist lightweight Admin page config in runtime storage."""
    try:
        current = _load_admin_config()
        widgets = _validate_admin_widgets(payload.widgets)
        prompts = _validate_admin_prompts(payload.prompts)
        runtime = _validate_admin_runtime(payload.runtime)

        if not widgets and not prompts and not runtime:
            raise HTTPException(status_code=400, detail="No update fields provided")

        next_config = {
            "widgets": {**current["widgets"], **widgets},
            "prompts": {**current["prompts"], **prompts},
            "runtime": {**current["runtime"], **runtime},
        }

        if not save_admin_config_raw(next_config):
            raise HTTPException(
                status_code=500,
                detail=get_admin_config_runtime_warning() or "Failed to persist admin config",
            )
        return _admin_config_response()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update admin config error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sources/channels")
async def list_channel_sources():
    """List configured Telegram channel sources from Supabase."""
    try:
        items = get_supabase_writer().list_channels()
        return {"count": len(items), "items": items}
    except Exception as e:
        logger.error(f"List source channels error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/sources/channels")
async def create_channel_source(payload: ChannelSourceCreateRequest):
    """Create or reactivate a Telegram channel source for scheduler pickup."""
    try:
        normalized_handle = _normalize_channel_username(payload.channel_username)
        _validate_channel_username(normalized_handle)
        canonical_username = _canonical_channel_username(normalized_handle)

        provided_title = (payload.channel_title or "").strip()
        channel_title = provided_title or normalized_handle

        writer = get_supabase_writer()
        existing = writer.get_channel_by_handle(normalized_handle)
        if existing:
            update_payload = {
                "channel_username": canonical_username,
                "scrape_depth_days": payload.scrape_depth_days,
                "scrape_comments": payload.scrape_comments,
            }
            existing_title = (existing.get("channel_title") or "").strip()
            if provided_title:
                update_payload["channel_title"] = provided_title
            elif (not existing_title) or (existing_title.lower() == canonical_username.lower()):
                update_payload["channel_title"] = normalized_handle
            action = "exists"
            if not existing.get("is_active", False):
                update_payload["is_active"] = True
                action = "reactivated"

            updated = writer.update_channel(existing["id"], update_payload)
            if updated:
                await _try_enrich_channel_metadata(
                    updated["id"],
                    updated.get("channel_username") or canonical_username,
                    updated.get("channel_title"),
                )
                updated = writer.get_channel_by_id(updated["id"]) or updated
            return {"action": action, "item": updated}

        created = writer.create_channel(
            {
                "channel_username": canonical_username,
                "channel_title": channel_title,
                "is_active": True,
                "scrape_depth_days": payload.scrape_depth_days,
                "scrape_comments": payload.scrape_comments,
            }
        )
        await _try_enrich_channel_metadata(
            created["id"],
            created.get("channel_username") or canonical_username,
            created.get("channel_title"),
        )
        created = writer.get_channel_by_id(created["id"]) or created
        return {"action": "created", "item": created}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create source channel error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/sources/channels/{channel_id}")
async def update_channel_source(channel_id: str, payload: ChannelSourceUpdateRequest):
    """Update source settings (active flag and scrape settings)."""
    try:
        writer = get_supabase_writer()
        existing = writer.get_channel_by_id(channel_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Channel source not found")

        update_payload: dict = {}
        if payload.is_active is not None:
            update_payload["is_active"] = payload.is_active
        if payload.scrape_depth_days is not None:
            update_payload["scrape_depth_days"] = payload.scrape_depth_days
        if payload.scrape_comments is not None:
            update_payload["scrape_comments"] = payload.scrape_comments

        if not update_payload:
            raise HTTPException(status_code=400, detail="No update fields provided")

        updated = writer.update_channel(channel_id, update_payload)
        if updated and updated.get("is_active"):
            await _try_enrich_channel_metadata(
                updated["id"],
                updated.get("channel_username") or "",
                updated.get("channel_title"),
            )
            updated = writer.get_channel_by_id(updated["id"]) or updated
        return {"item": updated}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update source channel error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/scraper/scheduler")
async def get_scraper_scheduler_status():
    """Current scraper scheduler runtime status."""
    return get_scraper_scheduler().status()


@app.get("/api/freshness", dependencies=[Depends(require_analytics_access)])
async def freshness_snapshot(force: bool = Query(False)):
    """Pipeline freshness/truth snapshot with backlog and Supabase↔Neo4j drift."""
    try:
        if force:
            return await run_request(
                lambda: get_freshness_snapshot(
                    get_supabase_writer(),
                    scheduler_status=get_current_scraper_scheduler_status(),
                    force_refresh=True,
                )
            )

        resolution = _cached_freshness_resolution(allow_live=False)
        snapshot = resolution.get("snapshot")
        source = str(resolution.get("source") or "")
        if isinstance(snapshot, dict) and snapshot:
            if source in {"memory_stale", "persisted_stale"}:
                _ensure_background_freshness_refresh()
            return snapshot

        snapshot = await run_request(
            lambda: get_freshness_snapshot(
                get_supabase_writer(),
                scheduler_status=get_current_scraper_scheduler_status(),
                force_refresh=False,
            )
        )
        _persist_freshness_snapshot_async(snapshot)
        return snapshot
    except Exception as e:
        logger.error(f"Freshness endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/quality/taxonomy", dependencies=[Depends(require_analytics_access)])
async def taxonomy_quality_snapshot():
    """Enterprise taxonomy quality snapshot with sign-off gates."""
    try:
        snapshot = await run_request(_taxonomy_quality_snapshot)
        snapshot["generated_at"] = datetime.now(timezone.utc).isoformat()
        return snapshot
    except Exception as e:
        logger.error(f"Taxonomy quality endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/quality/trending-widget", dependencies=[Depends(require_analytics_access)])
async def trending_widget_quality_snapshot(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """QA snapshot for the Trending widget read model and evidence integrity."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        def _build_snapshot() -> dict[str, Any]:
            writer = get_supabase_writer()
            proposal_rows = writer.list_topic_proposals(status="pending", limit=500)
            category_keys = {
                "".join(part for part in str(category).lower().replace("&", " and ").replace("-", " ").split() if part != "and")
                for categories in TAXONOMY_DOMAINS.values()
                for category in categories.keys()
            }
            domain_keys = {
                "".join(part for part in str(domain).lower().replace("&", " and ").replace("-", " ").split() if part != "and")
                for domain in TAXONOMY_DOMAINS.keys()
            }
            proposal_summary = {
                "pending": len(proposal_rows),
                "visibleEmergingCandidates": len(writer.list_emerging_topic_candidates(status="pending", limit=500)),
                "structureLabelLike": 0,
                "rawProposedOnly": 0,
            }
            for row in proposal_rows:
                topic_name = str(row.get("topic_name") or "").strip()
                normalized = topic_name.lower().replace("&", " and ")
                compact = "".join(ch if ch.isalnum() else " " for ch in normalized)
                proposal_key = "".join(part for part in compact.split() if part and part != "and")
                if not proposal_key:
                    proposal_summary["structureLabelLike"] += 1
                    continue
                if proposal_key in category_keys or proposal_key in domain_keys:
                    proposal_summary["structureLabelLike"] += 1
                    continue
                proposal_summary["rawProposedOnly"] += 1

            return {
                "generatedAt": datetime.now(timezone.utc).isoformat(),
                "window": {
                    "from": ctx.from_date.isoformat(),
                    "to": ctx.to_date.isoformat(),
                    "days": ctx.days,
                },
                "runtime": _active_ai_runtime_summary(),
                "widget": pulse.get_trending_widget_diagnostics(ctx),
                "proposalQueue": proposal_summary,
            }

        return await run_request(_build_snapshot)
    except Exception as e:
        logger.error(f"Trending widget quality endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/start")
async def start_scraper_scheduler():
    """Start recurring scraper schedule using persisted interval."""
    try:
        return await get_scraper_scheduler().start()
    except Exception as e:
        logger.error(f"Start scraper scheduler error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/stop")
async def stop_scraper_scheduler():
    """Stop recurring scraper schedule."""
    try:
        return await get_scraper_scheduler().stop()
    except Exception as e:
        logger.error(f"Stop scraper scheduler error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/scraper/scheduler")
async def update_scraper_scheduler(payload: ScraperSchedulerUpdateRequest):
    """Update scraper scheduler interval in minutes."""
    try:
        return await get_scraper_scheduler().set_interval(payload.interval_minutes)
    except Exception as e:
        logger.error(f"Update scraper scheduler error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/run-once")
async def run_scraper_once():
    """Trigger one immediate scrape cycle."""
    try:
        return await get_scraper_scheduler().run_once()
    except Exception as e:
        logger.error(f"Run-once scraper error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/catchup-once")
async def run_scraper_catchup_once():
    """Trigger one immediate processing/sync-heavy catch-up cycle (no scraping)."""
    try:
        return await get_scraper_scheduler().run_catchup_once()
    except Exception as e:
        logger.error(f"Catchup-once scraper error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ai/failures")
async def list_ai_failures(
    dead_letter_only: bool = Query(True),
    scope_type: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    """List AI processing failure scopes for operator triage."""
    try:
        items = get_supabase_writer().list_processing_failures(
            dead_letter_only=dead_letter_only,
            scope_type=scope_type,
            limit=limit,
        )
        return {
            "count": len(items),
            "items": items,
        }
    except Exception as e:
        logger.error(f"List AI failures endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ai/failures/retry")
async def retry_ai_failures(payload: FailureRetryRequest):
    """Unlock selected AI failure scopes for immediate retry."""
    scope_type = (payload.scope_type or "").strip().lower()
    if scope_type not in {"comment_group", "post"}:
        raise HTTPException(status_code=400, detail="scope_type must be 'comment_group' or 'post'")

    try:
        retried = get_supabase_writer().retry_processing_failures(
            scope_type=scope_type,
            scope_keys=payload.scope_keys,
        )
        return {
            "retried": int(retried),
            "scope_type": scope_type,
        }
    except Exception as e:
        logger.error(f"Retry AI failures endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/taxonomy/proposals")
async def list_taxonomy_proposals(
    status: str = Query("pending"),
    visibility_state: Optional[str] = Query(None),
    visible_only: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
):
    """List proposed topics for review queue triage."""
    try:
        items = get_supabase_writer().list_topic_proposals(status=status, limit=limit)
        normalized_visibility = (visibility_state or "").strip().lower()

        if visible_only:
            filtered: list[dict] = []
            for item in items:
                state = str(item.get("visibility_state") or "").strip().lower()
                if bool(item.get("visibility_eligible")) or state == "emerging_visible" or int(item.get("proposed_count") or 0) >= 3:
                    filtered.append(item)
            items = filtered

        if normalized_visibility:
            items = [
                item for item in items
                if str(item.get("visibility_state") or "").strip().lower() == normalized_visibility
            ]

        return {
            "count": len(items),
            "status": status,
            "visibility_state": visibility_state,
            "visible_only": visible_only,
            "items": items,
        }
    except Exception as e:
        logger.error(f"List taxonomy proposals endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/taxonomy/trending-new")
async def list_taxonomy_trending_new(
    status: str = Query("pending"),
    limit: int = Query(30, ge=1, le=200),
):
    """List emerging proposed topics eligible for frontend visibility."""
    try:
        items = get_supabase_writer().list_emerging_topic_candidates(status=status, limit=limit)
        return {
            "count": len(items),
            "status": status,
            "items": items,
        }
    except Exception as e:
        logger.error(f"List taxonomy trending-new endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/taxonomy/proposals/review")
async def review_taxonomy_proposal(payload: TopicProposalReviewRequest):
    """Approve or reject a proposed topic, with optional alias promotions."""
    decision = (payload.decision or "").strip().lower()
    if decision not in {"approve", "reject"}:
        raise HTTPException(status_code=400, detail="decision must be 'approve' or 'reject'")

    try:
        item = get_supabase_writer().review_topic_proposal(
            topic_name=payload.topic_name,
            decision=decision,
            canonical_topic=payload.canonical_topic,
            aliases=payload.aliases,
            notes=payload.notes,
            reviewed_by=payload.reviewed_by,
        )
        if not item:
            raise HTTPException(status_code=404, detail="Topic proposal not found")
        return {
            "success": True,
            "item": item,
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Review taxonomy proposal endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/taxonomy/promotions")
async def list_taxonomy_promotions(
    active_only: bool = Query(True),
    limit: int = Query(200, ge=1, le=500),
):
    """List runtime topic promotion aliases."""
    try:
        items = get_supabase_writer().list_topic_promotions(limit=limit, active_only=active_only)
        return {
            "count": len(items),
            "active_only": active_only,
            "items": items,
        }
    except Exception as e:
        logger.error(f"List taxonomy promotions endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/taxonomy/promotions/reload")
async def reload_taxonomy_promotions():
    """Reload runtime alias map from approved promotions table."""
    try:
        loaded = get_supabase_writer().refresh_runtime_topic_aliases()
        return {
            "loaded_aliases": int(loaded),
        }
    except Exception as e:
        logger.error(f"Reload taxonomy promotions endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/audience", dependencies=[Depends(require_analytics_access)])
async def audience(
    request: Request,
    page: int = Query(0, ge=0),
    size: int = Query(500, ge=1, le=1000),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Audience detail page — paginated."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        query_started_at = time.perf_counter()
        payload = await run_request(lambda: get_audience_page(page, size, ctx))
        _record_query_timing(request, query_started_at)
        return payload
    except DetailRefreshUnavailableError as e:
        logger.warning(f"Audience endpoint degraded: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Audience endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/audience/detail", dependencies=[Depends(require_analytics_access)])
async def audience_detail(
    request: Request,
    user_id: str = Query(..., alias="userId", min_length=1),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Single audience-member detail payload."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        query_started_at = time.perf_counter()
        payload = await run_request(lambda: get_audience_detail(user_id, ctx))
        _record_query_timing(request, query_started_at)
        if payload is None:
            raise HTTPException(status_code=404, detail="Audience member not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except DetailRefreshUnavailableError as e:
        logger.warning(f"Audience detail endpoint degraded: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Audience detail endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/audience/messages", dependencies=[Depends(require_analytics_access)])
async def audience_messages(
    request: Request,
    user_id: str = Query(..., alias="userId", min_length=1),
    page: int = Query(0, ge=0),
    size: int = Query(20, ge=1, le=50),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Paginated recent messages feed for a selected audience member."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        query_started_at = time.perf_counter()
        payload = await run_request(lambda: get_audience_messages_page(user_id, page, size, ctx))
        _record_query_timing(request, query_started_at)
        if payload is None:
            raise HTTPException(status_code=404, detail="Audience member not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except DetailRefreshUnavailableError as e:
        logger.warning(f"Audience messages endpoint degraded: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Audience messages endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cache/clear")
async def clear_cache():
    """Invalidate dashboard/freshness caches in memory and persisted storage."""
    invalidate_cache()
    clear_cached_freshness_snapshot()
    deleted_runtime_files = _clear_persisted_dashboard_cache()
    question_briefs.invalidate_question_briefs_cache()
    behavioral_briefs.invalidate_behavioral_briefs_cache()
    opportunity_briefs.invalidate_opportunity_briefs_cache()
    topic_overviews.invalidate_topic_overviews_cache()
    return {
        "success": True,
        "message": "Cache cleared",
        "persistedRuntimeFilesDeleted": deleted_runtime_files,
    }


@app.post("/api/question-briefs/debug/refresh")
async def debug_refresh_question_briefs():
    """Force-refresh question cards and return stage diagnostics."""
    try:
        diagnostics = await run_background(lambda: question_briefs.refresh_question_briefs_with_diagnostics(force=True))
        return {
            "success": True,
            "cardsProduced": diagnostics.get("cardsProduced", 0),
            "firstRejectionBucket": diagnostics.get("firstRejectionBucket", ""),
            "diagnostics": diagnostics,
        }
    except Exception as e:
        logger.error(f"Question briefs debug refresh endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/behavioral-briefs/debug/refresh")
async def debug_refresh_behavioral_briefs():
    """Force-refresh behavioral cards and return stage diagnostics."""
    try:
        diagnostics = await run_background(
            lambda: behavioral_briefs.refresh_behavioral_briefs_with_diagnostics(force=True),
        )
        return {
            "success": True,
            "problemCardsProduced": diagnostics.get("stages", {}).get("problemCards", 0),
            "serviceCardsProduced": diagnostics.get("stages", {}).get("serviceCards", 0),
            "urgencyCardsProduced": diagnostics.get("stages", {}).get("urgencyCards", 0),
            "diagnostics": diagnostics,
        }
    except Exception as e:
        logger.error(f"Behavioral briefs debug refresh endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/opportunity-briefs/debug/refresh")
async def debug_refresh_opportunity_briefs():
    """Force-refresh opportunity cards and return stage diagnostics."""
    try:
        diagnostics = await run_background(
            lambda: opportunity_briefs.refresh_opportunity_briefs_with_diagnostics(force=True),
        )
        return {
            "success": True,
            "cardsProduced": diagnostics.get("cardsProduced", 0),
            "firstRejectionBucket": diagnostics.get("firstRejectionBucket", ""),
            "diagnostics": diagnostics,
        }
    except Exception as e:
        logger.error(f"Opportunity briefs debug refresh endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/topic-overviews/debug/refresh")
async def debug_refresh_topic_overviews():
    """Force-refresh topic overviews and return stage diagnostics."""
    try:
        freshness_snapshot = _dashboard_freshness_snapshot(force_refresh=False)
        ctx = _default_dashboard_context(freshness_snapshot)
        diagnostics = await run_background(
            lambda: topic_overviews.refresh_topic_overviews_with_diagnostics(ctx=ctx, force=True),
        )
        return {
            "success": True,
            "itemsProduced": diagnostics.get("stages", {}).get("finalTopics", 0),
            "diagnostics": diagnostics,
        }
    except Exception as e:
        logger.error(f"Topic overviews debug refresh endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.server:app",
        host=config.APP_HOST,
        port=config.APP_PORT,
        reload=False,
    )
