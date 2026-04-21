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
  venv/bin/python -m uvicorn api.server:app --reload --port 8001
"""
from __future__ import annotations
import base64
import sys, os
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

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
if config.should_validate_on_import():
    config.validate()

from fastapi import FastAPI, Query, HTTPException, Depends, Header, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
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

from api import aggregator as dashboard_aggregator
from api.aggregator import (
    CRITICAL_TIERS as DASHBOARD_CRITICAL_TIERS,
    build_dashboard_snapshot_once,
    get_dashboard_data, get_dashboard_snapshot, get_topics_page, get_channels_page,
    get_audience_page, get_topic_detail, get_channel_detail, get_audience_detail,
    get_topic_evidence_page, get_channel_posts_page, get_audience_messages_page,
    invalidate_cache, peek_dashboard_snapshot, refresh_dashboard_snapshot_async,
    schedule_dashboard_snapshot_refresh,
)
from api.dashboard_dates import build_dashboard_date_context
from api.queries import graph_dashboard, pulse
from api import freshness as freshness_runtime
from api.freshness import get_freshness_snapshot
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
from api.ai_helper import AIHelperError, OpenClawAiHelperProvider, get_default_ai_helper_provider
from api.runtime_coordinator import get_runtime_coordinator
from api.source_resolution import (
    build_pending_source_payload,
    enqueue_missing_peer_ref_backfill,
    ensure_resolution_job,
)
from buffer.supabase_writer import SupabaseWriter
from api.scraper_scheduler import ScraperSchedulerService
from processor import intent_extractor
from scraper.channel_metadata import minimal_source_metadata_from_entity, resolve_source_metadata
from social.store import SocialStore
from social.runtime import SocialRuntimeService
from utils.taxonomy import TAXONOMY_DOMAINS

# Preserve import-time patch targets used by existing tests.
_DASHBOARD_IMPORT_COMPAT = (build_dashboard_snapshot_once, get_dashboard_snapshot)

# ── App setup ────────────────────────────────────────────────────────────────

def _normalize_app_role(value: str | None) -> str:
    role = str(value or "").strip().lower()
    if role in {"web", "worker", "social-worker", "all"}:
        return role
    # Preserve the historical single-service deployment shape by default.
    return "all"


def _should_run_background_jobs(role: str | None = None) -> bool:
    return _normalize_app_role(APP_ROLE if role is None else role) in {"worker", "all"}


def _should_run_social_background_jobs(role: str | None = None) -> bool:
    return _normalize_app_role(APP_ROLE if role is None else role) in {"social-worker", "all"}


def _should_run_topic_overviews_materializer() -> bool:
    return _should_run_background_jobs() and bool(config.FEATURE_TOPIC_OVERVIEWS_AI)


def _enqueue_worker_scheduler_control(action: str, *, interval_minutes: int | None = None) -> dict[str, Any]:
    writer = get_supabase_writer()
    save_fn = getattr(writer, "save_shared_scraper_control_command", None)
    if not callable(save_fn):
        raise HTTPException(
            status_code=503,
            detail="Worker scheduler control is unavailable. Shared control storage is not configured.",
        )

    command = {
        "request_id": str(uuid.uuid4()),
        "action": str(action or "").strip().lower(),
        "status": "pending",
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "requested_by_role": _normalize_app_role(APP_ROLE),
    }
    if interval_minutes is not None:
        command["interval_minutes"] = max(1, int(interval_minutes))

    if not save_fn(command):
        raise HTTPException(
            status_code=503,
            detail="Worker scheduler control is unavailable. Failed to persist control command.",
        )

    try:
        status = dict(get_current_scraper_scheduler_status() or {})
    except Exception as exc:
        logger.warning(f"Falling back to default scraper status after control enqueue error: {exc}")
        status = _default_scraper_scheduler_status()
    status["worker_control"] = {
        "request_id": command["request_id"],
        "action": command["action"],
        "status": "pending",
        "requested_at": command["requested_at"],
    }
    if interval_minutes is not None:
        status["interval_minutes"] = max(1, int(interval_minutes))
    return status


def _enqueue_social_runtime_control(
    action: str,
    *,
    interval_minutes: int | None = None,
    stage: str | None = None,
    scope_key: str | None = None,
    activity_uids: list[str] | None = None,
) -> dict[str, Any]:
    command = {
        "request_id": str(uuid.uuid4()),
        "action": str(action or "").strip().lower(),
        "status": "pending",
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "requested_by_role": _normalize_app_role(APP_ROLE),
    }
    if interval_minutes is not None:
        command["interval_minutes"] = max(15, int(interval_minutes))
    if stage:
        command["stage"] = str(stage).strip().lower()
    if scope_key:
        command["scope_key"] = str(scope_key).strip()
    if activity_uids:
        command["activity_uids"] = [str(item).strip() for item in activity_uids if str(item).strip()]

    get_social_store().save_runtime_setting("control_command", command)
    status = dict(get_current_social_runtime_status() or {})
    status["worker_control"] = {
        "request_id": command["request_id"],
        "action": command["action"],
        "status": "pending",
        "requested_at": command["requested_at"],
    }
    if interval_minutes is not None:
        status["interval_minutes"] = max(15, int(interval_minutes))
    return status


def _apply_testing_release_invariants(role: str, warmers_enabled: bool) -> tuple[str, bool]:
    if config.IS_STAGING:
        if role == "social-worker" and config.ALLOW_STAGING_SOCIAL_WORKER:
            if warmers_enabled:
                logger.warning("Staging/testing social worker forced to RUN_STARTUP_WARMERS=false")
            return "social-worker", False
        if role != "web":
            logger.warning("Staging/testing environment forced to APP_ROLE=web (was {})", role)
        if warmers_enabled:
            logger.warning("Staging/testing environment forced to RUN_STARTUP_WARMERS=false")
        return "web", False
    return role, warmers_enabled


APP_ROLE = _normalize_app_role(os.getenv("APP_ROLE"))
RUN_STARTUP_WARMERS = str(os.getenv("RUN_STARTUP_WARMERS", "true")).strip().lower() in {"1", "true", "yes", "on"}
APP_ROLE, RUN_STARTUP_WARMERS = _apply_testing_release_invariants(APP_ROLE, RUN_STARTUP_WARMERS)
SENTRY_DSN = str(os.getenv("SENTRY_DSN", "")).strip()
SENTRY_TRACES_SAMPLE_RATE = max(0.0, min(float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.05")), 1.0))
_DASHBOARD_RESPONSE_DROP_KEYS = {"trendData", "voiceData", "integrationLevels", "housingHotTopics"}
_SERVER_TIMING_PATHS = {
    "/api/dashboard",
    "/api/topics",
    "/api/topics/detail",
    "/api/topics/evidence",
}
_HISTORICAL_FASTPATH_ENABLED = str(
    os.getenv("DASH_HISTORICAL_FASTPATH_ENABLED", "true")
).strip().lower() in {"1", "true", "yes", "on"}
_DEFAULT_DASHBOARD_LOOKBACK_DAYS = 14
_DEFAULT_ALIAS_FALLBACK_LOOKBACK_DAYS = max(1, int(os.getenv("DASH_DEFAULT_ALIAS_FALLBACK_DAYS", "3")))
_DASHBOARD_PERSISTED_PREFIX = "dashboard/snapshots"
_DASHBOARD_DEFAULT_ALIAS_PATH = f"{_DASHBOARD_PERSISTED_PREFIX}/default.json"
_HISTORICAL_FASTPATH_SKIP_TIERS = {
    item.strip().lower()
    for item in os.getenv(
        "DASH_HISTORICAL_FASTPATH_SKIP_TIERS",
        "network,comparative,predictive",
    ).split(",")
    if item.strip()
}
_orjson_dashboard_enabled = ORJSONResponse is not None
_orjson_dashboard_verified = ORJSONResponse is None


class DashboardWarmingError(RuntimeError):
    """Raised when a dashboard range has no usable exact snapshot yet."""


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


def _trim_dashboard_payload(snapshot: dict) -> dict:
    return {key: value for key, value in snapshot.items() if key not in _DASHBOARD_RESPONSE_DROP_KEYS}


def _has_usable_community_brief(snapshot: dict[str, Any] | None) -> bool:
    if not isinstance(snapshot, dict):
        return False
    brief = snapshot.get("communityBrief")
    if not isinstance(brief, dict) or not brief:
        return False
    return any(
        key in brief
        for key in (
            "postsAnalyzed24h",
            "commentScopesAnalyzed24h",
            "postsLast24h",
            "commentsLast24h",
            "totalAnalyses24h",
            "windowDays",
            "refreshedMinutesAgo",
        )
    )


def _is_placeholder_dashboard_snapshot(
    snapshot: dict[str, Any] | None,
    meta: dict[str, Any] | None,
) -> bool:
    cache_status = str((meta or {}).get("cacheStatus") or "").strip().lower()
    if cache_status == "emergency_degraded":
        return True
    return not _has_usable_community_brief(snapshot)


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


_init_sentry()


@asynccontextmanager
async def app_lifespan(_app: FastAPI):
    startup_started_at = time.perf_counter()
    startup_phases: dict[str, float] = {}
    key_fp = hashlib.sha256(config.OPENAI_API_KEY.encode("utf-8")).hexdigest()[:12] if config.OPENAI_API_KEY else "missing"
    logger.info(f"AI runtime configured | model={config.OPENAI_MODEL} key_fp={key_fp} role={APP_ROLE}")

    runtime_coordinator = get_runtime_coordinator()
    if config.IS_LOCKED_ENV:
        if not runtime_coordinator.ping():
            raise RuntimeError("Locked environments require a healthy Redis runtime coordinator.")
    elif config.REDIS_URL and not runtime_coordinator.ping():
        logger.warning("Redis runtime coordinator is configured but unavailable; falling back to local coordination")

    background_started = False
    if _should_run_background_jobs():
        background_started = True
        scheduler_started_at = time.perf_counter()
        scheduler = get_scraper_scheduler()
        startup_phases["schedulerInitMs"] = round((time.perf_counter() - scheduler_started_at) * 1000, 2)
        scheduler_boot_at = time.perf_counter()
        await scheduler.startup()
        startup_phases["schedulerStartupMs"] = round((time.perf_counter() - scheduler_boot_at) * 1000, 2)

        cards_scheduler_started_at = time.perf_counter()
        _start_question_cards_scheduler()
        _start_behavioral_cards_scheduler()
        _start_opportunity_cards_scheduler()
        if _should_run_topic_overviews_materializer():
            _start_topic_overviews_scheduler()
        startup_phases["cardsSchedulerStartupMs"] = round((time.perf_counter() - cards_scheduler_started_at) * 1000, 2)
        if RUN_STARTUP_WARMERS:
            warmers_started_at = time.perf_counter()
            asyncio.create_task(_warm_dashboard_cache())
            if config.QUESTION_BRIEFS_REFRESH_ON_STARTUP:
                asyncio.create_task(_materialize_question_cards_once(force=False))
            if config.BEHAVIORAL_BRIEFS_REFRESH_ON_STARTUP:
                asyncio.create_task(_materialize_behavioral_cards_once(force=False))
            if config.OPPORTUNITY_BRIEFS_REFRESH_ON_STARTUP:
                asyncio.create_task(_materialize_opportunity_cards_once(force=False))
            if _should_run_topic_overviews_materializer() and config.TOPIC_OVERVIEWS_REFRESH_ON_STARTUP:
                asyncio.create_task(_materialize_topic_overviews_once(force=False))
            startup_phases["warmersEnqueuedMs"] = round((time.perf_counter() - warmers_started_at) * 1000, 2)
    if _should_run_social_background_jobs() and config.SOCIAL_RUNTIME_ENABLED:
        background_started = True
        social_runtime_started_at = time.perf_counter()
        social_scheduler = get_social_runtime()
        await social_scheduler.startup()
        startup_phases["socialRuntimeStartupMs"] = round((time.perf_counter() - social_runtime_started_at) * 1000, 2)
    if not background_started:
        logger.info("Web-only runtime ready | background jobs disabled")

    startup_phases["totalStartupMs"] = round((time.perf_counter() - startup_started_at) * 1000, 2)
    logger.info(
        json.dumps(
            {
                "level": "info",
                "message": "startup_completed",
                "role": APP_ROLE,
                "background_jobs_enabled": _should_run_background_jobs(),
                "run_startup_warmers": RUN_STARTUP_WARMERS,
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
        global scraper_scheduler, supabase_writer, social_runtime_service, social_store_writer
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
            await scraper_scheduler.shutdown()
            scraper_scheduler = None
        if social_runtime_service is not None:
            await social_runtime_service.shutdown()
            social_runtime_service = None
        supabase_writer = None
        social_store_writer = None
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


@app.exception_handler(AIHelperError)
async def ai_helper_error_handler(_request: Request, exc: AIHelperError):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "error": {
                "code": exc.code,
                "message": exc.message,
                "retryable": exc.retryable,
            },
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    if request.url.path.startswith("/api/ai-helper/") or request.url.path.startswith("/api/ai/chat"):
        first = exc.errors()[0] if exc.errors() else {}
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": {
                    "code": "invalid_request",
                    "message": str(first.get("msg") or "Invalid AI helper request."),
                    "retryable": False,
                },
            },
        )
    return await request_validation_exception_handler(request, exc)


def _format_server_timing(request: Request, total_ms: float) -> str:
    metrics = [f"app;dur={total_ms:.2f}"]
    query_ms = getattr(request.state, "query_ms", None)
    if isinstance(query_ms, (int, float)):
        metrics.append(f"query;dur={float(query_ms):.2f}")
    return ", ".join(metrics)


def _record_query_timing(request: Request, started_at: float, *, cache_status: Optional[str] = None) -> None:
    elapsed_ms = (time.perf_counter() - started_at) * 1000
    request.state.query_ms = round(elapsed_ms, 2)
    if cache_status:
        request.state.cache_status = cache_status


def _is_ai_chat_path(path: str) -> bool:
    value = str(path or "")
    return value.startswith("/api/ai-helper/") or value.startswith("/api/ai/chat")


@app.middleware("http")
async def request_metrics_middleware(request: Request, call_next):
    request_id = uuid.uuid4().hex[:12]
    request.state.request_id = request_id
    started_at = time.perf_counter()
    response = None
    status_code = 500
    try:
        if _is_ai_chat_path(request.url.path):
            raw_length = str(request.headers.get("content-length") or "").strip()
            if raw_length:
                try:
                    content_length = int(raw_length)
                except ValueError:
                    content_length = 0
                if content_length > int(config.OPENCLAW_HELPER_HTTP_MAX_BODY_BYTES):
                    response = JSONResponse(
                        status_code=413,
                        content={
                            "ok": False,
                            "error": {
                                "code": "request_too_large",
                                "message": "The AI helper request body is too large.",
                                "retryable": False,
                            },
                        },
                    )
                    status_code = response.status_code
                    return response
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
        dashboard_meta = getattr(request.state, "dashboard_meta", None)
        if isinstance(dashboard_meta, dict):
            for src_key, dst_key in (
                ("cacheStatus", "dashboard_cache_status"),
                ("cacheSource", "dashboard_cache_source"),
                ("buildElapsedSeconds", "dashboard_build_elapsed_seconds"),
                ("buildMode", "dashboard_build_mode"),
                ("tierTimes", "dashboard_tier_times"),
                ("refreshFailureCount", "dashboard_refresh_failure_count"),
            ):
                value = dashboard_meta.get(src_key)
                if value not in (None, "", []):
                    payload[dst_key] = value
        logger.info(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))


class AIQueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)


class AIHelperChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=2000)
    sessionId: Optional[str] = Field(default=None, max_length=64)


class AIHelperSessionRequest(BaseModel):
    sessionId: Optional[str] = Field(default=None, max_length=64)


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


class SocialAccountUpsertRequest(BaseModel):
    platform: str = Field(..., min_length=1, max_length=32)
    account_handle: Optional[str] = Field(default=None, max_length=256)
    account_external_id: Optional[str] = Field(default=None, max_length=256)
    domain: Optional[str] = Field(default=None, max_length=256)
    is_active: bool = True
    metadata: Dict[str, Any] = Field(default_factory=dict)


class SocialEntityCreateRequest(BaseModel):
    legacy_company_id: str = Field(..., min_length=1, max_length=64)
    is_active: Optional[bool] = None
    accounts: List[SocialAccountUpsertRequest] = Field(default_factory=list)


class SocialEntityUpdateRequest(BaseModel):
    is_active: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None
    accounts: List[SocialAccountUpsertRequest] = Field(default_factory=list)


class SocialRuntimeRetryRequest(BaseModel):
    stage: str = Field(..., min_length=1, max_length=32)
    scope_key: str = Field(..., min_length=1, max_length=512)


class SocialRuntimeUpdateRequest(BaseModel):
    interval_minutes: int = Field(..., ge=15, le=1440)


class SocialRuntimeReplayRequest(BaseModel):
    stage: str = Field(default="analysis", min_length=1, max_length=32)
    activity_uids: List[str] = Field(..., min_length=1)


class AdminConfigPatchRequest(BaseModel):
    widgets: Optional[Dict[str, Dict[str, Any]]] = None
    prompts: Optional[Dict[str, Any]] = None
    runtime: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"


class GraphRequest(BaseModel):
    mode: Optional[str] = Field(default=None, max_length=64)
    timeframe: Optional[str] = Field(default="Last 7 Days", max_length=64)
    from_date: Optional[str] = Field(default=None, max_length=10)
    to_date: Optional[str] = Field(default=None, max_length=10)
    channels: Optional[List[str]] = None
    brandSource: Optional[List[str]] = None
    sentiment: Optional[List[str]] = None
    sentiments: Optional[List[str]] = None
    topics: Optional[List[str]] = None
    category: Optional[str] = Field(default=None, max_length=120)
    signalFocus: Optional[str] = Field(default=None, max_length=32)
    sourceDetail: Optional[str] = Field(default=None, max_length=32)
    rankingMode: Optional[str] = Field(default=None, max_length=32)
    minMentions: Optional[int] = Field(default=None, ge=1, le=100)
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
social_store_writer: SocialStore | None = None
social_runtime_service: SocialRuntimeService | None = None
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


def get_social_store() -> SocialStore:
    global social_store_writer
    if social_store_writer is None:
        social_store_writer = SocialStore()
    return social_store_writer


def get_social_runtime() -> SocialRuntimeService:
    global social_runtime_service
    if social_runtime_service is None:
        social_runtime_service = SocialRuntimeService(get_social_store())
    return social_runtime_service


def _default_social_runtime_status() -> dict[str, Any]:
    return {
        "status": "stopped",
        "is_active": False,
        "interval_minutes": 360,
        "running_now": False,
        "last_run_started_at": None,
        "last_run_finished_at": None,
        "last_success_at": None,
        "next_run_at": None,
        "last_error": None,
        "last_result": None,
        "run_history": [],
        "runtime_enabled": bool(config.SOCIAL_RUNTIME_ENABLED),
        "tiktok_enabled": bool(config.SOCIAL_TIKTOK_ENABLED),
        "postgres_worker_enabled": bool(config.SOCIAL_DATABASE_URL),
        "worker_id": None,
    }


_last_shared_social_status: dict[str, Any] | None = None
_last_shared_social_status_ts: datetime | None = None


def get_current_social_runtime_status() -> dict[str, Any]:
    global _last_shared_social_status, _last_shared_social_status_ts
    if not _should_run_social_background_jobs():
        now = datetime.now(timezone.utc)
        try:
            shared = get_social_store().get_runtime_setting("runtime_snapshot", {})
        except Exception as exc:
            logger.warning(f"Shared social runtime snapshot read failed on passive web: {exc}")
            shared = {}
        if shared:
            _last_shared_social_status = dict(shared)
            _last_shared_social_status_ts = now
            return dict(shared)
        if (
            _last_shared_social_status
            and _last_shared_social_status_ts
            and (now - _last_shared_social_status_ts).total_seconds() <= 10
        ):
            return dict(_last_shared_social_status)
        return _default_social_runtime_status()
    if social_runtime_service is None:
        return _default_social_runtime_status()
    return social_runtime_service.status()


_last_shared_scraper_status: dict[str, Any] | None = None
_last_shared_scraper_status_ts: datetime | None = None


def _default_scraper_scheduler_status() -> dict[str, Any]:
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
        "resolution": {
            "enabled": bool(config.FEATURE_SOURCE_RESOLUTION_WORKER),
            "running_now": False,
            "interval_minutes": max(1, int(config.SOURCE_RESOLUTION_INTERVAL_MINUTES)),
            "last_run_started_at": None,
            "last_run_finished_at": None,
            "last_success_at": None,
            "next_run_at": None,
            "last_error": None,
            "last_result": None,
            "run_history": [],
            "snapshot": {
                "slot_key": "primary",
                "due_jobs": 0,
                "leased_jobs": 0,
                "dead_letter_jobs": 0,
                "cooldown_slots": 0,
                "cooldown_until": None,
                "oldest_due_age_seconds": None,
                "active_pending_sources": 0,
                "active_missing_peer_refs": 0,
            },
        },
        "persisted": None,
    }


def get_current_scraper_scheduler_status() -> dict[str, Any]:
    global _last_shared_scraper_status, _last_shared_scraper_status_ts
    if not _should_run_background_jobs():
        now = datetime.now(timezone.utc)
        try:
            shared = get_supabase_writer().get_shared_scraper_runtime_snapshot(default={}, timeout_seconds=2.5)
        except Exception as exc:
            logger.warning(f"Shared scraper runtime snapshot read failed on passive web: {exc}")
            shared = {}
        if shared:
            _last_shared_scraper_status = dict(shared)
            _last_shared_scraper_status_ts = now
            return dict(shared)
        if (
            _last_shared_scraper_status
            and _last_shared_scraper_status_ts
            and (now - _last_shared_scraper_status_ts).total_seconds() <= 10
        ):
            return dict(_last_shared_scraper_status)
        return _default_scraper_scheduler_status()
    if scraper_scheduler is None:
        return _default_scraper_scheduler_status()
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

    window_seconds = max(1, int(config.ANALYTICS_RATE_LIMIT_WINDOW_SECONDS))
    max_requests = max(1, int(config.ANALYTICS_RATE_LIMIT_MAX_REQUESTS))
    client_ip = _analytics_client_ip(request)
    counter_name = f"analytics:{client_ip}:{request.url.path}"
    count = get_runtime_coordinator().increment_window_counter(counter_name, window_seconds)
    if count > max_requests:
        logger.warning(
            "Analytics rate limit exceeded | endpoint={} client_ip={} count={}".format(
                request.url.path,
                client_ip,
                count,
            )
        )
        raise HTTPException(status_code=429, detail="Rate limit exceeded for analytics API.")


def _validate_analytics_access(
    request: Request,
    authorization: Optional[str],
    *,
    enforce_rate_limit: bool,
) -> None:
    if enforce_rate_limit:
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


def require_analytics_access(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> None:
    _validate_analytics_access(
        request,
        authorization,
        enforce_rate_limit=True,
    )


def _extract_bearer_token(raw_value: str | None) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    scheme, _, token = text.partition(" ")
    if scheme.lower() == "bearer" and token.strip():
        return token.strip()
    return ""


def _extract_basic_credentials(raw_value: str | None) -> tuple[str, str] | None:
    text = str(raw_value or "").strip()
    if not text:
        return None
    scheme, _, token = text.partition(" ")
    if scheme.lower() != "basic" or not token.strip():
        return None
    try:
        decoded = base64.b64decode(token.strip()).decode("utf-8")
    except Exception:
        return None
    username, sep, password = decoded.partition(":")
    if not sep:
        return None
    return username, password


def _simple_auth_credentials_are_valid(username: str, password: str) -> bool:
    configured_username = str(getattr(config, "SIMPLE_AUTH_USERNAME", "") or "").strip()
    configured_password = str(getattr(config, "SIMPLE_AUTH_PASSWORD", "") or "")
    if not configured_username or not configured_password:
        return False
    normalized_username = str(username or "").strip().lower()
    return hmac.compare_digest(normalized_username, configured_username.lower()) and hmac.compare_digest(
        password,
        configured_password,
    )


def get_ai_helper_provider():
    return get_default_ai_helper_provider()


_AI_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{8,64}$")


def _normalize_ai_session_id(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if not _AI_SESSION_ID_RE.fullmatch(text):
        raise AIHelperError(
            status_code=400,
            code="invalid_request",
            message="sessionId must be 8-64 characters using letters, numbers, '-' or '_'.",
            retryable=False,
        )
    return text


async def require_admin_user(
    x_supabase_authorization: Optional[str] = Header(default=None, alias="X-Supabase-Authorization"),
    authorization: Optional[str] = Header(default=None),
    *,
    allow_frontend_proxy_token: bool = False,
) -> Dict[str, str]:
    supabase_token = _extract_bearer_token(x_supabase_authorization)
    auth_token = _extract_bearer_token(authorization)
    token = supabase_token or auth_token
    if not token:
        raise AIHelperError(
            status_code=401,
            code="auth_required",
            message="Sign in as the configured admin to use the AI helper.",
            retryable=False,
        )

    admin_user_id = str(getattr(config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "") or "").strip()
    admin_email = str(getattr(config, "AI_HELPER_ADMIN_EMAIL", "") or "").strip().lower()
    if not admin_user_id and not admin_email:
        raise AIHelperError(
            status_code=503,
            code="auth_unconfigured",
            message="The AI helper admin identity is not configured.",
            retryable=False,
        )

    if (
        allow_frontend_proxy_token
        and not supabase_token
        and auth_token
        and config.ANALYTICS_API_KEY_FRONTEND
        and hmac.compare_digest(auth_token, config.ANALYTICS_API_KEY_FRONTEND)
    ):
        logger.info("AI helper auth satisfied via trusted frontend proxy token")
        return {"id": "frontend-proxy", "email": "", "auth": "frontend_proxy"}

    try:
        loop = asyncio.get_running_loop()
        user_response = await loop.run_in_executor(None, lambda: get_supabase_writer().client.auth.get_user(token))
    except Exception as exc:
        logger.warning(f"AI helper auth validation failed: {exc}")
        raise AIHelperError(
            status_code=401,
            code="auth_invalid",
            message="Your session could not be validated. Please sign in again.",
            retryable=False,
        ) from exc

    user = getattr(user_response, "user", None)
    if user is None:
        raise AIHelperError(
            status_code=401,
            code="auth_invalid",
            message="Your session could not be validated. Please sign in again.",
            retryable=False,
        )

    user_id = str(getattr(user, "id", "") or "").strip()
    email = str(getattr(user, "email", "") or "").strip().lower()

    is_allowed = bool(admin_user_id and user_id == admin_user_id)
    if not is_allowed and not config.IS_PRODUCTION and admin_email:
        is_allowed = email == admin_email

    if not is_allowed:
        raise AIHelperError(
            status_code=403,
            code="admin_only",
            message="The AI helper is available to the configured admin only.",
            retryable=False,
        )

    return {"id": user_id, "email": email}


async def require_ai_helper_access(
    x_supabase_authorization: Optional[str] = Header(default=None, alias="X-Supabase-Authorization"),
    x_admin_authorization: Optional[str] = Header(default=None, alias="X-Admin-Authorization"),
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, str]:
    if x_supabase_authorization or x_admin_authorization:
        try:
            return await require_operator_access(
                x_supabase_authorization=x_supabase_authorization,
                x_admin_authorization=x_admin_authorization,
                authorization=authorization,
            )
        except HTTPException as exc:
            detail = str(getattr(exc, "detail", "") or "").strip()
            status_code = int(getattr(exc, "status_code", 401) or 401)
            code = "auth_invalid"
            if status_code == 403:
                code = "admin_only"
            elif status_code == 401 and detail.startswith("Sign in as the configured admin"):
                code = "auth_required"
            raise AIHelperError(
                status_code=status_code,
                code=code,
                message=detail or "Unable to authorize the AI helper request.",
                retryable=False,
            ) from exc
    return await require_admin_user(
        x_supabase_authorization=x_supabase_authorization,
        authorization=authorization,
        allow_frontend_proxy_token=True,
    )


def _allow_local_operator_bypass(
    *,
    x_supabase_authorization: Optional[str],
    x_admin_authorization: Optional[str],
    authorization: Optional[str],
) -> bool:
    if config.IS_LOCKED_ENV:
        return False
    if str(getattr(config, "ADMIN_API_KEY", "") or "").strip():
        return False
    if str(getattr(config, "AI_HELPER_ADMIN_SUPABASE_USER_ID", "") or "").strip():
        return False
    if str(getattr(config, "AI_HELPER_ADMIN_EMAIL", "") or "").strip():
        return False
    supplied_tokens = (
        _extract_bearer_token(x_supabase_authorization),
        _extract_bearer_token(x_admin_authorization),
        _extract_bearer_token(authorization),
    )
    return not any(supplied_tokens)


async def require_operator_access(
    x_supabase_authorization: Optional[str] = Header(default=None, alias="X-Supabase-Authorization"),
    x_admin_authorization: Optional[str] = Header(default=None, alias="X-Admin-Authorization"),
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, str]:
    admin_api_key = str(getattr(config, "ADMIN_API_KEY", "") or "").strip()
    admin_token = _extract_bearer_token(x_admin_authorization) or _extract_bearer_token(authorization)
    if admin_api_key and admin_token and hmac.compare_digest(admin_token, admin_api_key):
        return {"id": "admin-api-key", "email": "", "auth": "api_key"}

    simple_credentials = _extract_basic_credentials(x_admin_authorization) or _extract_basic_credentials(authorization)
    if simple_credentials and _simple_auth_credentials_are_valid(*simple_credentials):
        return {
            "id": f"simple-auth:{simple_credentials[0].strip().lower()}",
            "email": "",
            "auth": "simple_password",
        }

    if _allow_local_operator_bypass(
        x_supabase_authorization=x_supabase_authorization,
        x_admin_authorization=x_admin_authorization,
        authorization=authorization,
    ):
        logger.warning("Operator route accessed via local-development bypass")
        return {"id": "local-dev", "email": "", "auth": "local_dev"}

    try:
        operator = await require_admin_user(
            x_supabase_authorization=x_supabase_authorization,
            authorization=None if x_supabase_authorization else authorization,
        )
    except AIHelperError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc

    operator["auth"] = "supabase"
    return operator


async def require_kb_access(
    request: Request,
    x_supabase_authorization: Optional[str] = Header(default=None, alias="X-Supabase-Authorization"),
    x_admin_authorization: Optional[str] = Header(default=None, alias="X-Admin-Authorization"),
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, str]:
    """
    KB routes are shared by the browser admin UI and the OpenClaw bridge.
    Accept either operator/admin auth or analytics bearer tokens without
    changing the existing contracts for other endpoints.
    """
    _enforce_analytics_rate_limit(request)

    if x_supabase_authorization or x_admin_authorization:
        return await require_operator_access(
            x_supabase_authorization=x_supabase_authorization,
            x_admin_authorization=x_admin_authorization,
            authorization=authorization,
        )

    try:
        _validate_analytics_access(
            request,
            authorization,
            enforce_rate_limit=False,
        )
        return {"id": "analytics-api", "email": "", "auth": "analytics"}
    except HTTPException as analytics_exc:
        try:
            return await require_operator_access(
                x_supabase_authorization=x_supabase_authorization,
                x_admin_authorization=x_admin_authorization,
                authorization=authorization,
            )
        except HTTPException:
            raise analytics_exc


async def require_debug_endpoint_access(
    x_supabase_authorization: Optional[str] = Header(default=None, alias="X-Supabase-Authorization"),
    x_admin_authorization: Optional[str] = Header(default=None, alias="X-Admin-Authorization"),
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, str]:
    if not config.ENABLE_DEBUG_ENDPOINTS:
        raise HTTPException(status_code=404, detail="Not found")
    return await require_operator_access(
        x_supabase_authorization=x_supabase_authorization,
        x_admin_authorization=x_admin_authorization,
        authorization=authorization,
    )


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
        prefer_shared_snapshot=not _should_run_background_jobs(),
    )


def _dashboard_snapshot_storage_path(cache_key: str) -> str:
    safe_key = str(cache_key or "").strip().replace("/", "_")
    return f"{_DASHBOARD_PERSISTED_PREFIX}/{safe_key}.json"


def _dashboard_context_from_trusted_end(trusted_end: date):
    from_date = (trusted_end - timedelta(days=_DEFAULT_DASHBOARD_LOOKBACK_DAYS)).isoformat()
    return build_dashboard_date_context(from_date, trusted_end.isoformat())


def _persisted_dashboard_payload(
    *,
    ctx,
    snapshot: dict,
    meta: dict[str, Any],
    trusted_end_date: str,
) -> dict[str, Any]:
    snapshot_built_at = (
        _parse_snapshot_date(meta.get("snapshotBuiltAt"))
        or datetime.now(timezone.utc)
    )
    normalized_meta = dict(meta or {})
    normalized_meta["snapshotBuiltAt"] = snapshot_built_at.isoformat()
    return {
        "cacheKey": ctx.cache_key,
        "from": ctx.from_date.isoformat(),
        "to": ctx.to_date.isoformat(),
        "trustedEndDate": str(trusted_end_date or ctx.to_date.isoformat()),
        "snapshotBuiltAt": snapshot_built_at.isoformat(),
        "snapshot": dict(snapshot or {}),
        "meta": normalized_meta,
    }


def _load_persisted_dashboard_snapshot(path: str) -> dict[str, Any]:
    key = str(path or "").strip()
    if not key:
        return {"status": "miss", "readMs": 0.0}

    try:
        result = get_supabase_writer().read_runtime_json(
            key,
            prefer_signed_read=False,
            timeout_seconds=1.5,
        )
        status = str(result.get("status") or "missing")
        read_ms = float(result.get("elapsed_ms") or 0.0)
        if status != "ok":
            if status in {"missing", "timeout", "invalid_path"}:
                return {"status": "miss", "readMs": read_ms}
            return {"status": status, "readMs": read_ms}

        payload = result.get("payload")
        if not isinstance(payload, dict):
            return {"status": "invalid_payload", "readMs": read_ms}

        from_value = str(payload.get("from") or "").strip()
        to_value = str(payload.get("to") or "").strip()
        snapshot = payload.get("snapshot")
        meta = payload.get("meta")
        if not from_value or not to_value or not isinstance(snapshot, dict) or not isinstance(meta, dict):
            return {"status": "invalid_payload", "readMs": read_ms}

        try:
            ctx = build_dashboard_date_context(from_value, to_value)
        except Exception:
            return {"status": "invalid_payload", "readMs": read_ms}

        snapshot_built_at = _parse_snapshot_date(payload.get("snapshotBuiltAt") or meta.get("snapshotBuiltAt"))
        trusted_end_date = str(payload.get("trustedEndDate") or ctx.to_date.isoformat())
        return {
            "status": "hit",
            "readMs": read_ms,
            "snapshot": snapshot,
            "meta": meta,
            "ctx": ctx,
            "snapshotBuiltAt": snapshot_built_at or datetime.now(timezone.utc),
            "trustedEndDate": trusted_end_date,
        }
    except Exception as exc:
        logger.warning("Persisted dashboard snapshot read failed | path={} error={}", key, exc)
        return {"status": "miss", "readMs": 0.0}


def _load_recent_default_dashboard_snapshot(trusted_end: date) -> dict[str, Any]:
    best_choice: dict[str, Any] | None = None
    trusted_cutoff = datetime.combine(trusted_end, datetime.min.time(), tzinfo=timezone.utc)

    for offset_days in range(1, _DEFAULT_ALIAS_FALLBACK_LOOKBACK_DAYS + 1):
        candidate_end = trusted_end - timedelta(days=offset_days)
        ctx = _dashboard_context_from_trusted_end(candidate_end)
        loaded = _load_persisted_dashboard_snapshot(_dashboard_snapshot_storage_path(ctx.cache_key))
        if loaded.get("status") != "hit":
            continue
        built_at = loaded.get("snapshotBuiltAt")
        if best_choice is None:
            best_choice = loaded
            continue
        best_built_at = best_choice.get("snapshotBuiltAt")
        if isinstance(built_at, datetime) and isinstance(best_built_at, datetime) and built_at > best_built_at:
            best_choice = loaded

    return best_choice or {"status": "miss", "readMs": 0.0, "trustedEndDate": trusted_cutoff.date().isoformat()}


def _snapshot_age_seconds(snapshot_built_at: datetime | None) -> float | None:
    if snapshot_built_at is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - snapshot_built_at).total_seconds())


def _is_persisted_snapshot_fresh(snapshot_built_at: datetime | None) -> bool:
    age_seconds = _snapshot_age_seconds(snapshot_built_at)
    return age_seconds is not None and age_seconds < float(dashboard_aggregator.CACHE_TTL_SECONDS)


def _is_persisted_snapshot_usable(snapshot_built_at: datetime | None) -> bool:
    age_seconds = _snapshot_age_seconds(snapshot_built_at)
    return age_seconds is not None and age_seconds < float(dashboard_aggregator.MAX_STALE_SECONDS)


def get_cached_freshness_snapshot() -> tuple[dict | None, datetime | None]:
    snapshot = freshness_runtime._CACHE if isinstance(freshness_runtime._CACHE, dict) else None
    return snapshot, freshness_runtime._CACHE_TS


def prime_freshness_snapshot(snapshot: dict, *, cached_at: datetime | None = None) -> None:
    freshness_runtime._CACHE = dict(snapshot or {})
    if isinstance(snapshot, dict):
        freshness_runtime._CACHE_TS = cached_at or _parse_snapshot_date(snapshot.get("generated_at"))
    else:
        freshness_runtime._CACHE_TS = cached_at


def freshness_cache_ttl_seconds() -> int:
    return int(getattr(freshness_runtime, "_CACHE_TTL_SECONDS", 300))


def _freshness_memory_snapshot() -> dict | None:
    cached_snapshot, cached_at = get_cached_freshness_snapshot()
    if not isinstance(cached_snapshot, dict):
        return None
    snapshot_built_at = cached_at or _parse_snapshot_date(cached_snapshot.get("generated_at"))
    age_seconds = _snapshot_age_seconds(snapshot_built_at)
    if age_seconds is None or age_seconds >= float(freshness_cache_ttl_seconds()):
        return None
    return cached_snapshot


def _load_current_freshness_snapshot(*, force_refresh: bool) -> dict:
    return get_freshness_snapshot(
        get_supabase_writer(),
        scheduler_status=get_current_scraper_scheduler_status(),
        force_refresh=force_refresh,
        prefer_shared_snapshot=not force_refresh,
    )


async def _resolve_freshness_snapshot(*, force_refresh: bool) -> dict:
    if not force_refresh:
        cached_snapshot = _freshness_memory_snapshot()
        if cached_snapshot is not None:
            return cached_snapshot

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None,
        lambda: _load_current_freshness_snapshot(force_refresh=force_refresh),
    )


def prime_dashboard_snapshot(ctx, snapshot: dict, meta: dict[str, Any], *, cached_at_ts: float | None = None) -> None:
    cached_at = cached_at_ts if cached_at_ts is not None else time.time()
    with dashboard_aggregator._cache_lock:
        dashboard_aggregator._cache_entries[ctx.cache_key] = (cached_at, dict(snapshot or {}), dict(meta or {}))


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

    if not allow_live:
        return {
            "snapshot": None,
            "source": None,
            "snapshotBuiltAt": None,
            "persistedReadStatus": "miss",
            "persistedReadMs": 0.0,
        }

    snapshot = _dashboard_freshness_snapshot(force_refresh=False)
    snapshot_built_at = _parse_snapshot_date(snapshot.get("generated_at")) or datetime.now(timezone.utc)
    prime_freshness_snapshot(snapshot, cached_at=snapshot_built_at)
    return {
        "snapshot": snapshot,
        "source": "live",
        "snapshotBuiltAt": snapshot_built_at,
        "persistedReadStatus": "miss",
        "persistedReadMs": 0.0,
    }


def _ensure_background_freshness_refresh() -> bool:
    thread = threading.Thread(
        target=lambda: _dashboard_freshness_snapshot(force_refresh=True),
        daemon=True,
        name="freshness-refresh",
    )
    thread.start()
    return True


def _should_persist_dashboard_snapshot(meta: dict[str, Any]) -> bool:
    if bool(meta.get("isStale")):
        return False
    degraded = {
        str(name).strip()
        for name in (meta.get("degradedTiers") or [])
        if str(name).strip()
    }
    return not bool(degraded.intersection(DASHBOARD_CRITICAL_TIERS))


def _persist_dashboard_snapshot_async(
    ctx,
    snapshot: dict,
    meta: dict[str, Any],
    *,
    trusted_end_date: str,
    write_default_alias: bool,
) -> None:
    def _runner() -> None:
        try:
            payload = _persisted_dashboard_payload(
                ctx=ctx,
                snapshot=snapshot,
                meta=meta,
                trusted_end_date=trusted_end_date,
            )
            writer = get_supabase_writer()
            primary_path = _dashboard_snapshot_storage_path(ctx.cache_key)
            primary_ok = writer.save_runtime_json_fast(primary_path, payload)
            alias_ok = True
            if write_default_alias:
                alias_ok = writer.save_runtime_json_fast(_DASHBOARD_DEFAULT_ALIAS_PATH, payload)
            if not primary_ok or not alias_ok:
                logger.warning(
                    "Persisted dashboard snapshot write incomplete | key={} primary_ok={} alias_ok={}",
                    ctx.cache_key,
                    primary_ok,
                    alias_ok,
                )
        except Exception as exc:
            logger.warning("Persisted dashboard snapshot write failed | key={} error={}", ctx.cache_key, exc)

    thread = threading.Thread(
        target=_runner,
        daemon=True,
        name=f"persist-dashboard-{ctx.cache_key}",
    )
    thread.start()
    return None


def _ensure_background_dashboard_refresh(
    ctx,
    *,
    trusted_end_date: str,
    write_default_alias: bool,
) -> bool:
    def _runner() -> None:
        try:
            snapshot, meta = get_dashboard_snapshot(ctx, force_refresh=True)
            if _should_persist_dashboard_snapshot(meta):
                payload = _persisted_dashboard_payload(
                    ctx=ctx,
                    snapshot=snapshot,
                    meta=meta,
                    trusted_end_date=trusted_end_date,
                )
                writer = get_supabase_writer()
                writer.save_runtime_json_fast(_dashboard_snapshot_storage_path(ctx.cache_key), payload)
                if write_default_alias:
                    writer.save_runtime_json_fast(_DASHBOARD_DEFAULT_ALIAS_PATH, payload)
        except Exception as exc:
            logger.warning("Background dashboard refresh failed | key={} error={}", ctx.cache_key, exc)

    thread = threading.Thread(
        target=_runner,
        daemon=True,
        name=f"dashboard-refresh-persist-{ctx.cache_key}",
    )
    thread.start()
    return True


def _newer_snapshot_choice(
    left: tuple[str, dict, dict[str, Any], datetime | None] | None,
    right: tuple[str, dict, dict[str, Any], datetime | None] | None,
) -> tuple[str, dict, dict[str, Any], datetime | None] | None:
    if left is None:
        return right
    if right is None:
        return left

    left_built_at = left[3] or datetime.min.replace(tzinfo=timezone.utc)
    right_built_at = right[3] or datetime.min.replace(tzinfo=timezone.utc)
    return left if left_built_at >= right_built_at else right


def _default_dashboard_context(snapshot: Optional[dict] = None):
    freshness_snapshot = snapshot or _dashboard_freshness_snapshot(force_refresh=False)
    trusted_end = _trusted_end_date_from_freshness(freshness_snapshot)
    return _dashboard_context_from_trusted_end(trusted_end)


def _should_use_historical_fastpath(
    *,
    default_request: bool,
    ctx,
    trusted_end_date,
) -> bool:
    return bool(
        _HISTORICAL_FASTPATH_ENABLED
        and not default_request
        and _HISTORICAL_FASTPATH_SKIP_TIERS
        and ctx.to_date < trusted_end_date
    )


def _build_dashboard_api_payload(
    *,
    ctx,
    requested_from: str,
    requested_to: str,
    trusted_end_date: str,
    dashboard_data: dict,
    dashboard_runtime_meta: dict[str, Any],
    freshness_snapshot: dict,
    cache_source: str | None = None,
    freshness_source: str | None = None,
    persisted_read_status: str | None = None,
    persisted_read_ms: float | None = None,
    cache_status_override: str | None = None,
    default_resolution_path: str | None = None,
    fallback_reason: str | None = None,
    refresh_suppressed: bool | None = None,
) -> dict[str, Any]:
    trimmed_dashboard_data = _trim_dashboard_payload(dashboard_data)
    cache_status = cache_status_override or dashboard_runtime_meta.get("cacheStatus")
    response = {
        "data": trimmed_dashboard_data,
        "meta": {
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
            "cacheStatus": cache_status,
            "cacheSource": cache_source or dashboard_runtime_meta.get("cacheSource"),
            "isStale": dashboard_runtime_meta.get("isStale", False),
            "buildElapsedSeconds": dashboard_runtime_meta.get("buildElapsedSeconds"),
            "buildMode": dashboard_runtime_meta.get("buildMode"),
            "refreshFailureCount": dashboard_runtime_meta.get("refreshFailureCount", 0),
            "skippedTiers": dashboard_runtime_meta.get("skippedTiers", []),
            "persistedReadStatus": persisted_read_status,
            "persistedReadMs": persisted_read_ms,
            "defaultResolutionPath": default_resolution_path,
            "freshness": {
                "status": freshness_snapshot.get("health", {}).get("status") if isinstance(freshness_snapshot, dict) else None,
                "generatedAt": freshness_snapshot.get("generated_at") if isinstance(freshness_snapshot, dict) else None,
                "source": freshness_source,
            },
        },
    }
    if fallback_reason is not None:
        response["meta"]["fallbackReason"] = fallback_reason
    if refresh_suppressed is not None:
        response["meta"]["refreshSuppressed"] = bool(refresh_suppressed)
    response["meta"]["responseBytes"] = -1  # measured by proxy/CDN; removed double-serialize overhead
    response["meta"]["responseSerializeMs"] = 0
    return response


def _build_dashboard_response_payload(
    from_date: Optional[str],
    to_date: Optional[str],
) -> dict[str, Any]:
    default_request = not from_date or not to_date
    recent_default_fallback: dict[str, Any] | None = None
    recent_default_fallback_checked = False
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
                if _is_placeholder_dashboard_snapshot(default_snapshot.get("snapshot"), dashboard_meta):
                    logger.warning(
                        "Ignoring placeholder persisted dashboard alias snapshot | cache_status={} built_at={}",
                        dashboard_meta.get("cacheStatus"),
                        dashboard_meta.get("snapshotBuiltAt"),
                    )
                else:
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
                        freshness_snapshot={},
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
            recent_default_fallback = _load_recent_default_dashboard_snapshot(ctx.to_date)
            recent_default_fallback_checked = True
            if recent_default_fallback.get("status") == "hit":
                fallback_ctx = recent_default_fallback["ctx"]
                fallback_trusted_end = str(recent_default_fallback.get("trustedEndDate") or fallback_ctx.to_date.isoformat())
                fallback_meta = dict(recent_default_fallback.get("meta") or {})
                fallback_meta["isStale"] = True
                if _is_placeholder_dashboard_snapshot(recent_default_fallback.get("snapshot"), fallback_meta):
                    logger.warning(
                        "Ignoring placeholder recent default dashboard snapshot | cache_status={} built_at={}",
                        fallback_meta.get("cacheStatus"),
                        fallback_meta.get("snapshotBuiltAt"),
                    )
                else:
                    prime_dashboard_snapshot(
                        fallback_ctx,
                        recent_default_fallback["snapshot"],
                        fallback_meta,
                        cached_at_ts=recent_default_fallback["snapshotBuiltAt"].timestamp(),
                    )
                    _ensure_background_dashboard_refresh(
                        fallback_ctx,
                        trusted_end_date=fallback_trusted_end,
                        write_default_alias=True,
                    )
                    return _build_dashboard_api_payload(
                        ctx=fallback_ctx,
                        trusted_end_date=fallback_trusted_end,
                        dashboard_data=recent_default_fallback["snapshot"],
                        dashboard_runtime_meta=fallback_meta,
                        requested_from=ctx.from_date.isoformat(),
                        requested_to=ctx.to_date.isoformat(),
                        cache_source="persisted",
                        freshness_snapshot=freshness_snapshot or {},
                        freshness_source=freshness_source,
                        persisted_read_status=recent_default_fallback.get("status"),
                        persisted_read_ms=recent_default_fallback.get("readMs"),
                        cache_status_override="persisted_recent_fallback_while_revalidate",
                        default_resolution_path="persisted_recent_fallback",
                    )
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
        if _is_placeholder_dashboard_snapshot(memory_snapshot, memory_meta):
            logger.warning(
                "Ignoring placeholder memory dashboard snapshot | key={} cache_status={}",
                ctx.cache_key,
                memory_meta.get("cacheStatus"),
            )
        else:
            return _build_dashboard_api_payload(
                ctx=ctx,
                trusted_end_date=trusted_end_iso,
                dashboard_data=memory_snapshot,
                dashboard_runtime_meta=memory_meta,
                requested_from=requested_from,
                requested_to=requested_to,
                cache_source="memory",
                freshness_snapshot=freshness_snapshot or {},
                freshness_source=freshness_source,
                cache_status_override="memory_fresh",
            )

    persisted_snapshot = _load_persisted_dashboard_snapshot(_dashboard_snapshot_storage_path(ctx.cache_key))
    persisted_read_status = persisted_snapshot.get("status")
    persisted_read_ms = persisted_snapshot.get("readMs")

    if default_request and persisted_snapshot.get("status") != "hit" and not recent_default_fallback_checked:
        recent_default_snapshot = _load_recent_default_dashboard_snapshot(ctx.to_date)
        recent_default_fallback_checked = True
        recent_default_fallback = recent_default_snapshot

    if (
        default_request
        and persisted_snapshot.get("status") != "hit"
        and recent_default_fallback is not None
        and recent_default_fallback.get("status") == "hit"
    ):
        recent_default_snapshot = recent_default_fallback
        fallback_ctx = recent_default_snapshot["ctx"]
        fallback_trusted_end = str(recent_default_snapshot.get("trustedEndDate") or fallback_ctx.to_date.isoformat())
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
        return _build_dashboard_api_payload(
            ctx=fallback_ctx,
            trusted_end_date=fallback_trusted_end,
            dashboard_data=recent_default_snapshot["snapshot"],
            dashboard_runtime_meta=fallback_meta,
            requested_from=requested_from,
            requested_to=requested_to,
            cache_source="persisted",
            freshness_snapshot=freshness_snapshot or {},
            freshness_source=freshness_source,
            persisted_read_status=persisted_read_status,
            persisted_read_ms=persisted_read_ms,
            default_resolution_path="persisted_recent_fallback",
            cache_status_override=(
                "persisted_recent_fallback_while_revalidate"
                if refresh_started
                else "persisted_recent_fallback_refresh_inflight"
            ),
        )

    memory_stale_choice: tuple[str, dict, dict[str, Any], datetime | None] | None = None
    if memory_state == "stale" and memory_snapshot is not None and memory_meta is not None:
        memory_stale_choice = (
            "memory",
            memory_snapshot,
            dict(memory_meta),
            _parse_snapshot_date(memory_meta.get("snapshotBuiltAt")),
        )

    persisted_stale_choice: tuple[str, dict, dict[str, Any], datetime | None] | None = None
    if persisted_snapshot.get("status") == "hit":
        persisted_meta = dict(persisted_snapshot.get("meta") or {})
        persisted_built_at = persisted_snapshot.get("snapshotBuiltAt")
        persisted_meta["isStale"] = not _is_persisted_snapshot_fresh(persisted_built_at)
        if not persisted_meta["isStale"]:
            if _is_placeholder_dashboard_snapshot(persisted_snapshot.get("snapshot"), persisted_meta):
                logger.warning(
                    "Ignoring placeholder persisted dashboard snapshot | key={} cache_status={}",
                    ctx.cache_key,
                    persisted_meta.get("cacheStatus"),
                )
            else:
                prime_dashboard_snapshot(
                    ctx,
                    persisted_snapshot["snapshot"],
                    persisted_meta,
                    cached_at_ts=persisted_built_at.timestamp(),
                )
                return _build_dashboard_api_payload(
                    ctx=ctx,
                    trusted_end_date=trusted_end_iso,
                    dashboard_data=persisted_snapshot["snapshot"],
                    dashboard_runtime_meta=persisted_meta,
                    requested_from=requested_from,
                    requested_to=requested_to,
                    cache_source="persisted",
                    freshness_snapshot=freshness_snapshot or {},
                    freshness_source=freshness_source,
                    persisted_read_status=persisted_read_status,
                    persisted_read_ms=persisted_read_ms,
                    cache_status_override="persisted_fresh",
                )

        if _is_persisted_snapshot_usable(persisted_built_at):
            persisted_stale_choice = ("persisted", persisted_snapshot["snapshot"], persisted_meta, persisted_built_at)

    stale_choice = _newer_snapshot_choice(memory_stale_choice, persisted_stale_choice)
    if stale_choice is not None:
        cache_source, stale_snapshot, stale_meta, persisted_built_at = stale_choice
        if _is_placeholder_dashboard_snapshot(stale_snapshot, stale_meta):
            logger.warning(
                "Ignoring placeholder stale dashboard snapshot | key={} cache_source={} cache_status={}",
                ctx.cache_key,
                cache_source,
                stale_meta.get("cacheStatus"),
            )
        else:
            if cache_source == "persisted" and persisted_built_at is not None:
                prime_dashboard_snapshot(
                    ctx,
                    stale_snapshot,
                    stale_meta,
                    cached_at_ts=persisted_built_at.timestamp(),
                )
            refresh_status = schedule_dashboard_snapshot_refresh(ctx)
            if default_request and freshness_snapshot is None:
                _ensure_background_freshness_refresh()
            stale_meta["isStale"] = True
            stale_meta["refreshSuppressed"] = bool(refresh_status.get("suppressed"))
            cache_status = f"{cache_source}_stale_while_revalidate"
            fallback_reason = "exact_stale_snapshot"
            if refresh_status.get("suppressed"):
                cache_status = f"{cache_source}_stale_refresh_suppressed"
                fallback_reason = "exact_stale_snapshot_refresh_suppressed"
            elif not refresh_status.get("started"):
                cache_status = f"{cache_source}_stale_refresh_inflight"
                fallback_reason = "exact_stale_snapshot_refresh_inflight"
            return _build_dashboard_api_payload(
                ctx=ctx,
                trusted_end_date=trusted_end_iso,
                dashboard_data=stale_snapshot,
                dashboard_runtime_meta=stale_meta,
                requested_from=requested_from,
                requested_to=requested_to,
                cache_source=cache_source,
                freshness_snapshot=freshness_snapshot or {},
                freshness_source=freshness_source,
                persisted_read_status=persisted_read_status,
                persisted_read_ms=persisted_read_ms,
                cache_status_override=cache_status,
                fallback_reason=fallback_reason,
                refresh_suppressed=bool(refresh_status.get("suppressed")),
            )

    if _should_use_historical_fastpath(
        default_request=default_request,
        ctx=ctx,
        trusted_end_date=_trusted_end_date_from_freshness(freshness_snapshot or {}) if freshness_snapshot else ctx.to_date,
    ):
        fast_data, fast_meta = build_dashboard_snapshot_once(
            ctx,
            skipped_tiers=set(_HISTORICAL_FASTPATH_SKIP_TIERS),
            cache_status="historical_fastpath_uncached",
        )
        critical_degraded = {
            str(name).strip()
            for name in (fast_meta.get("degradedTiers") or [])
            if str(name).strip()
        }.intersection(DASHBOARD_CRITICAL_TIERS)
        if not critical_degraded:
            if _is_placeholder_dashboard_snapshot(fast_data, fast_meta):
                logger.warning(
                    "Ignoring placeholder historical fastpath snapshot | key={} cache_status={}",
                    ctx.cache_key,
                    fast_meta.get("cacheStatus"),
                )
            else:
                refresh_started = _ensure_background_dashboard_refresh(
                    ctx,
                    trusted_end_date=trusted_end_iso,
                    write_default_alias=default_request,
                )
                fast_meta["cacheSource"] = "fastpath"
                fast_meta["cacheStatus"] = (
                    "historical_fastpath_while_revalidate"
                    if refresh_started
                    else "historical_fastpath_refresh_inflight"
                )
                fast_meta["isStale"] = True
                return _build_dashboard_api_payload(
                    ctx=ctx,
                    trusted_end_date=trusted_end_iso,
                    dashboard_data=fast_data,
                    dashboard_runtime_meta=fast_meta,
                    requested_from=requested_from,
                    requested_to=requested_to,
                    cache_source="fastpath",
                    freshness_snapshot=freshness_snapshot or {},
                    freshness_source=freshness_source,
                    persisted_read_status=persisted_read_status,
                    persisted_read_ms=persisted_read_ms,
                )

    dashboard_data, dashboard_runtime_meta = get_dashboard_snapshot(ctx, force_refresh=True)
    if _is_placeholder_dashboard_snapshot(dashboard_data, dashboard_runtime_meta):
        logger.warning(
            "Dashboard rebuild returned placeholder snapshot; responding with warming 503 | key={} cache_status={} degraded={}",
            ctx.cache_key,
            dashboard_runtime_meta.get("cacheStatus"),
            dashboard_runtime_meta.get("degradedTiers", []),
        )
        raise DashboardWarmingError("We’re still warming this date range. Please try again shortly.")
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
        freshness_snapshot=freshness_snapshot or {},
        freshness_source=freshness_source,
        persisted_read_status=persisted_read_status,
        persisted_read_ms=persisted_read_ms,
        default_resolution_path="rebuild" if default_request else None,
    )


async def _warm_dashboard_cache() -> None:
    """Warm dashboard cache in background after startup."""
    try:
        loop = asyncio.get_running_loop()
        freshness_snapshot = _dashboard_freshness_snapshot(force_refresh=False)
        ctx = _default_dashboard_context(freshness_snapshot)
        await loop.run_in_executor(None, lambda: get_dashboard_data(ctx))
        logger.info("Dashboard cache warm-up completed")
    except Exception as e:
        logger.warning(f"Dashboard cache warm-up failed: {e}")


async def _materialize_question_cards_once(force: bool = False) -> None:
    """Run question-card materialization off the request path."""
    try:
        loop = asyncio.get_running_loop()
        cards = await loop.run_in_executor(
            None,
            lambda: question_briefs.refresh_question_briefs(force=force),
        )
        logger.info(f"Question cards materialization completed | cards={len(cards)}")
    except Exception as e:
        logger.warning(f"Question cards materialization failed: {e}")


async def _materialize_behavioral_cards_once(force: bool = False) -> None:
    """Run behavioral-card materialization off the request path."""
    try:
        loop = asyncio.get_running_loop()
        payload = await loop.run_in_executor(
            None,
            lambda: behavioral_briefs.refresh_behavioral_briefs(force=force),
        )
        problems = len(payload.get("problemBriefs") or []) if isinstance(payload, dict) else 0
        services = len(payload.get("serviceGapBriefs") or []) if isinstance(payload, dict) else 0
        logger.info(f"Behavioral cards materialization completed | problem_cards={problems} service_cards={services}")
    except Exception as e:
        logger.warning(f"Behavioral cards materialization failed: {e}")


async def _materialize_opportunity_cards_once(force: bool = False) -> None:
    """Run opportunity-card materialization off the request path."""
    try:
        loop = asyncio.get_running_loop()
        cards = await loop.run_in_executor(
            None,
            lambda: opportunity_briefs.refresh_opportunity_briefs(force=force),
        )
        logger.info(f"Opportunity cards materialization completed | cards={len(cards)}")
    except Exception as e:
        logger.warning(f"Opportunity cards materialization failed: {e}")


async def _materialize_topic_overviews_once(force: bool = False) -> None:
    """Run topic-overview materialization off the request path."""
    try:
        loop = asyncio.get_running_loop()
        freshness_snapshot = _dashboard_freshness_snapshot(force_refresh=False)
        ctx = _default_dashboard_context(freshness_snapshot)
        payload = await loop.run_in_executor(
            None,
            lambda: topic_overviews.refresh_topic_overviews(ctx=ctx, force=force),
        )
        items = len(payload.get("items") or []) if isinstance(payload, dict) else 0
        logger.info(f"Topic overviews materialization completed | items={items} window={ctx.cache_key}")
    except Exception as e:
        logger.warning(f"Topic overviews materialization failed: {e}")


def _start_question_cards_scheduler() -> None:
    """Start recurring question-card materialization scheduler."""
    global question_cards_scheduler

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
    value = re.sub(r"^www\.", "", value, flags=re.IGNORECASE)
    lowered = value.lower()
    if lowered.startswith("t.me/"):
        value = value[5:]
    elif lowered.startswith("telegram.me/"):
        value = value[12:]

    value = value.split("?", 1)[0].split("#", 1)[0].strip()
    if value.startswith("@"):
        value = value[1:]

    segments = [segment.strip() for segment in value.split("/") if segment.strip()]
    if not segments:
        return ""

    candidate = segments[0]
    if candidate.lower() == "c":
        candidate = segments[1] if len(segments) > 1 else ""

    candidate = candidate.strip().lower().lstrip("@")
    if not USERNAME_RE.match(candidate):
        return ""
    return candidate


def _canonical_channel_username(handle: str) -> str:
    normalized = (handle or "").strip().lower().lstrip("@")
    return f"@{normalized}" if normalized else ""


async def _try_enrich_channel_metadata(
    channel_uuid: str,
    canonical_username: str,
    fallback_title: Optional[str] = None,
) -> dict | None:
    """
    Best-effort Telegram source resolution for a source row.

    This keeps source creation fast while allowing immediate type resolution when
    the current process has an authorized Telegram session.
    """
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        import os
    except Exception as e:
        logger.warning(f"Telethon unavailable for source resolution: {e}")
        return None

    username = _canonical_channel_username(canonical_username)
    if not username:
        return None

    if not config.has_telegram_runtime_credentials():
        logger.info(f"Source resolution deferred for {username}: Telegram runtime credentials are unavailable")
        return None

    writer = get_supabase_writer()

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
            logger.info(f"Source resolution deferred for {username}: Telegram session is not authorized")
            return None

        entity = await client.get_entity(username)
        metadata, _entity = await resolve_source_metadata(client, username=username, entity=entity)
        if not metadata.get("channel_title") and fallback_title:
            metadata["channel_title"] = fallback_title

        writer.update_channel(channel_uuid, metadata)
        return writer.get_channel_by_id(channel_uuid)
    except Exception as e:
        logger.warning(f"Source resolution failed for {username}: {e}")
        try:
            entity = await client.get_entity(username)
            metadata = minimal_source_metadata_from_entity(
                entity,
                username=username,
                fallback_title=fallback_title,
            )
            writer.update_channel(channel_uuid, metadata)
        except Exception:
            writer.update_channel(
                channel_uuid,
                {
                    "source_type": "pending",
                    "resolution_status": "error",
                    "last_resolution_error": str(e)[:500],
                },
            )
        return writer.get_channel_by_id(channel_uuid)
    finally:
        await client.disconnect()


def _pending_source_payload(*, channel_title: str) -> dict:
    return {
        "source_type": "pending",
        "resolution_status": "pending",
        "last_resolution_error": None,
        "channel_title": channel_title,
    }


def _validate_channel_username(username: str) -> None:
    if not USERNAME_RE.match(username):
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid Telegram source. Use @name, t.me/name, t.me/name/123, "
                "or t.me/c/public_name/123; private numeric t.me/c links are not supported. "
                "Username must be 5-32 chars, letters/digits/underscore, and start with a letter."
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
    return {"status": "ready", "role": APP_ROLE}


@app.get("/api/health")
async def health():
    """Health check."""
    try:
        db.run_single("RETURN 1 AS ok")
        return {"status": "ok", "neo4j": "connected"}
    except Exception as e:
        if config.IS_LOCKED_ENV:
            return {"status": "degraded", "neo4j": "unavailable"}
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
    query_started_at = time.perf_counter()
    try:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            lambda: _build_dashboard_response_payload(from_date, to_date),
        )
        _record_query_timing(
            request,
            query_started_at,
            cache_status=str(response.get("meta", {}).get("cacheStatus") or ""),
        )
        request.state.dashboard_meta = response["meta"]
        return _dashboard_response(response)
    except DashboardWarmingError as e:
        _record_query_timing(request, query_started_at, cache_status="warming")
        logger.warning(f"Dashboard endpoint warming response: {e}")
        raise HTTPException(status_code=503, detail=str(e))
    except TimeoutError as e:
        logger.warning(f"Dashboard endpoint warming timeout: {e}")
        raise HTTPException(
            status_code=503,
            detail="We’re still warming this date range. Please try again shortly.",
        )
    except Exception as e:
        logger.error(f"Dashboard endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/ai/query", dependencies=[Depends(require_analytics_access)])
async def ai_query(request: AIQueryRequest):
    """Deprecated legacy AI endpoint backed by the live dashboard snapshot."""
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


async def _run_ai_helper_chat(
    request: Request,
    payload: AIHelperChatRequest,
):
    session_id = _normalize_ai_session_id(payload.sessionId)
    request_id = str(getattr(request.state, "request_id", "") or "")
    provider = get_ai_helper_provider()
    message = await provider.chat(
        payload.message.strip(),
        session_id=session_id,
        request_id=request_id,
    )
    return {
        "ok": True,
        "sessionId": session_id,
        "message": message.to_dict(),
    }


async def _run_ai_helper_history(
    request: Request,
    limit: int = Query(default=50, ge=1, le=100),
    sessionId: Optional[str] = Query(default=None),
):
    session_id = _normalize_ai_session_id(sessionId)
    request_id = str(getattr(request.state, "request_id", "") or "")
    provider = get_ai_helper_provider()
    messages = await provider.history(limit=limit, session_id=session_id, request_id=request_id)
    return {
        "ok": True,
        "sessionId": session_id,
        "messages": [message.to_dict() for message in messages],
    }


async def _run_ai_helper_reset(
    request: Request,
    payload: AIHelperSessionRequest,
):
    session_id = _normalize_ai_session_id(payload.sessionId)
    request_id = str(getattr(request.state, "request_id", "") or "")
    provider = get_ai_helper_provider()
    reset_at = await provider.reset(session_id=session_id, request_id=request_id)
    return {
        "ok": True,
        "reset": True,
        "sessionId": session_id,
        "timestamp": reset_at,
    }


async def _run_ai_helper_smoke(
    request: Request,
    payload: AIHelperSessionRequest,
):
    smoke_payload = AIHelperChatRequest(
        message="Reply with exactly WEB_HELPER_OK",
        sessionId=payload.sessionId,
    )
    return await _run_ai_helper_chat(request, smoke_payload)


@app.post("/api/ai/chat")
@app.post("/api/ai-helper/chat")
async def ai_helper_chat(
    request: Request,
    payload: AIHelperChatRequest,
    _admin_user: Dict[str, str] = Depends(require_ai_helper_access),
):
    return await _run_ai_helper_chat(request, payload)


@app.get("/api/ai/chat/history")
@app.get("/api/ai-helper/history")
async def ai_helper_history(
    request: Request,
    limit: int = Query(default=50, ge=1, le=100),
    sessionId: Optional[str] = Query(default=None),
    _admin_user: Dict[str, str] = Depends(require_ai_helper_access),
):
    return await _run_ai_helper_history(request, limit=limit, sessionId=sessionId)


@app.post("/api/ai/chat/reset")
@app.post("/api/ai-helper/reset")
async def ai_helper_reset(
    request: Request,
    payload: AIHelperSessionRequest,
    _admin_user: Dict[str, str] = Depends(require_ai_helper_access),
):
    return await _run_ai_helper_reset(request, payload)


@app.post("/api/ai/chat/smoke")
async def ai_helper_smoke(
    request: Request,
    payload: AIHelperSessionRequest,
    _admin_user: Dict[str, str] = Depends(require_ai_helper_access),
):
    return await _run_ai_helper_smoke(request, payload)


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
        loop = asyncio.get_running_loop()
        query_started_at = time.perf_counter()
        payload = await loop.run_in_executor(None, lambda: get_topics_page(page, size, ctx))
        _record_query_timing(request, query_started_at)
        return payload
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
        loop = asyncio.get_running_loop()
        query_started_at = time.perf_counter()
        payload = await loop.run_in_executor(None, lambda: get_topic_detail(topic, category, ctx))
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
        loop = asyncio.get_running_loop()
        query_started_at = time.perf_counter()
        payload = await loop.run_in_executor(
            None,
            lambda: get_topic_evidence_page(topic, category, normalized_view, page, size, focus_id, ctx),
        )
        _record_query_timing(request, query_started_at)
        if payload is None:
            raise HTTPException(status_code=404, detail="Topic not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Topic evidence endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/channels", dependencies=[Depends(require_analytics_access)])
async def channels(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Channels detail page."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: get_channels_page(ctx))
    except Exception as e:
        logger.error(f"Channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/channels/detail", dependencies=[Depends(require_analytics_access)])
async def channel_detail(
    channel: str = Query(..., min_length=1),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Single channel detail payload with recent posts and distributions."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        loop = asyncio.get_running_loop()
        payload = await loop.run_in_executor(None, lambda: get_channel_detail(channel, ctx))
        if payload is None:
            raise HTTPException(status_code=404, detail="Channel not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Channel detail endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/channels/posts", dependencies=[Depends(require_analytics_access)])
async def channel_posts(
    channel: str = Query(..., min_length=1),
    page: int = Query(0, ge=0),
    size: int = Query(20, ge=1, le=50),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Paginated recent posts feed for a selected channel."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        loop = asyncio.get_running_loop()
        payload = await loop.run_in_executor(None, lambda: get_channel_posts_page(channel, page, size, ctx))
        if payload is None:
            raise HTTPException(status_code=404, detail="Channel not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Channel posts endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/graph", dependencies=[Depends(require_analytics_access)])
async def graph_data(payload: GraphRequest):
    """Graph dataset for /graph page (server-side Neo4j)."""
    try:
        filters = payload.model_dump(exclude_none=True)
        graph = graph_dashboard.get_graph_data(filters)
        freshness = get_freshness_snapshot(
            get_supabase_writer(),
            scheduler_status=get_current_scraper_scheduler_status(),
            prefer_shared_snapshot=not _should_run_background_jobs(),
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
        sentiment_filters = [label.strip() for label in (sentiments or "").split(",") if label.strip()]
        details = graph_dashboard.get_node_details(
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
        return graph_dashboard.search_graph(query, limit)
    except Exception as e:
        logger.error(f"Graph search endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trending-topics", dependencies=[Depends(require_analytics_access)])
async def trending_topics(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Top trending topics for graph filters."""
    try:
        return graph_dashboard.get_trending_topics(limit, timeframe)
    except Exception as e:
        logger.error(f"Trending topics endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-channels", dependencies=[Depends(require_analytics_access)])
async def top_channels(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Top channels by post activity (graph context)."""
    try:
        return graph_dashboard.get_top_channels(limit, timeframe)
    except Exception as e:
        logger.error(f"Top channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/all-channels", dependencies=[Depends(require_analytics_access)])
async def all_channels_graph():
    """All channels list for graph filters."""
    try:
        return graph_dashboard.get_all_channels()
    except Exception as e:
        logger.error(f"All channels endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/top-brands", dependencies=[Depends(require_analytics_access)])
async def top_brands_compat(limit: int = Query(10, ge=1, le=100), timeframe: str = Query("Last 7 Days")):
    """Compatibility endpoint: returns top channels in legacy shape."""
    try:
        return graph_dashboard.get_top_channels(limit, timeframe)
    except Exception as e:
        logger.error(f"Top brands compatibility endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/all-brands", dependencies=[Depends(require_analytics_access)])
async def all_brands_compat():
    """Compatibility endpoint: returns all channels in legacy shape."""
    try:
        return graph_dashboard.get_all_channels()
    except Exception as e:
        logger.error(f"All brands compatibility endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sentiment-distribution", dependencies=[Depends(require_analytics_access)])
async def sentiment_distribution(timeframe: str = Query("Last 7 Days")):
    """Sentiment distribution for graph side panels/filters."""
    try:
        return graph_dashboard.get_sentiment_distribution(timeframe)
    except Exception as e:
        logger.error(f"Sentiment distribution endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/graph-insights", dependencies=[Depends(require_analytics_access)])
async def graph_insights(timeframe: str = Query("Last 7 Days")):
    """Narrative summary for graph context."""
    try:
        return graph_dashboard.get_graph_insights(timeframe)
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
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: insights.get_insight_cards(payload.filters or {}, audience),
        )
    except Exception as e:
        logger.error(f"Insight cards endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/config", dependencies=[Depends(require_operator_access)])
async def get_admin_config():
    """Return merged Admin config with defaults and runtime overrides."""
    try:
        return _admin_config_response()
    except Exception as e:
        logger.error(f"Get admin config error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/admin/config", dependencies=[Depends(require_operator_access)])
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


@app.get("/api/sources/channels", dependencies=[Depends(require_operator_access)])
async def list_channel_sources():
    """List configured Telegram channel sources from Supabase."""
    try:
        items = get_supabase_writer().list_channels()
        return {"count": len(items), "items": items}
    except Exception as e:
        logger.error(f"List source channels error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/social/overview", dependencies=[Depends(require_operator_access)])
async def get_social_overview():
    try:
        overview = get_social_store().get_overview()
        overview["runtime"] = get_current_social_runtime_status()
        return overview
    except Exception as e:
        logger.error(f"Social overview error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/social/activities", dependencies=[Depends(require_operator_access)])
async def list_social_activities(
    limit: int = Query(100, ge=1, le=500),
    entity_id: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
):
    try:
        items = get_social_store().list_activities(limit=limit, entity_id=entity_id, platform=platform)
        return {"count": len(items), "items": items}
    except Exception as e:
        logger.error(f"Social activities error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/social/entities", dependencies=[Depends(require_operator_access)])
async def list_social_entities():
    try:
        items = get_social_store().list_entities()
        return {"count": len(items), "items": items}
    except Exception as e:
        logger.error(f"Social entities error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/social/entities", dependencies=[Depends(require_operator_access)])
async def create_social_entity(payload: SocialEntityCreateRequest):
    try:
        store = get_social_store()
        entity = store.ensure_entity_from_company(payload.legacy_company_id)
        if payload.is_active is not None or payload.accounts:
            entity = store.update_entity(
                entity["id"],
                is_active=payload.is_active,
                accounts=[account.model_dump() for account in payload.accounts],
            )
        return {"item": entity}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as e:
        logger.error(f"Create social entity error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/social/entities/{entity_id}", dependencies=[Depends(require_operator_access)])
async def update_social_entity(entity_id: str, payload: SocialEntityUpdateRequest):
    try:
        store = get_social_store()
        existing = store.get_entity(entity_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Social entity not found")
        if payload.is_active is None and payload.metadata is None and not payload.accounts:
            raise HTTPException(status_code=400, detail="No update fields provided")
        item = store.update_entity(
            entity_id,
            is_active=payload.is_active,
            metadata=payload.metadata,
            accounts=[account.model_dump() for account in payload.accounts],
        )
        return {"item": item}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update social entity error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/social/runtime/status", dependencies=[Depends(require_operator_access)])
async def get_social_runtime_status():
    return get_current_social_runtime_status()


@app.post("/api/social/runtime/start", dependencies=[Depends(require_operator_access)])
async def start_social_runtime():
    try:
        if not _should_run_social_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: _enqueue_social_runtime_control("start"))
        return await get_social_runtime().start()
    except Exception as e:
        logger.error(f"Social runtime start error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/social/runtime/stop", dependencies=[Depends(require_operator_access)])
async def stop_social_runtime():
    try:
        if not _should_run_social_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: _enqueue_social_runtime_control("stop"))
        return await get_social_runtime().stop()
    except Exception as e:
        logger.error(f"Social runtime stop error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/social/runtime", dependencies=[Depends(require_operator_access)])
async def update_social_runtime(payload: SocialRuntimeUpdateRequest):
    try:
        if not _should_run_social_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                lambda: _enqueue_social_runtime_control(
                    "set_interval",
                    interval_minutes=payload.interval_minutes,
                ),
            )
        return await get_social_runtime().set_interval(payload.interval_minutes)
    except Exception as e:
        logger.error(f"Social runtime update error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/social/runtime/run-once", dependencies=[Depends(require_operator_access)])
async def run_social_runtime_once():
    try:
        if not _should_run_social_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: _enqueue_social_runtime_control("run_once"))
        return await get_social_runtime().run_once()
    except Exception as e:
        logger.error(f"Social runtime run-once error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/social/runtime/failures", dependencies=[Depends(require_operator_access)])
async def list_social_runtime_failures(
    dead_letter_only: bool = Query(False),
    stage: Optional[str] = Query(default=None),
    limit: int = Query(100, ge=1, le=500),
):
    try:
        items = get_social_store().list_failures(
            dead_letter_only=dead_letter_only,
            stage=stage,
            limit=limit,
        )
        return {"count": len(items), "items": items}
    except Exception as e:
        logger.error(f"Social runtime failures error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/social/runtime/retry", dependencies=[Depends(require_operator_access)])
async def retry_social_runtime_failure(payload: SocialRuntimeRetryRequest):
    try:
        if not _should_run_social_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                lambda: _enqueue_social_runtime_control(
                    "retry",
                    stage=payload.stage,
                    scope_key=payload.scope_key,
                ),
            )
        return await get_social_runtime().retry_failure(stage=payload.stage, scope_key=payload.scope_key)
    except ValueError as exc:
        status_code = 404 if "not found" in str(exc).lower() or "no active failure" in str(exc).lower() else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as e:
        logger.error(f"Social runtime retry error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/social/runtime/replay", dependencies=[Depends(require_operator_access)])
async def replay_social_runtime_items(payload: SocialRuntimeReplayRequest):
    try:
        if not _should_run_social_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                lambda: _enqueue_social_runtime_control(
                    "replay",
                    stage=payload.stage,
                    activity_uids=payload.activity_uids,
                ),
            )
        return await get_social_runtime().replay_activities(
            stage=payload.stage,
            activity_uids=payload.activity_uids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as e:
        logger.error(f"Social runtime replay error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _social_unavailable(detail: str, error: Exception) -> HTTPException:
    logger.error(f"{detail}: {error}")
    return HTTPException(status_code=503, detail=str(error))


@app.get("/api/social/intelligence/summary", dependencies=[Depends(require_operator_access)])
async def get_social_intelligence_summary(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    entity_id: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
):
    try:
        return get_social_store().get_intelligence_summary(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
        )
    except Exception as e:
        raise _social_unavailable("Social intelligence summary error", e) from e


@app.get("/api/social/intelligence/topic-timeline", dependencies=[Depends(require_operator_access)])
async def get_social_intelligence_topic_timeline(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    entity_id: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
    topic: Optional[str] = Query(default=None),
    bucket: str = Query(default="day"),
):
    try:
        return get_social_store().get_topic_timeline(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
            topic=topic,
            bucket=bucket,
        )
    except Exception as e:
        raise _social_unavailable("Social intelligence topic timeline error", e) from e


@app.get("/api/social/intelligence/topics", dependencies=[Depends(require_operator_access)])
async def get_social_intelligence_topics(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    entity_id: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
):
    try:
        return get_social_store().get_topic_intelligence(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
            limit=limit,
        )
    except Exception as e:
        raise _social_unavailable("Social intelligence topics error", e) from e


@app.get("/api/social/intelligence/ads", dependencies=[Depends(require_operator_access)])
async def get_social_intelligence_ads(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    entity_id: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
    cta_type: Optional[str] = Query(default=None),
    content_format: Optional[str] = Query(default=None),
    sort: str = Query(default="recent"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=100),
):
    try:
        return get_social_store().get_ad_intelligence(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
            cta_type=cta_type,
            content_format=content_format,
            sort=sort,
            page=page,
            size=size,
        )
    except Exception as e:
        raise _social_unavailable("Social intelligence ads error", e) from e


@app.get("/api/social/intelligence/audience-response", dependencies=[Depends(require_operator_access)])
async def get_social_intelligence_audience_response(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    entity_id: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
    limit: int = Query(default=10, ge=1, le=50),
):
    try:
        return get_social_store().get_audience_response(
            from_date=from_date,
            to_date=to_date,
            entity_id=entity_id,
            platform=platform,
            limit=limit,
        )
    except Exception as e:
        raise _social_unavailable("Social intelligence audience response error", e) from e


@app.get("/api/social/intelligence/competitors", dependencies=[Depends(require_operator_access)])
async def get_social_intelligence_competitors(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    platform: Optional[str] = Query(default=None),
    sort_by: str = Query(default="posts"),
    sort_dir: str = Query(default="desc"),
):
    try:
        return get_social_store().get_competitor_scorecard(
            from_date=from_date,
            to_date=to_date,
            platform=platform,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
    except Exception as e:
        raise _social_unavailable("Social intelligence competitors error", e) from e


@app.get("/api/social/intelligence/evidence", dependencies=[Depends(require_operator_access)])
async def get_social_intelligence_evidence(
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
    activity_uid: Optional[str] = Query(default=None),
    entity_id: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
    topic: Optional[str] = Query(default=None),
    marketing_intent: Optional[str] = Query(default=None),
    pain_point: Optional[str] = Query(default=None),
    customer_intent: Optional[str] = Query(default=None),
    source_kind: Optional[str] = Query(default=None),
    cta_type: Optional[str] = Query(default=None),
    content_format: Optional[str] = Query(default=None),
    sentiment: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=25, ge=1, le=100),
):
    try:
        return get_social_store().get_intelligence_evidence(
            from_date=from_date,
            to_date=to_date,
            activity_uid=activity_uid,
            entity_id=entity_id,
            platform=platform,
            topic=topic,
            marketing_intent=marketing_intent,
            pain_point=pain_point,
            customer_intent=customer_intent,
            source_kind=source_kind,
            cta_type=cta_type,
            content_format=content_format,
            sentiment=sentiment,
            page=page,
            size=size,
        )
    except Exception as e:
        raise _social_unavailable("Social intelligence evidence error", e) from e


@app.post("/api/sources/channels", dependencies=[Depends(require_operator_access)])
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
            if str(existing.get("resolution_status") or "").strip().lower() != "resolved":
                pending_title = (update_payload.get("channel_title") or channel_title or canonical_username).strip() or canonical_username
                if config.FEATURE_SOURCE_RESOLUTION_QUEUE:
                    update_payload.update(build_pending_source_payload(channel_title=pending_title))
                else:
                    update_payload.update(_pending_source_payload(channel_title=pending_title))
            action = "exists"
            if not existing.get("is_active", False):
                update_payload["is_active"] = True
                action = "reactivated"

            updated = writer.update_channel(existing["id"], update_payload)
            if updated and config.FEATURE_SOURCE_RESOLUTION_QUEUE:
                if str(updated.get("resolution_status") or "").strip().lower() != "resolved":
                    ensure_resolution_job(writer, updated)
                updated = writer.get_channel_by_id(updated["id"]) or updated
            elif updated:
                inline_resolved = await _try_enrich_channel_metadata(
                    updated["id"],
                    updated.get("channel_username") or canonical_username,
                    updated.get("channel_title"),
                )
                updated = inline_resolved or writer.get_channel_by_id(updated["id"]) or updated
            return {"action": action, "item": updated}

        create_payload = {
            "channel_username": canonical_username,
            "is_active": True,
            "scrape_depth_days": payload.scrape_depth_days,
            "scrape_comments": payload.scrape_comments,
        }
        if config.FEATURE_SOURCE_RESOLUTION_QUEUE:
            create_payload.update(build_pending_source_payload(channel_title=channel_title))
        else:
            create_payload.update(_pending_source_payload(channel_title=channel_title))
        created = writer.create_channel(
            create_payload
        )
        if config.FEATURE_SOURCE_RESOLUTION_QUEUE:
            ensure_resolution_job(writer, created)
            created = writer.get_channel_by_id(created["id"]) or created
        else:
            inline_resolved = await _try_enrich_channel_metadata(
                created["id"],
                created.get("channel_username") or canonical_username,
                created.get("channel_title"),
            )
            created = inline_resolved or writer.get_channel_by_id(created["id"]) or created
        return {"action": "created", "item": created}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create source channel error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/sources/channels/{channel_id}", dependencies=[Depends(require_operator_access)])
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
            if config.FEATURE_SOURCE_RESOLUTION_QUEUE:
                if str(updated.get("resolution_status") or "").strip().lower() != "resolved":
                    writer.update_channel(
                        channel_id,
                        build_pending_source_payload(
                            channel_title=(updated.get("channel_title") or updated.get("channel_username") or "").strip()
                            or ""
                        ),
                    )
                    updated = writer.get_channel_by_id(channel_id) or updated
                    ensure_resolution_job(writer, updated)
                updated = writer.get_channel_by_id(updated["id"]) or updated
            else:
                if str(updated.get("resolution_status") or "").strip().lower() != "resolved":
                    writer.update_channel(
                        channel_id,
                        _pending_source_payload(
                            channel_title=(updated.get("channel_title") or updated.get("channel_username") or "").strip()
                            or ""
                        ),
                    )
                    updated = writer.get_channel_by_id(channel_id) or updated
                inline_resolved = await _try_enrich_channel_metadata(
                    updated["id"],
                    updated.get("channel_username") or "",
                    updated.get("channel_title"),
                )
                updated = inline_resolved or writer.get_channel_by_id(updated["id"]) or updated
        return {"item": updated}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update source channel error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sources/resolution", dependencies=[Depends(require_operator_access)])
async def get_source_resolution_status():
    """Current source resolution worker status and queue snapshot."""
    return get_current_scraper_scheduler_status().get("resolution") or {}


@app.post("/api/sources/resolution/run-once", dependencies=[Depends(require_operator_access)])
async def run_source_resolution_once():
    """Trigger one immediate source resolution cycle."""
    try:
        return await get_scraper_scheduler().run_source_resolution_once()
    except Exception as e:
        logger.error(f"Run-once source resolution error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/sources/resolution/backfill-peer-refs", dependencies=[Depends(require_operator_access)])
async def backfill_source_peer_refs(active_only: bool = True, limit: int = 100):
    """Queue resolution jobs for sources that still lack cached peer refs."""
    try:
        capped_limit = max(1, min(int(limit), 1000))
        queued = enqueue_missing_peer_ref_backfill(
            get_supabase_writer(),
            session_slot="primary",
            active_only=bool(active_only),
            limit=capped_limit,
        )
        return {
            "queued": queued,
            "active_only": bool(active_only),
            "limit": capped_limit,
            "resolution": get_current_scraper_scheduler_status().get("resolution") or {},
        }
    except Exception as e:
        logger.error(f"Backfill source peer refs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/scraper/scheduler", dependencies=[Depends(require_operator_access)])
async def get_scraper_scheduler_status():
    """Current scraper scheduler runtime status."""
    return get_current_scraper_scheduler_status()


@app.get("/api/freshness", dependencies=[Depends(require_analytics_access)])
async def freshness_snapshot(force: bool = Query(False)):
    """Pipeline freshness/truth snapshot with backlog and Supabase↔Neo4j drift."""
    try:
        return await _resolve_freshness_snapshot(force_refresh=force)
    except Exception as e:
        logger.error(f"Freshness endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/quality/taxonomy", dependencies=[Depends(require_analytics_access)])
async def taxonomy_quality_snapshot():
    """Enterprise taxonomy quality snapshot with sign-off gates."""
    try:
        loop = asyncio.get_running_loop()
        snapshot = await loop.run_in_executor(None, _taxonomy_quality_snapshot)
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
        loop = asyncio.get_running_loop()

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

        return await loop.run_in_executor(None, _build_snapshot)
    except Exception as e:
        logger.error(f"Trending widget quality endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/start", dependencies=[Depends(require_operator_access)])
async def start_scraper_scheduler():
    """Start recurring scraper schedule using persisted interval."""
    try:
        if not _should_run_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: _enqueue_worker_scheduler_control("start"))
        return await get_scraper_scheduler().start()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Start scraper scheduler error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/stop", dependencies=[Depends(require_operator_access)])
async def stop_scraper_scheduler():
    """Stop recurring scraper schedule."""
    try:
        if not _should_run_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: _enqueue_worker_scheduler_control("stop"))
        return await get_scraper_scheduler().stop()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Stop scraper scheduler error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/scraper/scheduler", dependencies=[Depends(require_operator_access)])
async def update_scraper_scheduler(payload: ScraperSchedulerUpdateRequest):
    """Update scraper scheduler interval in minutes."""
    try:
        if not _should_run_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                None,
                lambda: _enqueue_worker_scheduler_control(
                    "set_interval",
                    interval_minutes=payload.interval_minutes,
                ),
            )
        return await get_scraper_scheduler().set_interval(payload.interval_minutes)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update scraper scheduler error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/run-once", dependencies=[Depends(require_operator_access)])
async def run_scraper_once():
    """Trigger one immediate scrape cycle."""
    try:
        if not _should_run_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: _enqueue_worker_scheduler_control("run_once"))
        return await get_scraper_scheduler().run_once()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Run-once scraper error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/scraper/scheduler/catchup-once", dependencies=[Depends(require_operator_access)])
async def run_scraper_catchup_once():
    """Trigger one immediate processing/sync-heavy catch-up cycle (no scraping)."""
    try:
        if not _should_run_background_jobs():
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, lambda: _enqueue_worker_scheduler_control("catchup_once"))
        return await get_scraper_scheduler().run_catchup_once()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Catchup-once scraper error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ai/failures", dependencies=[Depends(require_operator_access)])
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


@app.post("/api/ai/failures/retry", dependencies=[Depends(require_operator_access)])
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


@app.get("/api/taxonomy/proposals", dependencies=[Depends(require_operator_access)])
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


@app.get("/api/taxonomy/trending-new", dependencies=[Depends(require_operator_access)])
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


@app.post("/api/taxonomy/proposals/review", dependencies=[Depends(require_operator_access)])
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


@app.get("/api/taxonomy/promotions", dependencies=[Depends(require_operator_access)])
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


@app.post("/api/taxonomy/promotions/reload", dependencies=[Depends(require_operator_access)])
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
    page: int = Query(0, ge=0),
    size: int = Query(500, ge=1, le=1000),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Audience detail page — paginated."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: get_audience_page(page, size, ctx))
    except Exception as e:
        logger.error(f"Audience endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/audience/detail", dependencies=[Depends(require_analytics_access)])
async def audience_detail(
    user_id: str = Query(..., alias="userId", min_length=1),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Single audience-member detail payload."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        loop = asyncio.get_running_loop()
        payload = await loop.run_in_executor(None, lambda: get_audience_detail(user_id, ctx))
        if payload is None:
            raise HTTPException(status_code=404, detail="Audience member not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Audience detail endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/audience/messages", dependencies=[Depends(require_analytics_access)])
async def audience_messages(
    user_id: str = Query(..., alias="userId", min_length=1),
    page: int = Query(0, ge=0),
    size: int = Query(20, ge=1, le=50),
    from_date: Optional[str] = Query(default=None, alias="from"),
    to_date: Optional[str] = Query(default=None, alias="to"),
):
    """Paginated recent messages feed for a selected audience member."""
    try:
        ctx = build_dashboard_date_context(from_date, to_date) if from_date and to_date else _default_dashboard_context()
        loop = asyncio.get_running_loop()
        payload = await loop.run_in_executor(None, lambda: get_audience_messages_page(user_id, page, size, ctx))
        if payload is None:
            raise HTTPException(status_code=404, detail="Audience member not found for the selected window.")
        return payload
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Audience messages endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/cache/clear", dependencies=[Depends(require_operator_access)])
async def clear_cache():
    """Invalidate the in-memory dashboard cache."""
    invalidate_cache()
    graph_dashboard.invalidate_graph_cache()
    question_briefs.invalidate_question_briefs_cache()
    behavioral_briefs.invalidate_behavioral_briefs_cache()
    opportunity_briefs.invalidate_opportunity_briefs_cache()
    topic_overviews.invalidate_topic_overviews_cache()
    return {"success": True, "message": "Cache cleared"}


@app.post("/api/question-briefs/debug/refresh", dependencies=[Depends(require_debug_endpoint_access)])
async def debug_refresh_question_briefs():
    """Force-refresh question cards and return stage diagnostics."""
    try:
        loop = asyncio.get_running_loop()
        diagnostics = await loop.run_in_executor(
            None,
            lambda: question_briefs.refresh_question_briefs_with_diagnostics(force=True),
        )
        return {
            "success": True,
            "cardsProduced": diagnostics.get("cardsProduced", 0),
            "firstRejectionBucket": diagnostics.get("firstRejectionBucket", ""),
            "diagnostics": diagnostics,
        }
    except Exception as e:
        logger.error(f"Question briefs debug refresh endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/behavioral-briefs/debug/refresh", dependencies=[Depends(require_debug_endpoint_access)])
async def debug_refresh_behavioral_briefs():
    """Force-refresh behavioral cards and return stage diagnostics."""
    try:
        loop = asyncio.get_running_loop()
        diagnostics = await loop.run_in_executor(
            None,
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


@app.post("/api/opportunity-briefs/debug/refresh", dependencies=[Depends(require_debug_endpoint_access)])
async def debug_refresh_opportunity_briefs():
    """Force-refresh opportunity cards and return stage diagnostics."""
    try:
        loop = asyncio.get_running_loop()
        diagnostics = await loop.run_in_executor(
            None,
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

# ─────────────────────────────────────────────────────────────────────────────
# KNOWLEDGE BASE (RAG) ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

from fastapi import UploadFile, File, Form
from api.knowledge_base import (
    KBVectorStore, GeminiEmbedder, make_kb_components,
    ingest, hybrid_search, generate_answer,
    _build_context, _confidence_level,
    ParseError, UnsupportedFormatError,
)

def _kb_components() -> tuple[KBVectorStore, GeminiEmbedder]:
    """Lazy singleton — created on first KB call."""
    if not config.GEMINI_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="GEMINI_API_KEY is not configured. Add it to your .env and restart.",
        )
    try:
        return make_kb_components(
            gemini_api_key=config.GEMINI_API_KEY,
            storage_path=config.KB_STORAGE_PATH,
            embed_dim=config.KB_EMBED_DIM,
        )
    except ImportError as exc:
        logger.warning(f"KB runtime unavailable: {exc}")
        raise HTTPException(
            status_code=503,
            detail=f"Knowledge base runtime dependencies are unavailable. {exc}",
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(f"KB runtime bootstrap failed: {exc}")
        raise HTTPException(
            status_code=503,
            detail="Knowledge base runtime is unavailable. Check KB dependencies and configuration, then restart the backend.",
        ) from exc


def _kb_openai_model() -> str:
    return config.KB_GENERATION_MODEL or config.OPENAI_MODEL


_kb_openclaw_provider: OpenClawAiHelperProvider | None = None
_kb_openclaw_lock = threading.Lock()


def _get_kb_openclaw_provider() -> OpenClawAiHelperProvider | None:
    """Return a dedicated OpenClaw provider for KB generation, or None if not configured."""
    global _kb_openclaw_provider
    if str(config.OPENCLAW_GATEWAY_TRANSPORT or "").strip().lower() == "cli_bridge":
        return None
    if not config.OPENCLAW_GATEWAY_BASE_URL or not config.OPENCLAW_GATEWAY_TOKEN:
        return None
    if not config.OPENCLAW_KB_SESSION_KEY:
        return None
    with _kb_openclaw_lock:
        if _kb_openclaw_provider is None:
            _kb_openclaw_provider = OpenClawAiHelperProvider(
                base_url=config.OPENCLAW_GATEWAY_BASE_URL,
                gateway_token=config.OPENCLAW_GATEWAY_TOKEN,
                agent_id=config.OPENCLAW_ANALYTICS_AGENT_ID,
                session_key=config.OPENCLAW_KB_SESSION_KEY,
                timeout_seconds=config.OPENCLAW_HELPER_TIMEOUT_SECONDS,
                connect_timeout_seconds=config.OPENCLAW_HELPER_CONNECT_TIMEOUT_SECONDS,
                read_timeout_seconds=config.OPENCLAW_HELPER_READ_TIMEOUT_SECONDS,
                retry_attempts=config.OPENCLAW_HELPER_RETRY_ATTEMPTS,
                transport=config.OPENCLAW_GATEWAY_TRANSPORT,
                model=config.OPENCLAW_GATEWAY_MODEL,
                manage_transcript=False,
            )
        return _kb_openclaw_provider


# ── Collections ───────────────────────────────────────────────────────────────

class KBCollectionCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    description: str = Field("", max_length=300)


@app.post("/api/kb/collections", dependencies=[Depends(require_kb_access)])
async def kb_create_collection(body: KBCollectionCreate):
    """Create a named knowledge base collection."""
    store, _ = _kb_components()
    try:
        store.get_or_create_collection(body.name, body.description)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"name": body.name, "description": body.description, "created": True}


@app.get("/api/kb/collections", dependencies=[Depends(require_kb_access)])
async def kb_list_collections():
    """List all knowledge base collections with stats."""
    store, _ = _kb_components()
    return {"collections": store.list_collections()}


@app.delete("/api/kb/collections/{collection_name}", dependencies=[Depends(require_kb_access)])
async def kb_delete_collection(collection_name: str):
    """Delete a collection and all its documents."""
    store, _ = _kb_components()
    store.delete_collection(collection_name)
    return {"deleted": collection_name}


# ── Documents ─────────────────────────────────────────────────────────────────

@app.post("/api/kb/collections/{collection_name}/upload", dependencies=[Depends(require_kb_access)])
async def kb_upload_document(
    collection_name: str,
    file: UploadFile = File(...),
    doc_title: str = Form(""),
):
    """Upload a document (PDF, DOCX, TXT, MD) and index it."""
    max_bytes = config.KB_UPLOAD_MAX_MB * 1024 * 1024
    data = await file.read()
    if len(data) > max_bytes:
        raise HTTPException(status_code=413, detail=f"File exceeds {config.KB_UPLOAD_MAX_MB}MB limit.")

    store, embedder = _kb_components()
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None,
            lambda: ingest(
                file_path_or_url=file.filename or "upload",
                collection_name=collection_name,
                store=store,
                embedder=embedder,
                chunk_size=config.KB_CHUNK_SIZE,
                chunk_overlap=config.KB_CHUNK_OVERLAP,
                doc_title=doc_title,
                data=data,
                filename=file.filename or "",
            ),
        )
    except UnsupportedFormatError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except ParseError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.error(f"KB ingest error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    return result


class KBAddUrlBody(BaseModel):
    url: str = Field(..., min_length=10, max_length=2048)
    doc_title: str = Field("", max_length=200)


@app.post("/api/kb/collections/{collection_name}/add-url", dependencies=[Depends(require_kb_access)])
async def kb_add_url(collection_name: str, body: KBAddUrlBody):
    """Fetch a URL and index its content."""
    store, embedder = _kb_components()
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None,
            lambda: ingest(
                file_path_or_url=body.url,
                collection_name=collection_name,
                store=store,
                embedder=embedder,
                chunk_size=config.KB_CHUNK_SIZE,
                chunk_overlap=config.KB_CHUNK_OVERLAP,
                doc_title=body.doc_title,
            ),
        )
    except ParseError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        logger.error(f"KB add-url error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    return result


@app.get("/api/kb/collections/{collection_name}/documents", dependencies=[Depends(require_kb_access)])
async def kb_list_documents(collection_name: str):
    """List all documents in a collection."""
    store, _ = _kb_components()
    try:
        coll = store._client.get_collection(collection_name)
        if coll.count() == 0:
            return {"documents": []}
        all_data = coll.get(include=["metadatas"])
        metas = all_data.get("metadatas", []) or []
        seen: dict[str, dict] = {}
        for m in metas:
            if not m:
                continue
            doc_id = m.get("doc_id", "")
            if doc_id and doc_id not in seen:
                seen[doc_id] = {
                    "doc_id": doc_id,
                    "doc_title": m.get("doc_title", "Unknown"),
                    "source": m.get("source", ""),
                    "source_type": m.get("source_type", ""),
                    "ingested_at": m.get("ingested_at", ""),
                }
        docs = list(seen.values())
        # Count chunks per doc
        chunk_counts: dict[str, int] = {}
        for m in metas:
            if m:
                did = m.get("doc_id", "")
                chunk_counts[did] = chunk_counts.get(did, 0) + 1
        for doc in docs:
            doc["chunk_count"] = chunk_counts.get(doc["doc_id"], 0)
        return {"documents": docs}
    except Exception:
        raise HTTPException(status_code=404, detail=f"Collection not found: {collection_name}")


@app.delete("/api/kb/documents/{doc_id}", dependencies=[Depends(require_kb_access)])
async def kb_delete_document(doc_id: str, collection: str = Query(...)):
    """Delete a document from a collection by doc_id."""
    store, _ = _kb_components()
    deleted = store.delete_document(collection, doc_id)
    return {"doc_id": doc_id, "chunks_deleted": deleted}


# ── Query ─────────────────────────────────────────────────────────────────────

class KBAskBody(BaseModel):
    question: str = Field(..., min_length=3, max_length=500)
    collection: str = Field("default", min_length=1, max_length=80)


@app.post("/api/kb/ask", dependencies=[Depends(require_kb_access)])
async def kb_ask(body: KBAskBody):
    """Answer a question grounded in the knowledge base with citations."""
    store, embedder = _kb_components()
    loop = asyncio.get_running_loop()

    # Step 1: Retrieve chunks via hybrid search (unchanged)
    try:
        chunks = await loop.run_in_executor(
            None,
            lambda: hybrid_search(store, embedder, body.collection, body.question, config.KB_TOP_K),
        )
    except Exception as exc:
        logger.error(f"KB search error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

    if not chunks:
        return {
            "answer": "No relevant content found in this knowledge base for your question.",
            "citations": [],
            "confidence": "low_confidence",
            "caveat": "The collection may be empty. Try uploading documents first.",
        }

    # Step 2: Try OpenClaw for generation, fall back to direct OpenAI
    provider = _get_kb_openclaw_provider()
    if provider is not None:
        try:
            context = _build_context(chunks)
            prompt = (
                "Answer the following question using ONLY the context below. "
                "Cite sources inline as [Source: <title>, p.<page>]. "
                "If the answer cannot be found in the context, say so clearly. "
                "Be concise and direct.\n\n"
                f"Context:\n{context}\n\n"
                f"Question: {body.question}"
            )
            msg = await provider.chat(prompt)
            answer = msg.text

            # Build citations and confidence from retrieved chunks
            top_score = chunks[0]["score"] if chunks else 0.0
            confidence = _confidence_level(top_score)
            caveat = (
                "Note: Retrieved context may not fully address this question."
                if confidence == "low_confidence" else ""
            )

            seen: set[tuple] = set()
            citations = []
            for chunk in chunks:
                meta = chunk.get("metadata", {})
                key = (meta.get("doc_title", ""), meta.get("page", ""))
                if key not in seen:
                    seen.add(key)
                    citations.append({
                        "doc_title": meta.get("doc_title", "Unknown"),
                        "page": meta.get("page", "?"),
                        "doc_id": meta.get("doc_id", ""),
                        "source": meta.get("source", ""),
                    })

            return {"answer": answer, "citations": citations, "confidence": confidence, "caveat": caveat}
        except Exception as exc:
            logger.warning(f"KB OpenClaw generation failed, falling back to OpenAI: {exc}")

    # Fallback: direct OpenAI (same as original behavior)
    try:
        result = await loop.run_in_executor(
            None,
            lambda: generate_answer(
                question=body.question,
                chunks=chunks,
                openai_api_key=config.OPENAI_API_KEY,
                model=_kb_openai_model(),
            ),
        )
    except Exception as exc:
        logger.error(f"KB generate error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

    return result


@app.get("/api/kb/search", dependencies=[Depends(require_kb_access)])
async def kb_search(
    collection: str = Query(...),
    q: str = Query(..., min_length=2, max_length=300),
    top_k: int = Query(5, ge=1, le=20),
):
    """Semantic + keyword search returning ranked snippets."""
    store, embedder = _kb_components()
    loop = asyncio.get_running_loop()
    try:
        chunks = await loop.run_in_executor(
            None,
            lambda: hybrid_search(store, embedder, collection, q, top_k),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {
        "query": q,
        "collection": collection,
        "results": [
            {
                "text": c["text"][:500],
                "score": c["score"],
                "doc_title": c.get("metadata", {}).get("doc_title", ""),
                "page": c.get("metadata", {}).get("page", ""),
                "source_type": c.get("metadata", {}).get("source_type", ""),
            }
            for c in chunks
        ],
    }


@app.post("/api/topic-overviews/debug/refresh", dependencies=[Depends(require_debug_endpoint_access)])
async def debug_refresh_topic_overviews():
    """Force-refresh topic overviews and return stage diagnostics."""
    try:
        loop = asyncio.get_running_loop()
        freshness_snapshot = _dashboard_freshness_snapshot(force_refresh=False)
        ctx = _default_dashboard_context(freshness_snapshot)
        diagnostics = await loop.run_in_executor(
            None,
            lambda: topic_overviews.refresh_topic_overviews_with_diagnostics(ctx=ctx, force=True),
        )
        return {
            "success": True,
            "itemsProduced": diagnostics.get("itemsProduced", 0),
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
