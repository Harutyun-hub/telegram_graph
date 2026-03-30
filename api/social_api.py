from __future__ import annotations

import asyncio
import hmac
import threading
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from neo4j import GraphDatabase
from pydantic import BaseModel, Field
from supabase import Client

import config
from buffer.supabase_writer import SupabaseWriter
from social.runtime import SocialRuntimeService
from social.store import SocialStore

router = APIRouter(prefix="/api/social", tags=["social"])

_primary_auth_client: Client | None = None
_social_store: SocialStore | None = None
_social_runtime: SocialRuntimeService | None = None
_social_probe_lock = threading.Lock()
_social_probe_cache: dict[str, Any] | None = None
_SOCIAL_PROBE_TTL_SECONDS = 30.0


class SocialSubsystemUnavailable(RuntimeError):
    """Raised when the Social data plane is not configured or reachable."""


def _extract_bearer_token(raw_value: str | None) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    scheme, _, token = text.partition(" ")
    if scheme.lower() == "bearer" and token.strip():
        return token.strip()
    return ""


def _auth_clients() -> list[Client]:
    global _primary_auth_client
    clients: list[Client] = []

    if _primary_auth_client is None and config.SUPABASE_URL and config.SUPABASE_SERVICE_ROLE_KEY:
        _primary_auth_client = SupabaseWriter().client
    if _primary_auth_client is not None:
        clients.append(_primary_auth_client)

    social_url = str(config.SOCIAL_SUPABASE_URL or "").strip()
    social_key = str(config.SOCIAL_SUPABASE_SERVICE_ROLE_KEY or "").strip()
    if social_url and social_key:
        try:
            store_client = get_social_store().client
        except Exception:
            store_client = None
        if store_client is not None and all(store_client is not client for client in clients):
            clients.append(store_client)
    return clients


def _validate_operator_session(token: str) -> dict[str, str] | None:
    for client in _auth_clients():
        try:
            response = client.auth.get_user(token)
        except Exception:
            continue
        user = getattr(response, "user", None)
        if user is None:
            continue
        user_id = str(getattr(user, "id", "") or "").strip()
        if not user_id:
            continue
        return {
            "id": user_id,
            "email": str(getattr(user, "email", "") or "").strip().lower(),
            "auth": "supabase",
        }
    return None


async def require_operator_access(
    x_supabase_authorization: Optional[str] = Header(default=None, alias="X-Supabase-Authorization"),
    x_admin_authorization: Optional[str] = Header(default=None, alias="X-Admin-Authorization"),
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, str]:
    admin_api_key = str(getattr(config, "ADMIN_API_KEY", "") or "").strip()
    admin_token = _extract_bearer_token(x_admin_authorization) or _extract_bearer_token(authorization)
    if admin_api_key and admin_token and hmac.compare_digest(admin_token, admin_api_key):
        return {"id": "admin-api-key", "email": "", "auth": "api_key"}

    session_token = _extract_bearer_token(x_supabase_authorization) or _extract_bearer_token(authorization)
    if session_token:
        operator = await asyncio.to_thread(_validate_operator_session, session_token)
        if operator:
            return operator

    if not admin_api_key and not config.IS_LOCKED_ENV:
        return {"id": "local-dev", "email": "", "auth": "local_dev"}

    raise HTTPException(status_code=401, detail="Operator authentication required.")


def _social_required_config() -> dict[str, str]:
    return {
        "SOCIAL_SUPABASE_URL": str(config.SOCIAL_SUPABASE_URL or "").strip(),
        "SOCIAL_SUPABASE_SERVICE_ROLE_KEY": str(config.SOCIAL_SUPABASE_SERVICE_ROLE_KEY or "").strip(),
        "SOCIAL_NEO4J_URI": str(config.SOCIAL_NEO4J_URI or "").strip(),
        "SOCIAL_NEO4J_USERNAME": str(config.SOCIAL_NEO4J_USERNAME or "").strip(),
        "SOCIAL_NEO4J_PASSWORD": str(config.SOCIAL_NEO4J_PASSWORD or "").strip(),
        "SOCIAL_NEO4J_DATABASE": str(config.SOCIAL_NEO4J_DATABASE or "").strip(),
    }


def _probe_social_neo4j() -> None:
    uri = str(config.SOCIAL_NEO4J_URI or "").strip()
    if uri.startswith("neo4j+s://"):
        uri = uri.replace("neo4j+s://", "neo4j+ssc://")
    driver = GraphDatabase.driver(
        uri,
        auth=(config.SOCIAL_NEO4J_USERNAME, config.SOCIAL_NEO4J_PASSWORD),
    )
    try:
        with driver.session(database=config.SOCIAL_NEO4J_DATABASE) as session:
            session.run("RETURN 1 AS ok").single()
    finally:
        driver.close()


def probe_social_subsystem_status(*, force: bool = False) -> dict[str, Any]:
    global _social_probe_cache
    now = time.monotonic()
    with _social_probe_lock:
        if (
            not force
            and _social_probe_cache is not None
            and (now - float(_social_probe_cache.get("checked_monotonic") or 0.0)) < _SOCIAL_PROBE_TTL_SECONDS
        ):
            return dict(_social_probe_cache)

        required = _social_required_config()
        missing = [name for name, value in required.items() if not value]
        status: dict[str, Any] = {
            "available": False,
            "checked_monotonic": now,
            "missing": missing,
            "detail": "",
        }

        if missing:
            status["detail"] = f"Missing required Social configuration: {', '.join(missing)}"
            _social_probe_cache = status
            return dict(status)

        try:
            SocialStore().ping()
            _probe_social_neo4j()
        except Exception as exc:
            status["detail"] = f"Social data plane connectivity failed: {exc}"
            _social_probe_cache = status
            return dict(status)

        status["available"] = True
        status["detail"] = "Social data plane is ready"
        _social_probe_cache = status
        return dict(status)


def _raise_social_unavailable(detail: str) -> None:
    raise HTTPException(status_code=503, detail=detail)


def _require_social_available() -> None:
    status = probe_social_subsystem_status()
    if not status.get("available"):
        _raise_social_unavailable(
            str(status.get("detail") or "Social data plane is unavailable."),
        )


def get_social_store() -> SocialStore:
    global _social_store
    _require_social_available()
    if _social_store is None:
        _social_store = SocialStore()
    return _social_store


def get_social_runtime() -> SocialRuntimeService:
    global _social_runtime
    _require_social_available()
    if _social_runtime is None:
        _social_runtime = SocialRuntimeService(get_social_store())
    return _social_runtime


def _ensure_runtime_enabled() -> None:
    if not config.SOCIAL_RUNTIME_ENABLED:
        raise HTTPException(status_code=409, detail="Social runtime is disabled for this environment.")


async def startup_social_runtime() -> None:
    status = probe_social_subsystem_status(force=True)
    if not status.get("available"):
        return
    if config.SOCIAL_RUNTIME_ENABLED:
        await get_social_runtime().startup()


async def shutdown_social_runtime() -> None:
    global _social_runtime, _social_store
    if _social_runtime is not None:
        await _social_runtime.shutdown()
        _social_runtime = None
    _social_store = None


def get_current_social_runtime_status() -> dict[str, Any]:
    runtime_enabled = bool(config.SOCIAL_RUNTIME_ENABLED)
    if _social_runtime is None:
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
            "runtime_enabled": runtime_enabled,
            "tiktok_enabled": bool(config.SOCIAL_TIKTOK_ENABLED),
            "postgres_worker_enabled": bool(config.SOCIAL_DATABASE_URL),
            "worker_id": None,
        }
    return get_social_runtime().status()


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


class SocialRuntimeReplayRequest(BaseModel):
    stage: str = Field(default="analysis", min_length=1, max_length=32)
    activity_uids: List[str] = Field(..., min_length=1)


@router.get("/overview")
async def get_social_overview():
    _require_social_available()
    overview = get_social_store().get_overview()
    overview["runtime"] = get_current_social_runtime_status()
    return overview


@router.get("/activities")
async def list_social_activities(
    limit: int = Query(100, ge=1, le=500),
    entity_id: Optional[str] = Query(default=None),
    platform: Optional[str] = Query(default=None),
):
    _require_social_available()
    items = get_social_store().list_activities(limit=limit, entity_id=entity_id, platform=platform)
    return {"count": len(items), "items": items}


@router.get("/entities")
async def list_social_entities():
    _require_social_available()
    items = get_social_store().list_entities()
    return {"count": len(items), "items": items}


@router.get("/runtime/status")
async def get_social_runtime_status():
    _require_social_available()
    return get_current_social_runtime_status()


@router.get("/runtime/failures")
async def list_social_runtime_failures(
    dead_letter_only: bool = Query(False),
    stage: Optional[str] = Query(default=None),
    limit: int = Query(100, ge=1, le=500),
):
    _require_social_available()
    items = get_social_store().list_failures(
        dead_letter_only=dead_letter_only,
        stage=stage,
        limit=limit,
    )
    return {"count": len(items), "items": items}


@router.post("/entities", dependencies=[Depends(require_operator_access)])
async def create_social_entity(payload: SocialEntityCreateRequest):
    _require_social_available()
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


@router.patch("/entities/{entity_id}", dependencies=[Depends(require_operator_access)])
async def update_social_entity(entity_id: str, payload: SocialEntityUpdateRequest):
    _require_social_available()
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


@router.post("/runtime/run-once", dependencies=[Depends(require_operator_access)])
async def run_social_runtime_once():
    _ensure_runtime_enabled()
    return await get_social_runtime().run_once()


@router.post("/runtime/retry", dependencies=[Depends(require_operator_access)])
async def retry_social_runtime_failure(payload: SocialRuntimeRetryRequest):
    _ensure_runtime_enabled()
    try:
        return await get_social_runtime().retry_failure(stage=payload.stage, scope_key=payload.scope_key)
    except ValueError as exc:
        status_code = 404 if "not found" in str(exc).lower() or "no active failure" in str(exc).lower() else 400
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/runtime/replay", dependencies=[Depends(require_operator_access)])
async def replay_social_runtime_items(payload: SocialRuntimeReplayRequest):
    _ensure_runtime_enabled()
    try:
        return await get_social_runtime().replay_activities(
            stage=payload.stage,
            activity_uids=payload.activity_uids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
