"""Shared runtime-config helpers for the lightweight Admin page."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
import threading
import time
from typing import Any

from loguru import logger

from buffer.supabase_writer import SupabaseWriter

ADMIN_CONFIG_PATH = "admin/config.json"
ADMIN_CONFIG_CACHE_TTL_SECONDS = 5.0
ADMIN_CONFIG_READ_TIMEOUT_SECONDS = 8.0
ADMIN_CONFIG_SAVE_TIMEOUT_SECONDS = 8.0

_runtime_store_lock = threading.Lock()
_runtime_store: SupabaseWriter | None = None
_config_cache_lock = threading.Lock()
_config_cache: dict[str, Any] | None = None
_config_cache_ts: float = 0.0
_io_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="admin-runtime")


def _get_runtime_store() -> SupabaseWriter | None:
    global _runtime_store
    with _runtime_store_lock:
        if _runtime_store is not None:
            return _runtime_store
        try:
            _runtime_store = SupabaseWriter()
        except Exception as exc:
            logger.warning(f"Admin runtime store unavailable: {exc}")
            _runtime_store = None
    return _runtime_store


def load_admin_config_raw() -> dict[str, Any]:
    global _config_cache, _config_cache_ts
    now = time.time()
    with _config_cache_lock:
        if _config_cache is not None and (now - _config_cache_ts) < ADMIN_CONFIG_CACHE_TTL_SECONDS:
            return dict(_config_cache)

    store = _get_runtime_store()
    if not store:
        with _config_cache_lock:
            return dict(_config_cache or {})

    try:
        future = _io_executor.submit(store.get_runtime_json, ADMIN_CONFIG_PATH, {})
        payload = future.result(timeout=ADMIN_CONFIG_READ_TIMEOUT_SECONDS)
        config_payload = payload if isinstance(payload, dict) else {}
    except FutureTimeoutError:
        logger.warning("Admin config read timed out; using in-memory/default config")
        with _config_cache_lock:
            return dict(_config_cache or {})
    except Exception as exc:
        logger.warning(f"Admin config read failed; using in-memory/default config ({exc})")
        with _config_cache_lock:
            return dict(_config_cache or {})

    with _config_cache_lock:
        _config_cache = dict(config_payload)
        _config_cache_ts = now

    return dict(config_payload)


def save_admin_config_raw(payload: dict[str, Any]) -> bool:
    global _config_cache, _config_cache_ts
    data = payload if isinstance(payload, dict) else {}
    with _config_cache_lock:
        _config_cache = dict(data)
        _config_cache_ts = time.time()

    store = _get_runtime_store()
    if not store:
        logger.warning("Admin config store unavailable; cannot persist admin config")
        return False

    try:
        future = _io_executor.submit(store.save_runtime_json, ADMIN_CONFIG_PATH, data)
        saved = future.result(timeout=ADMIN_CONFIG_SAVE_TIMEOUT_SECONDS)
        if not saved:
            logger.warning("Admin config save failed in runtime storage")
        return bool(saved)
    except FutureTimeoutError:
        logger.warning("Admin config save timed out; cannot confirm persistence")
        return False
    except Exception as exc:
        logger.warning(f"Admin config save failed; cannot confirm persistence ({exc})")
        return False


def get_admin_prompt(prompt_key: str, default: str) -> str:
    key = str(prompt_key or "").strip()
    if not key:
        return default
    payload = load_admin_config_raw()
    prompts = payload.get("prompts") if isinstance(payload.get("prompts"), dict) else {}
    value = prompts.get(key)
    if isinstance(value, str):
        text = value.strip()
        if text:
            return text
    return default


def get_admin_runtime_value(key: str, default: Any) -> Any:
    name = str(key or "").strip()
    if not name:
        return default
    payload = load_admin_config_raw()
    runtime = payload.get("runtime") if isinstance(payload.get("runtime"), dict) else {}
    if name not in runtime:
        return default
    value = runtime.get(name)
    if value is None:
        return default
    return value
