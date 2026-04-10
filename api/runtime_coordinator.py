from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from loguru import logger

import config

try:
    import redis
except ImportError:  # pragma: no cover - optional in local dev until installed
    redis = None


_LOCAL_LOCK = threading.Lock()
_LOCAL_LOCKS: dict[str, tuple[str, float]] = {}
_LOCAL_COUNTERS: dict[str, tuple[int, float]] = {}
_LOCAL_JSON: dict[str, tuple[str, float]] = {}
_COORDINATOR_LOCK = threading.Lock()
_COORDINATOR: "RuntimeCoordinator | None" = None

_LOCK_RELEASE_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
"""


class RuntimeCoordinator:
    def __init__(self) -> None:
        self._client = None
        self._warned_disabled = False

        redis_url = str(getattr(config, "REDIS_URL", "") or "").strip()
        if not redis_url:
            return
        if redis is None:
            logger.warning("REDIS_URL is configured but redis package is not installed; falling back to local coordination")
            return

        try:
            self._client = redis.Redis.from_url(redis_url, decode_responses=True)
        except Exception as exc:  # pragma: no cover - depends on local redis setup
            logger.warning(f"Runtime coordinator failed to initialize Redis client: {exc}")
            self._client = None

    @property
    def enabled(self) -> bool:
        return self._client is not None

    def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            return bool(self._client.ping())
        except Exception as exc:  # pragma: no cover - network/runtime specific
            logger.warning(f"Runtime coordinator Redis ping failed: {exc}")
            return False

    def acquire_lock(self, name: str, ttl_seconds: int) -> str | None:
        key = f"coord:lock:{name}"
        ttl = max(1, int(ttl_seconds))
        token = str(uuid.uuid4())

        if self._client is not None:
            try:
                acquired = self._client.set(key, token, nx=True, ex=ttl)
                return token if acquired else None
            except Exception as exc:  # pragma: no cover - network/runtime specific
                logger.warning(f"Runtime coordinator Redis lock fallback for {name}: {exc}")

        now = time.monotonic()
        with _LOCAL_LOCK:
            current = _LOCAL_LOCKS.get(key)
            if current is not None:
                current_token, expires_at = current
                if expires_at > now and current_token:
                    return None
            _LOCAL_LOCKS[key] = (token, now + ttl)
        return token

    def release_lock(self, name: str, token: str | None) -> None:
        if not token:
            return

        key = f"coord:lock:{name}"
        if self._client is not None:
            try:
                self._client.eval(_LOCK_RELEASE_LUA, 1, key, token)
                return
            except Exception as exc:  # pragma: no cover - network/runtime specific
                logger.warning(f"Runtime coordinator Redis unlock fallback for {name}: {exc}")

        with _LOCAL_LOCK:
            current = _LOCAL_LOCKS.get(key)
            if current is None:
                return
            current_token, _expires_at = current
            if current_token == token:
                _LOCAL_LOCKS.pop(key, None)

    def increment_window_counter(self, name: str, window_seconds: int) -> int:
        key = f"coord:counter:{name}"
        ttl = max(1, int(window_seconds))

        if self._client is not None:
            try:
                count = int(self._client.incr(key))
                if count == 1:
                    self._client.expire(key, ttl)
                return count
            except Exception as exc:  # pragma: no cover - network/runtime specific
                logger.warning(f"Runtime coordinator Redis counter fallback for {name}: {exc}")

        now = time.monotonic()
        with _LOCAL_LOCK:
            count, expires_at = _LOCAL_COUNTERS.get(key, (0, now + ttl))
            if expires_at <= now:
                count = 0
                expires_at = now + ttl
            count += 1
            _LOCAL_COUNTERS[key] = (count, expires_at)
            return count

    def get_json(self, name: str) -> Any | None:
        key = f"coord:json:{name}"
        if self._client is not None:
            try:
                raw = self._client.get(key)
                if raw is None:
                    return None
                return raw
            except Exception as exc:  # pragma: no cover - network/runtime specific
                logger.warning(f"Runtime coordinator Redis read failed for {name}: {exc}")

        now = time.monotonic()
        with _LOCAL_LOCK:
            current = _LOCAL_JSON.get(key)
            if current is None:
                return None
            value, expires_at = current
            if expires_at <= now:
                _LOCAL_JSON.pop(key, None)
                return None
            return value

    def set_json(self, name: str, value: str, ttl_seconds: int) -> bool:
        key = f"coord:json:{name}"
        ttl = max(1, int(ttl_seconds))
        if self._client is not None:
            try:
                self._client.set(key, value, ex=ttl)
                return True
            except Exception as exc:  # pragma: no cover - network/runtime specific
                logger.warning(f"Runtime coordinator Redis write failed for {name}: {exc}")

        with _LOCAL_LOCK:
            _LOCAL_JSON[key] = (value, time.monotonic() + ttl)
        return True

    def delete_json(self, name: str) -> bool:
        key = f"coord:json:{name}"
        if self._client is not None:
            try:
                self._client.delete(key)
                return True
            except Exception as exc:  # pragma: no cover - network/runtime specific
                logger.warning(f"Runtime coordinator Redis delete failed for {name}: {exc}")

        with _LOCAL_LOCK:
            removed = _LOCAL_JSON.pop(key, None)
        return removed is not None


def get_runtime_coordinator() -> RuntimeCoordinator:
    global _COORDINATOR
    with _COORDINATOR_LOCK:
        if _COORDINATOR is None:
            _COORDINATOR = RuntimeCoordinator()
        return _COORDINATOR
