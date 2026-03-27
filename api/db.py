"""
db.py — Shared Neo4j driver management for API and background work.

This module keeps one long-lived driver per runtime lane (`request`,
`background`) and adds guarded reset/retry behavior for transient Aura routing
and connection failures.
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any, Callable, Dict, TypeVar

import config
from loguru import logger
from neo4j import GraphDatabase, ManagedTransaction
from neo4j.exceptions import ConfigurationError, DriverError, Neo4jError, ServiceUnavailable, SessionExpired

try:
    from neo4j.debug import watch as neo4j_debug_watch
except Exception:  # pragma: no cover - optional API availability depends on driver build
    neo4j_debug_watch = None

T = TypeVar("T")

REQUEST_DRIVER_KEY = "request"
BACKGROUND_DRIVER_KEY = "background"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


NEO4J_REQUEST_POOL_SIZE = max(1, _env_int("NEO4J_REQUEST_POOL_SIZE", 12))
NEO4J_BACKGROUND_POOL_SIZE = max(1, _env_int("NEO4J_BACKGROUND_POOL_SIZE", 4))
NEO4J_CONNECTION_TIMEOUT_SECONDS = max(1.0, _env_float("NEO4J_CONNECTION_TIMEOUT_SECONDS", 15.0))
NEO4J_CONNECTION_WRITE_TIMEOUT_SECONDS = max(
    1.0,
    _env_float("NEO4J_CONNECTION_WRITE_TIMEOUT_SECONDS", 30.0),
)
NEO4J_CONNECTION_ACQUISITION_TIMEOUT_SECONDS = max(
    1.0,
    _env_float("NEO4J_CONNECTION_ACQUISITION_TIMEOUT_SECONDS", 15.0),
)
NEO4J_MAX_CONNECTION_LIFETIME_SECONDS = max(
    60.0,
    _env_float("NEO4J_MAX_CONNECTION_LIFETIME_SECONDS", 1800.0),
)
NEO4J_LIVENESS_CHECK_TIMEOUT_SECONDS = max(
    1.0,
    _env_float("NEO4J_LIVENESS_CHECK_TIMEOUT_SECONDS", 60.0),
)
NEO4J_MAX_TRANSACTION_RETRY_TIME_SECONDS = max(
    1.0,
    _env_float("NEO4J_MAX_TRANSACTION_RETRY_TIME_SECONDS", 30.0),
)
NEO4J_DRIVER_RESET_COOLDOWN_SECONDS = max(
    1.0,
    _env_float("NEO4J_DRIVER_RESET_COOLDOWN_SECONDS", 30.0),
)
NEO4J_SLOW_QUERY_MS = max(0.0, _env_float("NEO4J_SLOW_QUERY_MS", 750.0))
NEO4J_DEBUG_WATCH = _env_bool("NEO4J_DEBUG_WATCH", False)


def _normalized_uri() -> str:
    uri = config.NEO4J_URI
    if uri.startswith("neo4j+s://"):
        return uri.replace("neo4j+s://", "neo4j+ssc://")
    return uri


def _error_text(exc: BaseException) -> str:
    return str(exc).strip()


def _is_retryable_driver_error(exc: BaseException) -> bool:
    if isinstance(exc, (ServiceUnavailable, SessionExpired)):
        return True
    if isinstance(exc, Neo4jError) and getattr(exc, "is_retryable", lambda: False)():
        return True
    message = _error_text(exc).lower()
    markers = (
        "defunct connection",
        "routing information",
        "unable to retrieve routing information",
        "failed to read from defunct connection",
        "failed to obtain a connection from the pool",
        "connection pool",
        "session expired",
        "service unavailable",
    )
    return any(marker in message for marker in markers)


class Neo4jDriverManager:
    def __init__(self) -> None:
        self._drivers: Dict[str, Any] = {}
        self._driver_lock = threading.Lock()
        self._reset_locks: Dict[str, threading.Lock] = {}
        self._reset_cooldowns: Dict[str, float] = {}
        self._debug_watch_enabled = False
        self._enable_debug_watch_if_requested()

    def _enable_debug_watch_if_requested(self) -> None:
        if not NEO4J_DEBUG_WATCH or neo4j_debug_watch is None or self._debug_watch_enabled:
            return
        neo4j_debug_watch("neo4j.pool", "neo4j.io")
        self._debug_watch_enabled = True
        logger.warning("Neo4j low-level debug watch enabled | channels=neo4j.pool,neo4j.io")

    def _reset_lock(self, driver_key: str) -> threading.Lock:
        with self._driver_lock:
            lock = self._reset_locks.get(driver_key)
            if lock is None:
                lock = threading.Lock()
                self._reset_locks[driver_key] = lock
            return lock

    def _pool_size_for(self, driver_key: str) -> int:
        if driver_key == BACKGROUND_DRIVER_KEY:
            return NEO4J_BACKGROUND_POOL_SIZE
        return NEO4J_REQUEST_POOL_SIZE

    def _driver_kwargs_for(self, driver_key: str) -> dict[str, Any]:
        return {
            "auth": (config.NEO4J_USERNAME, config.NEO4J_PASSWORD),
            "max_connection_pool_size": self._pool_size_for(driver_key),
            "connection_timeout": NEO4J_CONNECTION_TIMEOUT_SECONDS,
            "connection_write_timeout": NEO4J_CONNECTION_WRITE_TIMEOUT_SECONDS,
            "connection_acquisition_timeout": NEO4J_CONNECTION_ACQUISITION_TIMEOUT_SECONDS,
            "max_connection_lifetime": NEO4J_MAX_CONNECTION_LIFETIME_SECONDS,
            "liveness_check_timeout": NEO4J_LIVENESS_CHECK_TIMEOUT_SECONDS,
            "max_transaction_retry_time": NEO4J_MAX_TRANSACTION_RETRY_TIME_SECONDS,
            "keep_alive": True,
        }

    def _create_driver(self, driver_key: str):
        started_at = time.perf_counter()
        driver_kwargs = dict(self._driver_kwargs_for(driver_key))
        while True:
            try:
                driver = GraphDatabase.driver(_normalized_uri(), **driver_kwargs)
                break
            except ConfigurationError as exc:
                message = _error_text(exc)
                if "Unexpected config keys:" not in message:
                    raise
                unsupported = [
                    part.strip()
                    for part in message.split("Unexpected config keys:", 1)[1].split(",")
                    if part.strip()
                ]
                removed = False
                for key in unsupported:
                    if key in driver_kwargs:
                        driver_kwargs.pop(key, None)
                        removed = True
                if not removed:
                    raise
                logger.warning(
                    "Neo4j driver config fallback | driver={} unsupported_keys={}",
                    driver_key,
                    unsupported,
                )
        driver.verify_connectivity()
        connect_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.info(
            "Neo4j driver connected | driver={} pool={} connect_ms={}",
            driver_key,
            self._pool_size_for(driver_key),
            connect_ms,
        )
        return driver

    def get_driver(self, driver_key: str = REQUEST_DRIVER_KEY):
        driver = self._drivers.get(driver_key)
        if driver is not None:
            return driver
        with self._reset_lock(driver_key):
            driver = self._drivers.get(driver_key)
            if driver is not None:
                return driver
            driver = self._create_driver(driver_key)
            self._drivers[driver_key] = driver
            self._reset_cooldowns[driver_key] = time.monotonic()
            return driver

    def reset_driver(self, driver_key: str, reason: BaseException | str) -> bool:
        lock = self._reset_lock(driver_key)
        with lock:
            now = time.monotonic()
            last_reset = self._reset_cooldowns.get(driver_key, 0.0)
            if (now - last_reset) < NEO4J_DRIVER_RESET_COOLDOWN_SECONDS and self._drivers.get(driver_key) is not None:
                logger.warning(
                    "Neo4j driver reset skipped due to cooldown | driver={} cooldown_s={} reason={}",
                    driver_key,
                    NEO4J_DRIVER_RESET_COOLDOWN_SECONDS,
                    _error_text(reason if isinstance(reason, BaseException) else RuntimeError(str(reason))),
                )
                return False

            old_driver = self._drivers.pop(driver_key, None)
            if old_driver is not None:
                try:
                    old_driver.close()
                except Exception:
                    pass

            driver = self._create_driver(driver_key)
            self._drivers[driver_key] = driver
            self._reset_cooldowns[driver_key] = time.monotonic()
            logger.warning(
                "Neo4j driver reset completed | driver={} reason={}",
                driver_key,
                _error_text(reason if isinstance(reason, BaseException) else RuntimeError(str(reason))),
            )
            return True

    def execute_read(
        self,
        work: Callable[[ManagedTransaction], T],
        *,
        driver_key: str = REQUEST_DRIVER_KEY,
        op_name: str = "read",
    ) -> T:
        return self._execute(work, driver_key=driver_key, op_name=op_name, access_mode="read")

    def execute_write(
        self,
        work: Callable[[ManagedTransaction], T],
        *,
        driver_key: str = BACKGROUND_DRIVER_KEY,
        op_name: str = "write",
    ) -> T:
        return self._execute(work, driver_key=driver_key, op_name=op_name, access_mode="write")

    def _execute(
        self,
        work: Callable[[ManagedTransaction], T],
        *,
        driver_key: str,
        op_name: str,
        access_mode: str,
    ) -> T:
        retry_count = 0
        while True:
            session_started_at = time.perf_counter()
            query_started_at = None
            session_open_ms = 0.0
            try:
                driver = self.get_driver(driver_key)
                with driver.session(database=config.NEO4J_DATABASE) as session:
                    session_open_ms = round((time.perf_counter() - session_started_at) * 1000, 2)
                    query_started_at = time.perf_counter()
                    if access_mode == "write":
                        result = session.execute_write(work)
                    else:
                        result = session.execute_read(work)
                    query_ms = round((time.perf_counter() - query_started_at) * 1000, 2)
                    if query_ms >= NEO4J_SLOW_QUERY_MS:
                        logger.warning(
                            "Neo4j slow {} | driver={} op={} retry_count={} neo4jSessionOpenMs={} neo4jQueryMs={}",
                            access_mode,
                            driver_key,
                            op_name,
                            retry_count,
                            session_open_ms,
                            query_ms,
                        )
                    else:
                        logger.debug(
                            "Neo4j {} complete | driver={} op={} retry_count={} neo4jSessionOpenMs={} neo4jQueryMs={}",
                            access_mode,
                            driver_key,
                            op_name,
                            retry_count,
                            session_open_ms,
                            query_ms,
                        )
                    return result
            except Exception as exc:
                query_ms = 0.0
                if query_started_at is not None:
                    query_ms = round((time.perf_counter() - query_started_at) * 1000, 2)
                logger.warning(
                    "Neo4j {} failed | driver={} op={} retry_count={} neo4jSessionOpenMs={} neo4jQueryMs={} neo4jErrorClass={} error={}",
                    access_mode,
                    driver_key,
                    op_name,
                    retry_count,
                    round(session_open_ms, 2),
                    round(query_ms, 2),
                    exc.__class__.__name__,
                    _error_text(exc),
                )
                if retry_count == 0 and _is_retryable_driver_error(exc):
                    did_reset = self.reset_driver(driver_key, exc)
                    if did_reset:
                        retry_count += 1
                        continue
                raise

    def close(self, driver_key: str | None = None) -> None:
        keys = [driver_key] if driver_key else list(self._drivers.keys())
        for key in keys:
            driver = self._drivers.pop(key, None)
            if driver is None:
                continue
            try:
                driver.close()
            except Exception as exc:
                logger.debug("Neo4j driver close skipped | driver={} error={}", key, exc)


_driver_manager = Neo4jDriverManager()


def get_driver_manager() -> Neo4jDriverManager:
    return _driver_manager


def get_driver(driver_key: str = REQUEST_DRIVER_KEY):
    return _driver_manager.get_driver(driver_key)


def get_background_driver():
    return _driver_manager.get_driver(BACKGROUND_DRIVER_KEY)


def execute_read(
    work: Callable[[ManagedTransaction], T],
    *,
    driver_key: str = REQUEST_DRIVER_KEY,
    op_name: str = "read",
) -> T:
    return _driver_manager.execute_read(work, driver_key=driver_key, op_name=op_name)


def execute_write(
    work: Callable[[ManagedTransaction], T],
    *,
    driver_key: str = BACKGROUND_DRIVER_KEY,
    op_name: str = "write",
) -> T:
    return _driver_manager.execute_write(work, driver_key=driver_key, op_name=op_name)


def run_query(
    cypher: str,
    params: dict | None = None,
    *,
    driver_key: str = REQUEST_DRIVER_KEY,
    op_name: str = "run_query",
) -> list[dict]:
    payload = dict(params or {})

    def _work(tx: ManagedTransaction) -> list[dict]:
        return [dict(record) for record in tx.run(cypher, payload)]

    return execute_read(_work, driver_key=driver_key, op_name=op_name)


def run_single(
    cypher: str,
    params: dict | None = None,
    *,
    driver_key: str = REQUEST_DRIVER_KEY,
    op_name: str = "run_single",
) -> dict | None:
    rows = run_query(cypher, params, driver_key=driver_key, op_name=op_name)
    return rows[0] if rows else None


def close(driver_key: str | None = None) -> None:
    _driver_manager.close(driver_key)
