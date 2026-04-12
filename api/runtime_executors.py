from __future__ import annotations

import asyncio
import os
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable, TypeVar

from loguru import logger

T = TypeVar("T")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def _default_api_workers() -> int:
    cpu = os.cpu_count() or 1
    return min(16, max(4, cpu * 4))


def _default_background_workers() -> int:
    cpu = os.cpu_count() or 1
    return min(4, max(1, cpu))


API_EXECUTOR_MAX_WORKERS = max(1, _env_int("API_EXECUTOR_MAX_WORKERS", _default_api_workers()))
BACKGROUND_EXECUTOR_MAX_WORKERS = max(
    1,
    _env_int("BACKGROUND_EXECUTOR_MAX_WORKERS", _default_background_workers()),
)

_request_executor = ThreadPoolExecutor(
    max_workers=API_EXECUTOR_MAX_WORKERS,
    thread_name_prefix="api-request",
)
_background_executor = ThreadPoolExecutor(
    max_workers=BACKGROUND_EXECUTOR_MAX_WORKERS,
    thread_name_prefix="api-background",
)
_draining_lock = threading.Lock()
_draining = False


def request_executor_workers() -> int:
    return API_EXECUTOR_MAX_WORKERS


def background_executor_workers() -> int:
    return BACKGROUND_EXECUTOR_MAX_WORKERS


def log_executor_configuration() -> None:
    logger.info(
        "Runtime executors ready | request_workers={} background_workers={}",
        API_EXECUTOR_MAX_WORKERS,
        BACKGROUND_EXECUTOR_MAX_WORKERS,
    )


def mark_draining(value: bool) -> None:
    global _draining
    with _draining_lock:
        _draining = bool(value)


def is_draining() -> bool:
    with _draining_lock:
        return bool(_draining)


async def run_request(fn: Callable[[], T]) -> T:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_request_executor, fn)


async def run_background(fn: Callable[[], T]) -> T:
    if is_draining():
        raise RuntimeError("Background executor is draining")
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_background_executor, fn)


def submit_background(fn: Callable[[], T]) -> Future[T]:
    if is_draining():
        raise RuntimeError("Background executor is draining")
    return _background_executor.submit(fn)


def shutdown_background_executor(*, wait: bool = True) -> None:
    try:
        _background_executor.shutdown(wait=wait, cancel_futures=False)
    except TypeError:
        _background_executor.shutdown(wait=wait)


def shutdown_request_executor(*, wait: bool = True) -> None:
    try:
        _request_executor.shutdown(wait=wait, cancel_futures=False)
    except TypeError:
        _request_executor.shutdown(wait=wait)
