from __future__ import annotations

import json
import os
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = os.getenv("PRODUCTION_BASE_URL", "").strip().rstrip("/")
API_KEY = os.getenv("ANALYTICS_API_KEY_FRONTEND", "").strip()
CONCURRENCY = max(1, int(os.getenv("MIXED_LOAD_CONCURRENCY", "10")))
DURATION_SECONDS = max(10, int(os.getenv("MIXED_LOAD_DURATION_SECONDS", "120")))
TIMEOUT_SECONDS = max(1.0, float(os.getenv("MIXED_LOAD_TIMEOUT_SECONDS", "15")))

DEFAULT_QUERY = {
    "from": os.getenv("MIXED_LOAD_FROM", "2026-03-12"),
    "to": os.getenv("MIXED_LOAD_TO", "2026-03-26"),
}


@dataclass
class Sample:
    endpoint: str
    status_code: int
    elapsed_ms: float


def _headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    return headers


def _request(method: str, path: str, *, params: dict[str, Any] | None = None) -> tuple[int, bytes]:
    if not BASE_URL:
        raise RuntimeError("PRODUCTION_BASE_URL is required")
    url = f"{BASE_URL}{path}"
    if params:
        url = f"{url}?{urlencode(params, doseq=True)}"
    request = Request(url, method=method, headers=_headers())
    with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return int(response.status), response.read()


def _json_request(method: str, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    status, body = _request(method, path, params=params)
    if not body:
        return {"status_code": status}
    payload = json.loads(body.decode("utf-8"))
    if isinstance(payload, dict):
        payload.setdefault("status_code", status)
        return payload
    return {"status_code": status, "payload": payload}


def _wait_for_running_cycle(timeout_seconds: float = 90.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        status = _json_request("GET", "/api/scraper/scheduler")
        if status.get("running_now"):
            return status
        time.sleep(2.0)
    raise TimeoutError("scheduler did not enter running state")


def _probe_once(endpoint: str, params: dict[str, Any]) -> Sample:
    started_at = time.perf_counter()
    try:
        status_code, _body = _request("GET", endpoint, params=params)
        return Sample(endpoint=endpoint, status_code=status_code, elapsed_ms=(time.perf_counter() - started_at) * 1000)
    except HTTPError as exc:
        return Sample(endpoint=endpoint, status_code=int(exc.code), elapsed_ms=(time.perf_counter() - started_at) * 1000)
    except (URLError, TimeoutError):
        return Sample(endpoint=endpoint, status_code=0, elapsed_ms=(time.perf_counter() - started_at) * 1000)


def _run_endpoint_load(endpoint: str, params: dict[str, Any], results: list[Sample], stop_at: float) -> None:
    while time.monotonic() < stop_at:
        results.append(_probe_once(endpoint, params))


def _summarize(samples: list[Sample]) -> dict[str, Any]:
    if not samples:
        return {"count": 0}
    elapsed = sorted(sample.elapsed_ms for sample in samples)
    return {
        "count": len(samples),
        "non_200": sum(1 for sample in samples if sample.status_code != 200),
        "timeouts": sum(1 for sample in samples if sample.status_code == 0),
        "p50_ms": round(statistics.median(elapsed), 2),
        "p95_ms": round(elapsed[min(len(elapsed) - 1, max(0, int(len(elapsed) * 0.95) - 1))], 2),
        "max_ms": round(max(elapsed), 2),
    }


def main() -> None:
    print("Triggering scheduler run-once ...")
    _json_request("POST", "/api/scraper/scheduler/run-once")
    status = _wait_for_running_cycle()
    print(json.dumps({"scheduler": status}, ensure_ascii=True))

    stop_at = time.monotonic() + DURATION_SECONDS
    endpoint_params = {
        "/api/dashboard": dict(DEFAULT_QUERY),
        "/api/topics": {**DEFAULT_QUERY, "page": 0, "size": 100},
        "/api/topics/detail": {
            **DEFAULT_QUERY,
            "topic": os.getenv("MIXED_LOAD_TOPIC", "Prime Minister Policy"),
            "category": os.getenv("MIXED_LOAD_CATEGORY", "Government & Leadership"),
        },
        "/api/topics/evidence": {
            **DEFAULT_QUERY,
            "topic": os.getenv("MIXED_LOAD_TOPIC", "Prime Minister Policy"),
            "category": os.getenv("MIXED_LOAD_CATEGORY", "Government & Leadership"),
            "page": 0,
            "size": 20,
        },
    }

    samples: dict[str, list[Sample]] = {endpoint: [] for endpoint in endpoint_params}
    with ThreadPoolExecutor(max_workers=len(endpoint_params) * CONCURRENCY) as executor:
        futures = []
        for endpoint, params in endpoint_params.items():
            for _ in range(CONCURRENCY):
                futures.append(executor.submit(_run_endpoint_load, endpoint, params, samples[endpoint], stop_at))
        for future in as_completed(futures):
            future.result()

    summary = {endpoint: _summarize(rows) for endpoint, rows in samples.items()}
    print(json.dumps(summary, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
