"""
client.py — HTTP client for the KB skill.
Mirrors the analytics bridge client with exponential backoff retry.
Calls the /api/kb/* endpoints on the analytics backend.
"""
from __future__ import annotations

import json
import socket
import time
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from models import ClientConfig


RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class KBAPIError(RuntimeError):
    def __init__(self, message: str, *, error_type: str = "api_error", status_code: int | None = None):
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.status_code = status_code


class KBClient:
    def __init__(self, config: ClientConfig):
        self.base_url = config.base_url.rstrip("/")
        self.api_key = config.api_key
        self.timeout = config.timeout
        self.max_retries = config.max_retries
        self.backoff_base = config.backoff_base

    # ── High-level helpers ──────────────────────────────────────────────────

    def list_collections(self) -> dict[str, Any]:
        return self._request_json("GET", "/api/kb/collections")

    def ask_kb(self, question: str, collection: str) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/api/kb/ask",
            payload={"question": question, "collection": collection},
        )

    def add_url(self, url: str, collection: str, doc_title: str = "") -> dict[str, Any]:
        return self._request_json(
            "POST",
            f"/api/kb/collections/{urllib_parse.quote(collection, safe='')}/add-url",
            payload={"url": url, "doc_title": doc_title},
        )

    def search_kb(self, query: str, collection: str, top_k: int = 5) -> dict[str, Any]:
        return self._request_json(
            "GET",
            "/api/kb/search",
            query={"collection": collection, "q": query, "top_k": str(top_k)},
        )

    # ── Core HTTP ───────────────────────────────────────────────────────────

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        url = self._build_url(path, query=query)
        data = None
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")

        req = urllib_request.Request(url=url, data=data, headers=headers, method=method.upper())
        attempts = self.max_retries + 1
        last_error: KBAPIError | None = None

        for attempt in range(1, attempts + 1):
            try:
                with urllib_request.urlopen(req, timeout=self.timeout) as response:
                    raw = response.read().decode("utf-8")
                    if not raw.strip():
                        raise KBAPIError("API returned an empty response.", error_type="empty_response")
                    return json.loads(raw)
            except urllib_error.HTTPError as exc:
                last_error = self._map_http_error(exc)
                if exc.code in RETRYABLE_STATUS_CODES and attempt < attempts:
                    time.sleep(self.backoff_base * (2 ** (attempt - 1)))
                    continue
                raise last_error
            except (urllib_error.URLError, socket.timeout, TimeoutError) as exc:
                is_timeout = isinstance(exc, (socket.timeout, TimeoutError))
                last_error = KBAPIError(
                    "The backend is taking too long. It may be starting up. Try again in a moment."
                    if is_timeout
                    else "The backend could not be reached. Check ANALYTICS_API_BASE_URL.",
                    error_type="timeout" if is_timeout else "network_error",
                )
                if attempt < attempts:
                    time.sleep(self.backoff_base * (2 ** (attempt - 1)))
                    continue
                raise last_error
            except json.JSONDecodeError as exc:
                raise KBAPIError(f"API returned invalid JSON: {exc}", error_type="invalid_json") from exc

        raise last_error or KBAPIError("Unknown error.", error_type="api_error")

    def _build_url(self, path: str, *, query: dict[str, Any] | None = None) -> str:
        url = f"{self.base_url}{path}"
        if query:
            filtered = {k: v for k, v in query.items() if v is not None and v != ""}
            if filtered:
                url = f"{url}?{urllib_parse.urlencode(filtered)}"
        return url

    def _map_http_error(self, exc: urllib_error.HTTPError) -> KBAPIError:
        detail = ""
        try:
            body = exc.read().decode("utf-8")
            if body.strip():
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    detail = str(parsed.get("detail") or parsed.get("message") or "").strip()
                else:
                    detail = body.strip()
        except Exception:
            detail = ""

        if exc.code in {401, 403}:
            return KBAPIError(
                "Authentication failed. Check ANALYTICS_API_KEY.",
                error_type="auth_error",
                status_code=exc.code,
            )
        if exc.code == 503:
            return KBAPIError(
                detail or "GEMINI_API_KEY is not configured on the backend. Add it to your .env.",
                error_type="configuration_error",
                status_code=exc.code,
            )
        if exc.code == 400:
            return KBAPIError(
                detail or "Invalid request parameters.",
                error_type="invalid_request",
                status_code=exc.code,
            )
        if exc.code == 422:
            return KBAPIError(
                detail or "Unsupported file format or empty document.",
                error_type="unsupported_format",
                status_code=exc.code,
            )
        if exc.code == 404:
            return KBAPIError(
                "Endpoint not found. Ensure the backend is up to date.",
                error_type="not_found",
                status_code=exc.code,
            )
        return KBAPIError(
            detail or f"Backend returned HTTP {exc.code}.",
            error_type="upstream_error",
            status_code=exc.code,
        )
