from __future__ import annotations

import asyncio
import json
import socket
import threading
from functools import partial
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from loguru import logger

import config


_MAX_HISTORY_LIMIT = 100
_session_lock_guard = threading.Lock()
_session_locks: dict[str, asyncio.Lock] = {}
_provider_lock = threading.Lock()
_provider: "OpenClawAiHelperProvider | None" = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _session_lock(session_key: str) -> asyncio.Lock:
    key = str(session_key or "").strip()
    if not key:
        raise AIHelperError(
            status_code=503,
            code="upstream_config",
            message="AI helper session is not configured.",
            retryable=False,
        )
    with _session_lock_guard:
        lock = _session_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _session_locks[key] = lock
        return lock


async def _run_blocking(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))


def _coerce_timestamp(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip()
        if text:
            return text
    return _utc_now_iso()


def _extract_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("output_text", "text", "message", "value", "content"):
            text = _extract_text(value.get(key))
            if text:
                return text
        nested = value.get("text")
        if isinstance(nested, dict):
            text = _extract_text(nested)
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            text = _extract_text(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    return ""


@dataclass(frozen=True)
class AIHelperMessage:
    role: str
    text: str
    timestamp: str

    def to_dict(self) -> dict[str, str]:
        return {
            "role": self.role,
            "text": self.text,
            "timestamp": self.timestamp,
        }


class AIHelperProvider(Protocol):
    async def chat(self, message: str) -> AIHelperMessage:
        ...

    async def history(self, limit: int = 50) -> list[AIHelperMessage]:
        ...

    async def reset(self) -> str:
        ...


class AIHelperError(RuntimeError):
    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        retryable: bool,
    ) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.code = str(code or "ai_helper_error")
        self.message = str(message or "The AI helper failed.")
        self.retryable = bool(retryable)


class OpenClawAiHelperProvider:
    def __init__(
        self,
        *,
        base_url: str,
        gateway_token: str,
        agent_id: str,
        session_key: str,
        timeout_seconds: float,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.gateway_token = gateway_token
        self.agent_id = agent_id
        self.session_key = session_key
        self.timeout_seconds = max(1.0, float(timeout_seconds))

    def _build_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.gateway_token}",
            "Accept": "application/json",
            "x-openclaw-agent-id": self.agent_id,
            "x-openclaw-session-key": self.session_key,
        }

    async def chat(self, message: str) -> AIHelperMessage:
        async with _session_lock(self.session_key):
            payload = await _run_blocking(
                self._request_json,
                "POST",
                "/v1/responses",
                None,
                {
                    "model": "openclaw",
                    "stream": False,
                    "input": message,
                },
            )
        return self._normalize_chat_message(payload)

    async def history(self, limit: int = 50) -> list[AIHelperMessage]:
        safe_limit = min(max(int(limit), 1), _MAX_HISTORY_LIMIT)
        try:
            payload = await _run_blocking(
                self._request_json,
                "GET",
                f"/sessions/{urllib_parse.quote(self.session_key, safe='')}/history",
                {
                    "limit": safe_limit,
                    "includeTools": "false",
                },
                None,
            )
        except AIHelperError as exc:
            if exc.status_code == 404:
                return []
            raise
        return self._normalize_history(payload, limit=safe_limit)

    async def reset(self) -> str:
        async with _session_lock(self.session_key):
            await _run_blocking(
                self._request_json,
                "POST",
                "/v1/responses",
                None,
                {
                    "model": "openclaw",
                    "stream": False,
                    "input": "/reset",
                },
            )
        return _utc_now_iso()

    def _request_json(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None,
        payload: dict[str, Any] | None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        if query:
            filtered = {key: value for key, value in query.items() if value not in (None, "")}
            if filtered:
                url = f"{url}?{urllib_parse.urlencode(filtered)}"

        headers = self._build_headers()
        body = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload, ensure_ascii=True).encode("utf-8")

        request = urllib_request.Request(
            url=url,
            data=body,
            headers=headers,
            method=method.upper(),
        )

        try:
            with urllib_request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urllib_error.HTTPError as exc:
            raise self._map_http_error(exc) from exc
        except (urllib_error.URLError, socket.timeout, TimeoutError) as exc:
            raise self._map_network_error(exc) from exc

        if not raw.strip():
            raise AIHelperError(
                status_code=502,
                code="upstream_invalid_response",
                message="The AI helper service returned an empty response.",
                retryable=True,
            )

        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise AIHelperError(
                status_code=502,
                code="upstream_invalid_response",
                message="The AI helper service returned invalid JSON.",
                retryable=True,
            ) from exc

    def _map_network_error(self, exc: Exception) -> AIHelperError:
        is_timeout = isinstance(exc, (socket.timeout, TimeoutError))
        return AIHelperError(
            status_code=504 if is_timeout else 502,
            code="upstream_timeout" if is_timeout else "upstream_unavailable",
            message=(
                "The AI helper is taking longer than expected. Please try again in a moment."
                if is_timeout
                else "The AI helper service could not be reached right now."
            ),
            retryable=True,
        )

    def _map_http_error(self, exc: urllib_error.HTTPError) -> AIHelperError:
        detail = ""
        try:
            body = exc.read().decode("utf-8")
            if body.strip():
                parsed = json.loads(body)
                if isinstance(parsed, dict):
                    detail = _extract_text(parsed) or str(parsed.get("detail") or "").strip()
                else:
                    detail = str(parsed).strip()
        except Exception:
            detail = ""

        if exc.code in {401, 403}:
            return AIHelperError(
                status_code=502,
                code="upstream_auth",
                message=detail or "The AI helper credentials were rejected by OpenClaw.",
                retryable=False,
            )
        if exc.code == 404:
            return AIHelperError(
                status_code=404,
                code="upstream_not_found",
                message=detail or "The AI helper session could not be found.",
                retryable=False,
            )
        if exc.code == 400:
            return AIHelperError(
                status_code=502,
                code="upstream_config",
                message=detail or "The AI helper request was rejected by OpenClaw.",
                retryable=False,
            )
        if exc.code == 429:
            return AIHelperError(
                status_code=502,
                code="upstream_rate_limited",
                message=detail or "The AI helper is temporarily rate limited.",
                retryable=True,
            )
        return AIHelperError(
            status_code=502,
            code="upstream_unavailable",
            message=detail or f"The AI helper returned HTTP {exc.code}.",
            retryable=exc.code >= 500,
        )

    def _normalize_chat_message(self, payload: Any) -> AIHelperMessage:
        text = _extract_text_from_response(payload)
        if not text:
            raise AIHelperError(
                status_code=502,
                code="upstream_invalid_response",
                message="OpenClaw returned a response without assistant text.",
                retryable=True,
            )
        return AIHelperMessage(
            role="assistant",
            text=text,
            timestamp=_coerce_timestamp(
                payload.get("created_at")
                if isinstance(payload, dict)
                else None
            ),
        )

    def _normalize_history(self, payload: Any, *, limit: int) -> list[AIHelperMessage]:
        candidates: list[Any]
        if isinstance(payload, list):
            candidates = payload
        elif isinstance(payload, dict):
            for key in ("messages", "items", "history", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    candidates = value
                    break
            else:
                candidates = []
        else:
            candidates = []

        normalized: list[AIHelperMessage] = []
        for item in candidates:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "").strip().lower()
            if role not in {"user", "assistant"}:
                continue
            text = _extract_text(item.get("content"))
            if not text:
                text = _extract_text(item)
            if not text:
                continue
            normalized.append(
                AIHelperMessage(
                    role=role,
                    text=text,
                    timestamp=_coerce_timestamp(
                        item.get("timestamp")
                        or item.get("created_at")
                        or item.get("createdAt")
                    ),
                )
            )

        normalized.sort(key=lambda message: message.timestamp)
        if len(normalized) > limit:
            normalized = normalized[-limit:]
        return normalized


def _extract_text_from_response(payload: Any) -> str:
    if not isinstance(payload, dict):
        return _extract_text(payload)

    direct = _extract_text(payload.get("output_text"))
    if direct:
        return direct

    output = payload.get("output")
    if isinstance(output, list):
        parts: list[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "").strip().lower() not in {"message", "output_text", "text"}:
                continue
            text = _extract_text(item.get("content"))
            if not text:
                text = _extract_text(item)
            if text:
                parts.append(text)
        if parts:
            return "\n".join(parts).strip()

    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            text = _extract_text(first.get("message"))
            if text:
                return text

    return _extract_text(payload)


def get_default_ai_helper_provider() -> OpenClawAiHelperProvider:
    global _provider
    with _provider_lock:
        if _provider is None:
            if not config.OPENCLAW_GATEWAY_BASE_URL or not config.OPENCLAW_GATEWAY_TOKEN:
                raise AIHelperError(
                    status_code=503,
                    code="upstream_config",
                    message="The AI helper OpenClaw gateway is not configured.",
                    retryable=False,
                )
            if not config.OPENCLAW_ANALYTICS_AGENT_ID or not config.OPENCLAW_WEB_SESSION_KEY:
                raise AIHelperError(
                    status_code=503,
                    code="upstream_config",
                    message="The AI helper OpenClaw agent or session is not configured.",
                    retryable=False,
                )
            _provider = OpenClawAiHelperProvider(
                base_url=config.OPENCLAW_GATEWAY_BASE_URL,
                gateway_token=config.OPENCLAW_GATEWAY_TOKEN,
                agent_id=config.OPENCLAW_ANALYTICS_AGENT_ID,
                session_key=config.OPENCLAW_WEB_SESSION_KEY,
                timeout_seconds=config.OPENCLAW_HELPER_TIMEOUT_SECONDS,
            )
        return _provider


def reset_ai_helper_provider_cache() -> None:
    global _provider
    with _provider_lock:
        _provider = None
    logger.info("AI helper provider cache reset")
