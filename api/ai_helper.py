from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any, Literal, Protocol
from urllib import parse as urllib_parse

import httpx
from loguru import logger

import config
from api.runtime_coordinator import get_runtime_coordinator


_MAX_HISTORY_LIMIT = 100
_TRANSCRIPT_STORE_PREFIX = "ai-helper-transcript"
_session_lock_guard = threading.Lock()
_session_locks: dict[str, asyncio.Lock] = {}
_provider_lock = threading.Lock()
_provider: "OpenClawAiHelperProvider | None" = None
_local_transcript_lock = threading.Lock()
_local_transcripts: dict[str, tuple[float, list["AIHelperMessage"]]] = {}


Transport = Literal["openai_compatible", "legacy", "cli_bridge"]


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
    async def chat(
        self,
        message: str,
        *,
        session_id: str | None = None,
        request_id: str | None = None,
    ) -> AIHelperMessage:
        ...

    async def history(
        self,
        limit: int = 50,
        *,
        session_id: str | None = None,
        request_id: str | None = None,
    ) -> list[AIHelperMessage]:
        ...

    async def reset(
        self,
        *,
        session_id: str | None = None,
        request_id: str | None = None,
    ) -> str:
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


def _normalize_transport(value: str | None, *, has_model: bool) -> Transport:
    raw = str(value or "").strip().lower()
    if raw == "legacy":
        return "legacy"
    if raw == "openai_compatible":
        return "openai_compatible"
    if raw == "cli_bridge":
        return "cli_bridge"
    transport: Transport = "openai_compatible" if has_model else "legacy"
    logger.info(
        "AI helper transport auto-selected {} (configured={}, model_present={})",
        transport,
        raw or "auto",
        has_model,
    )
    return transport


def _compose_endpoint(base_url: str, path: str) -> str:
    clean_base = str(base_url or "").strip().rstrip("/")
    clean_path = "/" + str(path or "").strip().lstrip("/")
    if clean_base.endswith("/v1"):
        if clean_path.startswith("/v1/"):
            return f"{clean_base}{clean_path[3:]}"
        return f"{clean_base}{clean_path}"
    if clean_path.startswith("/v1/"):
        return f"{clean_base}{clean_path}"
    return f"{clean_base}/v1{clean_path}"


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


def _drop_oldest_turn(messages: list[AIHelperMessage]) -> list[AIHelperMessage]:
    if not messages:
        return messages
    remaining = messages[1:]
    if remaining and remaining[0].role == "assistant":
        remaining = remaining[1:]
    return remaining


def _trim_transcript(messages: list[AIHelperMessage], *, session_key: str | None = None) -> list[AIHelperMessage]:
    max_messages = max(2, int(config.OPENCLAW_HELPER_HISTORY_MAX_MESSAGES))
    max_chars = max(1000, int(config.OPENCLAW_HELPER_HISTORY_MAX_CHARS))
    trimmed = list(messages)
    trimmed_any = False

    while len(trimmed) > max_messages:
        next_trimmed = _drop_oldest_turn(trimmed)
        if len(next_trimmed) == len(trimmed):
            break
        trimmed = next_trimmed
        trimmed_any = True

    while trimmed and sum(len(item.text) for item in trimmed) > max_chars:
        next_trimmed = _drop_oldest_turn(trimmed)
        if len(next_trimmed) == len(trimmed):
            break
        trimmed = next_trimmed
        trimmed_any = True

    while trimmed and trimmed[0].role != "user":
        trimmed = trimmed[1:]
        trimmed_any = True

    if trimmed_any:
        logger.info(
            "AI helper transcript trimmed | session={} messages={} chars={}",
            session_key or "unknown",
            len(trimmed),
            sum(len(item.text) for item in trimmed),
        )
    return trimmed


def _bounded_replay_messages(messages: list[AIHelperMessage], *, session_key: str | None = None) -> list[AIHelperMessage]:
    max_messages = max(2, int(config.OPENCLAW_HELPER_REPLAY_MAX_MESSAGES))
    max_chars = max(1000, int(config.OPENCLAW_HELPER_REPLAY_MAX_CHARS))
    bounded = list(messages)

    while len(bounded) > max_messages:
        next_bounded = _drop_oldest_turn(bounded)
        if len(next_bounded) == len(bounded):
            break
        bounded = next_bounded

    while bounded and sum(len(item.text) for item in bounded) > max_chars:
        next_bounded = _drop_oldest_turn(bounded)
        if len(next_bounded) == len(bounded):
            break
        bounded = next_bounded

    while bounded and bounded[0].role != "user":
        bounded = bounded[1:]

    if len(bounded) != len(messages):
        logger.info(
            "AI helper replay bounded | session={} messages={} chars={}",
            session_key or "unknown",
            len(bounded),
            sum(len(item.text) for item in bounded),
        )
    return bounded


class _TranscriptStore:
    def __init__(self, session_key: str) -> None:
        self._session_key = session_key
        self._coordinator = get_runtime_coordinator()
        self._ttl_seconds = max(300, int(config.OPENCLAW_HELPER_TRANSCRIPT_TTL_SECONDS))

    @property
    def mode(self) -> str:
        return "redis" if self._coordinator.enabled else "local"

    def _storage_key(self) -> str:
        return f"{_TRANSCRIPT_STORE_PREFIX}:{self._session_key}"

    def load(self) -> list[AIHelperMessage]:
        key = self._storage_key()
        now = time.time()
        if self._coordinator.enabled:
            raw = self._coordinator.get_json(key)
            if raw is None:
                return []
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                logger.warning("AI helper transcript store contained invalid JSON for {}", self._session_key)
                return []
            return self._normalize_payload(payload)

        with _local_transcript_lock:
            stored = _local_transcripts.get(key)
            if stored is None:
                return []
            expires_at, messages = stored
            if expires_at <= now:
                _local_transcripts.pop(key, None)
                return []
            return list(messages)

    def save(self, messages: list[AIHelperMessage]) -> None:
        payload = _trim_transcript(messages, session_key=self._session_key)
        key = self._storage_key()
        if self._coordinator.enabled:
            encoded = json.dumps([item.to_dict() for item in payload], ensure_ascii=True)
            self._coordinator.set_json(key, encoded, self._ttl_seconds)
            return
        with _local_transcript_lock:
            _local_transcripts[key] = (time.time() + self._ttl_seconds, list(payload))

    def clear(self) -> None:
        key = self._storage_key()
        if self._coordinator.enabled:
            self._coordinator.delete_json(key)
            return
        with _local_transcript_lock:
            _local_transcripts.pop(key, None)

    def _normalize_payload(self, payload: Any) -> list[AIHelperMessage]:
        if not isinstance(payload, list):
            return []
        messages: list[AIHelperMessage] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "").strip().lower()
            text = str(item.get("text") or "").strip()
            if role not in {"user", "assistant"} or not text:
                continue
            messages.append(
                AIHelperMessage(
                    role=role,
                    text=text,
                    timestamp=_coerce_timestamp(item.get("timestamp")),
                )
            )
        return _trim_transcript(messages, session_key=self._session_key)


class OpenClawAiHelperProvider:
    def __init__(
        self,
        *,
        base_url: str,
        gateway_token: str,
        agent_id: str,
        session_key: str,
        timeout_seconds: float,
        connect_timeout_seconds: float | None = None,
        read_timeout_seconds: float | None = None,
        retry_attempts: int | None = None,
        transport: str | None = None,
        model: str | None = None,
        manage_transcript: bool = False,
    ) -> None:
        self.base_url = str(base_url or "").strip().rstrip("/")
        self.gateway_token = str(gateway_token or "").strip()
        self.agent_id = str(agent_id or "").strip()
        self.session_key = str(session_key or "").strip()
        self.transport = _normalize_transport(transport or config.OPENCLAW_GATEWAY_TRANSPORT, has_model=bool(model or config.OPENCLAW_GATEWAY_MODEL))
        self.model = str(model or config.OPENCLAW_GATEWAY_MODEL or "").strip()
        self.manage_transcript = bool(manage_transcript)
        self.retry_attempts = max(0, int(config.OPENCLAW_HELPER_RETRY_ATTEMPTS if retry_attempts is None else retry_attempts))
        default_timeout = max(1.0, float(timeout_seconds))
        self.connect_timeout_seconds = max(
            1.0,
            float(config.OPENCLAW_HELPER_CONNECT_TIMEOUT_SECONDS if connect_timeout_seconds is None else connect_timeout_seconds),
        )
        self.read_timeout_seconds = max(
            1.0,
            float(config.OPENCLAW_HELPER_READ_TIMEOUT_SECONDS if read_timeout_seconds is None else read_timeout_seconds),
        )
        if not connect_timeout_seconds and not read_timeout_seconds:
            self.connect_timeout_seconds = max(self.connect_timeout_seconds, min(default_timeout, self.connect_timeout_seconds))
            self.read_timeout_seconds = max(self.read_timeout_seconds, min(default_timeout, self.read_timeout_seconds))
        self._transcript_store_mode = (
            _TranscriptStore(self.session_key).mode if self.manage_transcript else "disabled"
        )
        logger.info(
            "AI helper provider configured | transport={} base_url={} model={} transcript_store={} legacy_agent={}",
            self.transport,
            self.base_url,
            self.model or "-",
            self._transcript_store_mode,
            bool(self.agent_id),
        )

    def _effective_session_key(self, session_id: str | None = None) -> str:
        session_suffix = str(session_id or "").strip()
        if not session_suffix:
            return self.session_key
        return f"{self.session_key}:{session_suffix}"

    def _transcript_store(self, session_id: str | None = None) -> _TranscriptStore | None:
        if not self.manage_transcript:
            return None
        return _TranscriptStore(self._effective_session_key(session_id))

    def _build_headers(self, *, session_key: str | None = None) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self.gateway_token}",
            "Accept": "application/json",
        }
        if self.transport == "legacy":
            headers["x-openclaw-agent-id"] = self.agent_id
            headers["x-openclaw-session-key"] = str(session_key or self.session_key)
        return headers

    def _validate_config(self) -> None:
        if not self.base_url or not self.gateway_token:
            raise AIHelperError(
                status_code=503,
                code="upstream_config",
                message="The AI helper OpenClaw gateway is not configured.",
                retryable=False,
            )
        if not self.session_key:
            raise AIHelperError(
                status_code=503,
                code="upstream_config",
                message="The AI helper OpenClaw session is not configured.",
                retryable=False,
            )
        if self.transport == "openai_compatible":
            if not self.model:
                raise AIHelperError(
                    status_code=503,
                    code="upstream_config",
                    message="The AI helper OpenClaw model is not configured.",
                    retryable=False,
                )
            return
        if not self.agent_id:
            raise AIHelperError(
                status_code=503,
                code="upstream_config",
                message="The AI helper OpenClaw agent is not configured.",
                retryable=False,
            )

    async def chat(
        self,
        message: str,
        *,
        session_id: str | None = None,
        request_id: str | None = None,
    ) -> AIHelperMessage:
        self._validate_config()
        effective_session_key = self._effective_session_key(session_id)
        transcript_store = self._transcript_store(session_id)
        async with _session_lock(effective_session_key):
            if self.transport == "openai_compatible":
                transcript = transcript_store.load() if transcript_store is not None else []
                payload = await _run_blocking(
                    self._request_json,
                    "POST",
                    "/chat/completions",
                    None,
                    {
                        "model": self.model,
                        "messages": [
                            *({"role": item.role, "content": item.text} for item in transcript),
                            {"role": "user", "content": message},
                        ],
                        "stream": False,
                    },
                    effective_session_key,
                    request_id,
                )
                reply = self._normalize_chat_message(payload)
                if transcript_store is not None:
                    transcript_store.save(
                        [
                            *transcript,
                            AIHelperMessage(role="user", text=message, timestamp=_utc_now_iso()),
                            reply,
                        ]
                    )
                return reply

            if self.transport == "cli_bridge":
                transcript = transcript_store.load() if transcript_store is not None else []
                outgoing_messages = _bounded_replay_messages(
                    [
                        *transcript,
                        AIHelperMessage(role="user", text=message, timestamp=_utc_now_iso()),
                    ],
                    session_key=effective_session_key,
                )
                payload = await _run_blocking(
                    self._request_json,
                    "POST",
                    "/openclaw-agent/chat",
                    None,
                    {
                        "requestId": request_id or "",
                        "sessionId": str(session_id or effective_session_key),
                        "messages": [{"role": item.role, "text": item.text} for item in outgoing_messages],
                    },
                    effective_session_key,
                    request_id,
                )
                reply = self._normalize_chat_message(payload)
                if transcript_store is not None:
                    transcript_store.save(
                        [
                            *transcript,
                            AIHelperMessage(role="user", text=message, timestamp=_utc_now_iso()),
                            reply,
                        ]
                    )
                return reply

            payload = await _run_blocking(
                self._request_json,
                "POST",
                "/responses",
                None,
                {
                    "model": "openclaw",
                    "stream": False,
                    "input": message,
                },
                effective_session_key,
                request_id,
            )
            return self._normalize_chat_message(payload)

    async def history(
        self,
        limit: int = 50,
        *,
        session_id: str | None = None,
        request_id: str | None = None,
    ) -> list[AIHelperMessage]:
        safe_limit = min(max(int(limit), 1), _MAX_HISTORY_LIMIT)
        self._validate_config()
        effective_session_key = self._effective_session_key(session_id)
        transcript_store = self._transcript_store(session_id)
        if self.transport in {"openai_compatible", "cli_bridge"}:
            if transcript_store is None:
                return []
            messages = transcript_store.load()
            if len(messages) > safe_limit:
                messages = messages[-safe_limit:]
            return messages

        try:
            payload = await _run_blocking(
                self._request_json,
                "GET",
                f"/sessions/{urllib_parse.quote(effective_session_key, safe='')}/history",
                {
                    "limit": safe_limit,
                    "includeTools": "false",
                },
                None,
                effective_session_key,
                request_id,
            )
        except AIHelperError as exc:
            if exc.status_code == 404:
                return []
            raise
        return self._normalize_history(payload, limit=safe_limit)

    async def reset(
        self,
        *,
        session_id: str | None = None,
        request_id: str | None = None,
    ) -> str:
        self._validate_config()
        effective_session_key = self._effective_session_key(session_id)
        transcript_store = self._transcript_store(session_id)
        async with _session_lock(effective_session_key):
            if self.transport in {"openai_compatible", "cli_bridge"}:
                if transcript_store is not None:
                    transcript_store.clear()
                return _utc_now_iso()

            await _run_blocking(
                self._request_json,
                "POST",
                "/responses",
                None,
                {
                    "model": "openclaw",
                    "stream": False,
                    "input": "/reset",
                },
                effective_session_key,
                request_id,
            )
        return _utc_now_iso()

    def _request_json(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None,
        payload: dict[str, Any] | None,
        session_key: str | None = None,
        request_id: str | None = None,
    ) -> Any:
        url = (
            f"{self.base_url.rstrip('/')}/{str(path or '').lstrip('/')}"
            if self.transport == "cli_bridge"
            else _compose_endpoint(self.base_url, path)
        )
        request_headers = self._build_headers(session_key=session_key)
        if request_id:
            request_headers["X-Request-ID"] = str(request_id)

        attempt = 0
        while True:
            started_at = time.perf_counter()
            try:
                with httpx.Client(
                    timeout=httpx.Timeout(
                        connect=self.connect_timeout_seconds,
                        read=self.read_timeout_seconds,
                        write=self.read_timeout_seconds,
                        pool=self.connect_timeout_seconds,
                    )
                ) as client:
                    response = client.request(
                        method.upper(),
                        url,
                        params=query,
                        headers=request_headers,
                        json=payload,
                    )
                    response.raise_for_status()
                    if not response.text.strip():
                        raise AIHelperError(
                            status_code=502,
                            code="upstream_invalid_response",
                            message="The AI helper service returned an empty response.",
                            retryable=True,
                        )
                    result = response.json()
                    logger.info(
                        "AI helper upstream request succeeded | request_id={} session_id={} transport={} agent_id={} duration_ms={} outcome=success",
                        request_id or "-",
                        session_key or self.session_key,
                        self.transport,
                        self.agent_id or "-",
                        round((time.perf_counter() - started_at) * 1000, 2),
                    )
                    return result
            except httpx.HTTPStatusError as exc:
                logger.warning(
                    "AI helper upstream HTTP error | request_id={} session_id={} transport={} agent_id={} status={} duration_ms={}",
                    request_id or "-",
                    session_key or self.session_key,
                    self.transport,
                    self.agent_id or "-",
                    exc.response.status_code,
                    round((time.perf_counter() - started_at) * 1000, 2),
                )
                raise self._map_http_error(exc.response) from exc
            except (httpx.ConnectTimeout, httpx.ReadTimeout) as exc:
                if attempt < self.retry_attempts:
                    attempt += 1
                    logger.warning(
                        "AI helper request timeout, retrying | request_id={} session_id={} transport={} agent_id={} attempt={} url={}",
                        request_id or "-",
                        session_key or self.session_key,
                        self.transport,
                        self.agent_id or "-",
                        attempt,
                        url,
                    )
                    continue
                logger.warning(
                    "AI helper upstream timeout | request_id={} session_id={} transport={} agent_id={} duration_ms={}",
                    request_id or "-",
                    session_key or self.session_key,
                    self.transport,
                    self.agent_id or "-",
                    round((time.perf_counter() - started_at) * 1000, 2),
                )
                raise self._map_network_error(exc) from exc
            except (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError) as exc:
                if attempt < self.retry_attempts:
                    attempt += 1
                    logger.warning(
                        "AI helper transient network error, retrying | request_id={} session_id={} transport={} agent_id={} attempt={} url={} error={}",
                        request_id or "-",
                        session_key or self.session_key,
                        self.transport,
                        self.agent_id or "-",
                        attempt,
                        url,
                        exc,
                    )
                    continue
                logger.warning(
                    "AI helper upstream network error | request_id={} session_id={} transport={} agent_id={} duration_ms={} error={}",
                    request_id or "-",
                    session_key or self.session_key,
                    self.transport,
                    self.agent_id or "-",
                    round((time.perf_counter() - started_at) * 1000, 2),
                    exc,
                )
                raise self._map_network_error(exc) from exc
            except ValueError as exc:
                logger.warning(
                    "AI helper upstream parse error | request_id={} session_id={} transport={} agent_id={} duration_ms={}",
                    request_id or "-",
                    session_key or self.session_key,
                    self.transport,
                    self.agent_id or "-",
                    round((time.perf_counter() - started_at) * 1000, 2),
                )
                raise AIHelperError(
                    status_code=502,
                    code="upstream_invalid_response",
                    message="The AI helper service returned invalid JSON.",
                    retryable=True,
                ) from exc

    def _map_network_error(self, exc: Exception) -> AIHelperError:
        is_timeout = isinstance(exc, (httpx.ConnectTimeout, httpx.ReadTimeout))
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

    def _map_http_error(self, response: httpx.Response) -> AIHelperError:
        detail = ""
        bridge_code = ""
        try:
            parsed = response.json()
            if isinstance(parsed, dict):
                detail = _extract_text(parsed) or str(parsed.get("detail") or "").strip()
                error_payload = parsed.get("error")
                if isinstance(error_payload, dict):
                    bridge_code = str(error_payload.get("code") or "").strip()
            else:
                detail = str(parsed).strip()
        except Exception:
            detail = response.text.strip()

        if self.transport == "cli_bridge" and bridge_code:
            retryable = bridge_code in {"bridge_timeout"}
            status_code = 504 if bridge_code == "bridge_timeout" else 502
            return AIHelperError(
                status_code=status_code,
                code=bridge_code,
                message=detail or "The OpenClaw bridge request failed.",
                retryable=retryable,
            )
        if response.status_code in {401, 403}:
            return AIHelperError(
                status_code=502,
                code="upstream_auth",
                message=detail or "The AI helper credentials were rejected by OpenClaw.",
                retryable=False,
            )
        if response.status_code == 404:
            return AIHelperError(
                status_code=404,
                code="upstream_not_found",
                message=detail or "The AI helper session could not be found.",
                retryable=False,
            )
        if response.status_code == 400:
            return AIHelperError(
                status_code=502,
                code="upstream_config",
                message=detail or "The AI helper request was rejected by OpenClaw.",
                retryable=False,
            )
        if response.status_code == 429:
            return AIHelperError(
                status_code=502,
                code="upstream_rate_limited",
                message=detail or "The AI helper is temporarily rate limited.",
                retryable=True,
            )
        return AIHelperError(
            status_code=502,
            code="upstream_unavailable",
            message=detail or f"The AI helper returned HTTP {response.status_code}.",
            retryable=response.status_code >= 500,
        )

    def _normalize_chat_message(self, payload: Any) -> AIHelperMessage:
        message_payload = payload.get("message") if isinstance(payload, dict) else None
        timestamp_source = payload.get("created_at") if isinstance(payload, dict) else None
        if isinstance(message_payload, dict):
            text = _extract_text_from_response(message_payload)
            timestamp_source = message_payload.get("timestamp") or timestamp_source
        else:
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
            timestamp=_coerce_timestamp(timestamp_source),
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


def get_default_ai_helper_provider() -> OpenClawAiHelperProvider:
    global _provider
    with _provider_lock:
        if _provider is None:
            transport = config.OPENCLAW_GATEWAY_TRANSPORT
            is_cli_bridge = str(transport or "").strip().lower() == "cli_bridge"
            _provider = OpenClawAiHelperProvider(
                base_url=config.OPENCLAW_BRIDGE_BASE_URL if is_cli_bridge else config.OPENCLAW_GATEWAY_BASE_URL,
                gateway_token=config.OPENCLAW_BRIDGE_TOKEN if is_cli_bridge else config.OPENCLAW_GATEWAY_TOKEN,
                agent_id=config.OPENCLAW_BRIDGE_AGENT_ID if is_cli_bridge else config.OPENCLAW_ANALYTICS_AGENT_ID,
                session_key=config.OPENCLAW_WEB_SESSION_KEY,
                timeout_seconds=config.OPENCLAW_HELPER_TIMEOUT_SECONDS,
                connect_timeout_seconds=config.OPENCLAW_HELPER_CONNECT_TIMEOUT_SECONDS,
                read_timeout_seconds=config.OPENCLAW_HELPER_READ_TIMEOUT_SECONDS,
                retry_attempts=config.OPENCLAW_HELPER_RETRY_ATTEMPTS,
                transport=transport,
                model=config.OPENCLAW_GATEWAY_MODEL,
                manage_transcript=True,
            )
            _provider._validate_config()
        return _provider


def reset_ai_helper_provider_cache() -> None:
    global _provider
    with _provider_lock:
        _provider = None
    logger.info("AI helper provider cache reset")
