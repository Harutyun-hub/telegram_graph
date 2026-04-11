from __future__ import annotations

import hmac
import json
import logging
import os
import re
import subprocess
import time
import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional

from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


OPENCLAW_BRIDGE_TOKEN = os.getenv("OPENCLAW_BRIDGE_TOKEN", "").strip()
OPENCLAW_BRIDGE_AGENT_ID = os.getenv("OPENCLAW_BRIDGE_AGENT_ID", "web-api-assistant").strip() or "web-api-assistant"
OPENCLAW_BRIDGE_DOCKER_BIN = os.getenv("OPENCLAW_BRIDGE_DOCKER_BIN", "/usr/bin/docker").strip() or "/usr/bin/docker"
OPENCLAW_BRIDGE_CONTAINER = os.getenv("OPENCLAW_BRIDGE_CONTAINER", "openclaw-k5ni-openclaw-1").strip() or "openclaw-k5ni-openclaw-1"
OPENCLAW_BRIDGE_OPENCLAW_BIN = os.getenv("OPENCLAW_BRIDGE_OPENCLAW_BIN", "openclaw").strip() or "openclaw"
OPENCLAW_BRIDGE_COMMAND_TIMEOUT_SECONDS = max(
    1,
    int(os.getenv("OPENCLAW_BRIDGE_COMMAND_TIMEOUT_SECONDS", "90")),
)
OPENCLAW_BRIDGE_MAX_MESSAGES = max(1, int(os.getenv("OPENCLAW_BRIDGE_MAX_MESSAGES", "20")))
OPENCLAW_BRIDGE_MAX_CHARS = max(1000, int(os.getenv("OPENCLAW_BRIDGE_MAX_CHARS", "8000")))
OPENCLAW_BRIDGE_HTTP_MAX_BODY_BYTES = max(
    1024,
    int(os.getenv("OPENCLAW_BRIDGE_HTTP_MAX_BODY_BYTES", "32768")),
)

logger = logging.getLogger("openclaw_bridge")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

app = FastAPI(title="OpenClaw Private Bridge", version="1.0.0")


class BridgeError(RuntimeError):
    def __init__(self, *, status_code: int, code: str, message: str, retryable: bool) -> None:
        super().__init__(message)
        self.status_code = int(status_code)
        self.code = str(code)
        self.message = str(message)
        self.retryable = bool(retryable)


class BridgeMessage(BaseModel):
    role: str = Field(..., min_length=1, max_length=20)
    text: str = Field(..., min_length=1, max_length=2000)


class BridgeChatRequest(BaseModel):
    requestId: Optional[str] = Field(default=None, max_length=128)
    sessionId: str = Field(..., min_length=8, max_length=64)
    messages: List[BridgeMessage]


def _log_event(level: int, payload: dict[str, Any]) -> None:
    logger.log(level, json.dumps(payload, ensure_ascii=True, separators=(",", ":")))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_bearer_token(raw_value: Optional[str]) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    scheme, _, token = text.partition(" ")
    if scheme.lower() == "bearer" and token.strip():
        return token.strip()
    return ""


def _extract_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("output_text", "text", "message", "value", "content"):
            text = _extract_text(value.get(key))
            if text:
                return text
        return ""
    if isinstance(value, list):
        parts = [_extract_text(item) for item in value]
        return "\n".join([item for item in parts if item]).strip()
    return ""


def _extract_assistant_text(payload: Any) -> str:
    if isinstance(payload, dict):
        if isinstance(payload.get("error"), dict):
            raise BridgeError(
                status_code=502,
                code="openclaw_runtime_error",
                message=_extract_text(payload["error"]) or "OpenClaw returned a runtime error.",
                retryable=False,
            )
        if isinstance(payload.get("choices"), list) and payload["choices"]:
            first = payload["choices"][0]
            if isinstance(first, dict):
                text = _extract_text(first.get("message"))
                if text:
                    return text
        if isinstance(payload.get("message"), dict):
            text = _extract_text(payload["message"])
            if text:
                return text
        result = payload.get("result")
        if isinstance(result, dict):
            payloads = result.get("payloads")
            if isinstance(payloads, list):
                parts = []
                for item in payloads:
                    text = _extract_text(item)
                    if text:
                        parts.append(text)
                if parts:
                    return "\n".join(parts).strip()
    text = _extract_text(payload)
    if text:
        return text
    raise BridgeError(
        status_code=502,
        code="bridge_parse_failed",
        message="Bridge could not extract assistant text from OpenClaw output.",
        retryable=False,
    )


def _render_prompt(messages: list[BridgeMessage]) -> str:
    parts = [
        "You are the web chat assistant for the Armenian community platform.",
        "Use the conversation transcript below as context and answer only as the assistant to the final user message.",
        "",
        "Transcript:",
    ]
    for message in messages:
        label = "User" if message.role == "user" else "Assistant"
        parts.append(f"{label}: {message.text}")
    return "\n".join(parts).strip()


def _validate_payload(payload: BridgeChatRequest) -> None:
    if not OPENCLAW_BRIDGE_TOKEN:
        raise BridgeError(
            status_code=503,
            code="bridge_validation_failed",
            message="Bridge token is not configured.",
            retryable=False,
        )
    if payload.sessionId and not re.fullmatch(r"[A-Za-z0-9_-]{8,64}", payload.sessionId):
        raise BridgeError(
            status_code=400,
            code="bridge_validation_failed",
            message="sessionId must be 8-64 characters using letters, numbers, '-' or '_'.",
            retryable=False,
        )
    if not payload.messages:
        raise BridgeError(
            status_code=400,
            code="bridge_validation_failed",
            message="messages must contain at least one item.",
            retryable=False,
        )
    if len(payload.messages) > OPENCLAW_BRIDGE_MAX_MESSAGES:
        raise BridgeError(
            status_code=400,
            code="bridge_validation_failed",
            message="messages exceeds the allowed replay window.",
            retryable=False,
        )
    total_chars = sum(len(item.text) for item in payload.messages)
    if total_chars > OPENCLAW_BRIDGE_MAX_CHARS:
        raise BridgeError(
            status_code=400,
            code="bridge_validation_failed",
            message="messages exceed the allowed character budget.",
            retryable=False,
        )
    if payload.messages[-1].role != "user":
        raise BridgeError(
            status_code=400,
            code="bridge_validation_failed",
            message="The final replay message must be from the user.",
            retryable=False,
        )
    for message in payload.messages:
        if message.role not in {"user", "assistant"}:
            raise BridgeError(
                status_code=400,
                code="bridge_validation_failed",
                message="messages may contain only 'user' and 'assistant' roles.",
                retryable=False,
            )


def _build_command(prompt: str) -> list[str]:
    return [
        OPENCLAW_BRIDGE_DOCKER_BIN,
        "exec",
        "-i",
        OPENCLAW_BRIDGE_CONTAINER,
        OPENCLAW_BRIDGE_OPENCLAW_BIN,
        "agent",
        "--agent",
        OPENCLAW_BRIDGE_AGENT_ID,
        "--json",
        "--message",
        prompt,
    ]


def _run_command(prompt: str) -> dict[str, str]:
    try:
        completed = subprocess.run(
            _build_command(prompt),
            capture_output=True,
            text=True,
            timeout=OPENCLAW_BRIDGE_COMMAND_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise BridgeError(
            status_code=504,
            code="bridge_timeout",
            message="OpenClaw bridge command timed out.",
            retryable=True,
        ) from exc
    except OSError as exc:
        raise BridgeError(
            status_code=502,
            code="bridge_subprocess_failed",
            message="OpenClaw bridge could not start the agent command.",
            retryable=False,
        ) from exc

    if completed.returncode != 0:
        raise BridgeError(
            status_code=502,
            code="bridge_subprocess_failed",
            message=(completed.stderr or "OpenClaw bridge command failed.").strip(),
            retryable=False,
        )

    stdout = completed.stdout.strip()
    if not stdout:
        raise BridgeError(
            status_code=502,
            code="bridge_parse_failed",
            message="OpenClaw bridge command returned empty output.",
            retryable=False,
        )

    parsed: Optional[Any] = None
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError:
        for line in reversed(stdout.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
                break
            except json.JSONDecodeError:
                continue
        if parsed is None:
            raise BridgeError(
                status_code=502,
                code="bridge_parse_failed",
                message="OpenClaw bridge could not parse agent JSON output.",
                retryable=False,
            )

    text = _extract_assistant_text(parsed)
    timestamp = _utc_now_iso()
    if isinstance(parsed, dict) and isinstance(parsed.get("message"), dict):
        timestamp = str(parsed["message"].get("timestamp") or "").strip() or timestamp
    elif isinstance(parsed, dict):
        timestamp = str(parsed.get("created_at") or "").strip() or timestamp

    return {
        "role": "assistant",
        "text": text,
        "timestamp": timestamp,
    }


@app.exception_handler(BridgeError)
async def bridge_error_handler(request: Request, exc: BridgeError):
    request_id = str(getattr(request.state, "request_id", "") or uuid.uuid4().hex[:12])
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "requestId": request_id,
            "error": {
                "code": exc.code,
                "message": exc.message,
                "retryable": exc.retryable,
            },
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    first = exc.errors()[0] if exc.errors() else {}
    request_id = str(getattr(request.state, "request_id", "") or uuid.uuid4().hex[:12])
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "requestId": request_id,
            "error": {
                "code": "bridge_validation_failed",
                "message": str(first.get("msg") or "Invalid bridge request."),
                "retryable": False,
            },
        },
    )


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    request.state.request_id = str(request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12])
    raw_length = str(request.headers.get("content-length") or "").strip()
    if raw_length:
        try:
            content_length = int(raw_length)
        except ValueError:
            content_length = 0
        if content_length > OPENCLAW_BRIDGE_HTTP_MAX_BODY_BYTES:
            return JSONResponse(
                status_code=413,
                content={
                    "ok": False,
                    "requestId": request.state.request_id,
                    "error": {
                        "code": "bridge_validation_failed",
                        "message": "Bridge request body is too large.",
                        "retryable": False,
                    },
                },
            )
    response = await call_next(request)
    response.headers["X-Request-ID"] = request.state.request_id
    return response


@app.get("/health")
async def health():
    return {"status": "ok", "agentId": OPENCLAW_BRIDGE_AGENT_ID}


@app.post("/openclaw-agent/chat")
async def openclaw_agent_chat(
    request: Request,
    payload: BridgeChatRequest,
    authorization: Optional[str] = Header(default=None),
):
    supplied = _extract_bearer_token(authorization)
    if not supplied or not OPENCLAW_BRIDGE_TOKEN or not hmac.compare_digest(supplied, OPENCLAW_BRIDGE_TOKEN):
        raise BridgeError(
            status_code=401,
            code="bridge_auth_failed",
            message="Bridge authorization failed.",
            retryable=False,
        )

    _validate_payload(payload)
    request_id = payload.requestId or str(getattr(request.state, "request_id", "") or uuid.uuid4().hex[:12])
    session_id = payload.sessionId
    started_at = time.perf_counter()

    try:
        message = _run_command(_render_prompt(payload.messages))
    except BridgeError as exc:
        _log_event(
            logging.WARNING,
            {
                "level": "warning",
                "message": "bridge_request_failed",
                "request_id": request_id,
                "session_id": session_id,
                "agent_id": OPENCLAW_BRIDGE_AGENT_ID,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "result": "error",
                "failure_category": exc.code,
            },
        )
        raise

    _log_event(
        logging.INFO,
        {
            "level": "info",
            "message": "bridge_request_completed",
            "request_id": request_id,
            "session_id": session_id,
            "agent_id": OPENCLAW_BRIDGE_AGENT_ID,
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "result": "success",
        },
    )
    return {
        "ok": True,
        "requestId": request_id,
        "message": message,
    }
