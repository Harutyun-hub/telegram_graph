"""Materialized topic-overview summaries for the topic detail page."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

import config
from api.admin_runtime import get_admin_prompt, get_admin_runtime_value
from api.dashboard_dates import DashboardDateContext
from api.queries import comparative
from buffer.supabase_writer import SupabaseWriter

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]


_cache_lock = threading.Lock()
_cached_snapshot: dict = {"generatedAt": None, "window": {}, "items": []}
_cached_index: dict[str, dict] = {}
_cache_ts: float = 0.0
_state_cache: dict = {}
_last_refresh_diagnostics: dict = {}

_runtime_store_lock = threading.Lock()
_runtime_store: SupabaseWriter | None = None

_SNAPSHOT_FOLDER = "topic_overviews/snapshots"
_STATE_FOLDER = "topic_overviews/state"
_LOCK_FOLDER = "topic_overviews/locks"
_SCHEMA_VERSION = 1
_INSTANCE_ID = f"{os.getpid()}-{int(time.time())}"

_client = OpenAI(api_key=config.OPENAI_API_KEY) if (OpenAI and config.OPENAI_API_KEY) else None

TOPIC_OVERVIEWS_SYNTHESIS_PROMPT = """
You generate a concise, evidence-grounded, insight-rich overview for one community topic.

The user already sees the chart, mentions, growth, sentiment, and top channels on the page.
Your job is not to repeat those stats. Your job is to explain what the conversation is actually about, what patterns are emerging, and what matters in the discussion.

Rules:
1) Use only the provided evidence, question-style evidence, and metrics. Do not invent causes, actors, numbers, or conclusions not supported by the evidence.
2) Do not restate raw numbers, percentages, trend lines, or channel names unless they are essential to explain a meaningful development that is not otherwise obvious.
3) Prioritize insight over recap. Focus on the dominant narrative, repeated complaints or concerns, visible tensions, expectations, unresolved questions, and what these signals suggest about the topic.
4) Synthesize across posts and comments. Highlight recurring patterns, not isolated claims from a single message.
5) Keep the tone analytical, professional, and useful to a decision-maker.
6) summaryEn and summaryRu should each be one short paragraph, maximum 2 sentences. They should tell the reader what this topic is really about right now and why it matters.
7) signalsEn and signalsRu must each contain exactly 3 short bullets. Every bullet must contain a distinct insight, not a reformatted statistic.
8) Avoid generic bullets such as "discussion remains active", "negative sentiment dominates", "mentions fell", or other observations the user can already see in the UI.
9) If the evidence is mixed, contradictory, or weak, describe the tension or uncertainty instead of forcing a strong conclusion.
10) Russian output must be natural and professional, not a literal translation.

Return JSON only:
{
  "overview": {
    "summaryEn": "string",
    "summaryRu": "string",
    "signalsEn": ["string", "string", "string"],
    "signalsRu": ["string", "string", "string"]
  }
}
""".strip()

ADMIN_PROMPT_DEFAULTS = {
    "topic_overviews.synthesis_prompt": TOPIC_OVERVIEWS_SYNTHESIS_PROMPT,
}


def _new_refresh_diagnostics(*, force: bool = False, ctx: DashboardDateContext | None = None) -> dict:
    return {
        "force": bool(force),
        "exitReason": "",
        "error": "",
        "window": {
            "from": ctx.from_date.isoformat() if ctx else "",
            "to": ctx.to_date.isoformat() if ctx else "",
            "days": int(ctx.days) if ctx else int(getattr(config, "TOPIC_OVERVIEWS_WINDOW_DAYS", 14)),
        },
        "runtime": {
            "hasOpenAIClient": bool(_client),
            "featureEnabled": bool(_runtime_topic_overviews_feature_enabled()),
            "model": _runtime_topic_overviews_model(config.TOPIC_OVERVIEWS_MODEL),
            "promptVersion": _runtime_topic_overviews_prompt_version(),
            "refreshMinutes": _runtime_topic_overviews_refresh_minutes(),
        },
        "config": {
            "maxTopics": int(config.TOPIC_OVERVIEWS_MAX_TOPICS),
            "evidencePerTopic": int(config.TOPIC_OVERVIEWS_EVIDENCE_PER_TOPIC),
            "questionLimit": int(config.TOPIC_OVERVIEWS_QUESTION_LIMIT),
            "minEvidence": int(config.TOPIC_OVERVIEWS_MIN_EVIDENCE),
            "minUsers": int(config.TOPIC_OVERVIEWS_MIN_USERS),
            "minChannels": int(config.TOPIC_OVERVIEWS_MIN_CHANNELS),
            "maxConcurrency": int(config.TOPIC_OVERVIEWS_MAX_CONCURRENCY),
        },
        "stages": {
            "candidateRows": 0,
            "activeTopics": 0,
            "changedTopics": 0,
            "reusedTopics": 0,
            "readyTopics": 0,
            "fallbackTopics": 0,
            "insufficientTopics": 0,
            "finalTopics": 0,
        },
        "snapshot": {
            "loadedItems": 0,
            "writeAttempted": False,
            "writeSucceeded": False,
            "readbackItems": 0,
        },
    }


def _store_refresh_diagnostics(diagnostics: dict) -> None:
    global _last_refresh_diagnostics
    _last_refresh_diagnostics = copy.deepcopy(diagnostics)


def _as_str(value: Any, default: str = "") -> str:
    if isinstance(value, str):
        return value
    if value is None:
        return default
    return str(value)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _trim_text(value: Any, limit: int) -> str:
    text = _as_str(value, "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _window_payload(ctx: DashboardDateContext) -> dict[str, Any]:
    return {
        "from": ctx.from_date.isoformat(),
        "to": ctx.to_date.isoformat(),
        "days": int(ctx.days),
    }


def _topic_key(topic: str, category: str) -> str:
    return f"{topic.strip().lower()}|{category.strip().lower()}"


def _summarize_sentiment(candidate: dict) -> tuple[str, str]:
    positive = max(0, _as_int(candidate.get("sentimentPositive"), 0))
    neutral = max(0, _as_int(candidate.get("sentimentNeutral"), 0))
    negative = max(0, _as_int(candidate.get("sentimentNegative"), 0))
    if negative >= max(positive, neutral):
        return "negative", "негативной"
    if positive >= max(negative, neutral):
        return "positive", "позитивной"
    return "mixed", "смешанной"


def _runtime_prompt(key: str, default: str) -> str:
    return get_admin_prompt(key, default)


def _runtime_topic_overviews_model(default: str) -> str:
    value = get_admin_runtime_value("topicOverviewsModel", default)
    text = _as_str(value, default).strip()
    return text or default


def _runtime_topic_overviews_prompt_version() -> str:
    value = get_admin_runtime_value("topicOverviewsPromptVersion", config.TOPIC_OVERVIEWS_PROMPT_VERSION)
    text = _as_str(value, config.TOPIC_OVERVIEWS_PROMPT_VERSION).strip()
    return text or config.TOPIC_OVERVIEWS_PROMPT_VERSION


def _runtime_topic_overviews_feature_enabled() -> bool:
    value = get_admin_runtime_value("featureTopicOverviewsAi", config.FEATURE_TOPIC_OVERVIEWS_AI)
    if isinstance(value, bool):
        return value
    return bool(config.FEATURE_TOPIC_OVERVIEWS_AI)


def _runtime_topic_overviews_refresh_minutes() -> int:
    value = get_admin_runtime_value("topicOverviewsRefreshMinutes", str(config.TOPIC_OVERVIEWS_REFRESH_MINUTES))
    try:
        return max(15, int(str(value).strip()))
    except Exception:
        return max(15, int(config.TOPIC_OVERVIEWS_REFRESH_MINUTES))


def get_topic_overviews_refresh_minutes() -> int:
    return _runtime_topic_overviews_refresh_minutes()


def get_admin_prompt_defaults() -> dict[str, str]:
    return dict(ADMIN_PROMPT_DEFAULTS)


def _get_runtime_store() -> SupabaseWriter | None:
    global _runtime_store
    with _runtime_store_lock:
        if _runtime_store is not None:
            return _runtime_store
        try:
            _runtime_store = SupabaseWriter()
        except Exception as exc:
            logger.warning(f"Topic overviews runtime store unavailable: {exc}")
            _runtime_store = None
    return _runtime_store


def _read_latest_runtime_json(folder: str, default: dict | None = None) -> dict:
    fallback = dict(default or {})
    store = _get_runtime_store()
    if not store:
        return fallback

    rows = store.list_runtime_files(folder)
    if not rows:
        return fallback

    json_rows = [row for row in rows if _as_str(row.get("name"), "").endswith(".json")]
    if not json_rows:
        return fallback

    latest = sorted(
        json_rows,
        key=lambda row: (_as_str(row.get("updated_at"), ""), _as_str(row.get("name"), "")),
        reverse=True,
    )[0]
    name = _as_str(latest.get("name"), "")
    if not name:
        return fallback
    return store.get_runtime_json(f"{folder}/{name}", default=fallback)


def _write_versioned_runtime_json(folder: str, payload: dict) -> bool:
    store = _get_runtime_store()
    if not store:
        return False

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    digest = hashlib.sha1(json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")).hexdigest()[:10]
    key = f"{folder}/{stamp}-{_INSTANCE_ID}-{digest}.json"
    saved = store.save_runtime_json(key, payload)
    if saved:
        _prune_runtime_folder(folder, keep=12)
    return saved


def _prune_runtime_folder(folder: str, keep: int = 12) -> None:
    store = _get_runtime_store()
    if not store:
        return
    rows = store.list_runtime_files(folder)
    json_rows = [row for row in rows if _as_str(row.get("name"), "").endswith(".json")]
    if len(json_rows) <= keep:
        return
    sorted_rows = sorted(
        json_rows,
        key=lambda row: (_as_str(row.get("updated_at"), ""), _as_str(row.get("name"), "")),
        reverse=True,
    )
    stale = sorted_rows[keep:]
    delete_paths = [f"{folder}/{_as_str(row.get('name'), '')}" for row in stale if _as_str(row.get("name"), "")]
    if delete_paths:
        store.delete_runtime_files(delete_paths)


def _acquire_refresh_lease(ttl_seconds: int) -> bool:
    store = _get_runtime_store()
    if not store:
        return True

    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    key = f"{_LOCK_FOLDER}/{stamp}-{_INSTANCE_ID}.json"
    payload = {
        "owner": _INSTANCE_ID,
        "createdAt": now.isoformat(),
        "expiresAt": (now + timedelta(seconds=max(120, ttl_seconds))).isoformat(),
    }
    if not store.save_runtime_json(key, payload):
        return True

    rows = store.list_runtime_files(_LOCK_FOLDER)
    json_rows = [row for row in rows if _as_str(row.get("name"), "").endswith(".json")]
    if not json_rows:
        return True

    latest = sorted(
        json_rows,
        key=lambda row: (_as_str(row.get("updated_at"), ""), _as_str(row.get("name"), "")),
        reverse=True,
    )[0]
    latest_name = _as_str(latest.get("name"), "")
    own_name = key.rsplit("/", 1)[1]
    if latest_name == own_name:
        _prune_runtime_folder(_LOCK_FOLDER, keep=20)
        return True

    latest_lock = store.get_runtime_json(f"{_LOCK_FOLDER}/{latest_name}", default={})
    expires = _as_str(latest_lock.get("expiresAt"), "")
    if expires:
        try:
            expires_at = datetime.fromisoformat(expires.replace("Z", "+00:00")).astimezone(timezone.utc)
            if expires_at <= now:
                return True
        except Exception:
            return True
    return False


def _load_state() -> dict:
    global _state_cache
    if isinstance(_state_cache, dict) and _state_cache.get("schemaVersion") == _SCHEMA_VERSION:
        return _state_cache

    state = _read_latest_runtime_json(
        _STATE_FOLDER,
        default={
            "schemaVersion": _SCHEMA_VERSION,
            "updatedAt": None,
            "topics": {},
        },
    )
    if not isinstance(state, dict):
        state = {"schemaVersion": _SCHEMA_VERSION, "updatedAt": None, "topics": {}}
    if not isinstance(state.get("topics"), dict):
        state["topics"] = {}
    state["schemaVersion"] = _SCHEMA_VERSION
    _state_cache = state
    return state


def _save_state(state: dict) -> bool:
    global _state_cache
    state["schemaVersion"] = _SCHEMA_VERSION
    state["updatedAt"] = _now_iso()
    saved = _write_versioned_runtime_json(_STATE_FOLDER, state)
    if saved:
        _state_cache = state
        return True
    logger.error("Topic overviews state persistence failed verification")
    return False


def _default_snapshot_payload() -> dict:
    return {
        "generatedAt": None,
        "source": "materialized",
        "window": {},
        "items": [],
    }


def _load_snapshot_payload(*, diagnostics: dict | None = None) -> dict:
    snapshot = _read_latest_runtime_json(_SNAPSHOT_FOLDER, default=_default_snapshot_payload())
    if not isinstance(snapshot, dict):
        snapshot = _default_snapshot_payload()
    items = snapshot.get("items")
    if not isinstance(items, list):
        snapshot["items"] = []
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("snapshot", {})["loadedItems"] = len(snapshot.get("items") or [])
    return snapshot


def _save_snapshot_payload(
    items: list[dict],
    ctx: DashboardDateContext,
    metadata: dict | None = None,
    diagnostics: dict | None = None,
) -> bool:
    payload: dict[str, Any] = {
        "generatedAt": _now_iso(),
        "source": "materialized",
        "window": _window_payload(ctx),
        "items": items,
    }
    if isinstance(metadata, dict) and metadata:
        payload["meta"] = metadata
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("snapshot", {})["writeAttempted"] = True
    saved = _write_versioned_runtime_json(_SNAPSHOT_FOLDER, payload)
    readback = _load_snapshot_payload(diagnostics=diagnostics) if saved else _default_snapshot_payload()
    readback_items = readback.get("items") if isinstance(readback, dict) else []
    readback_ok = isinstance(readback_items, list) and len(readback_items) == len(items)
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("snapshot", {})["readbackItems"] = len(readback_items or [])
        diagnostics.setdefault("snapshot", {})["writeSucceeded"] = bool(saved and readback_ok)
    return bool(saved and readback_ok)


def _snapshot_index(payload: dict) -> dict[str, dict]:
    items = payload.get("items") if isinstance(payload, dict) else []
    if not isinstance(items, list):
        return {}
    index: dict[str, dict] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        key = _topic_key(_as_str(item.get("topic")), _as_str(item.get("category")))
        if key and key not in index:
            index[key] = item
    return index


def _set_cached_snapshot(snapshot: dict) -> dict:
    global _cached_snapshot, _cached_index, _cache_ts
    payload = snapshot if isinstance(snapshot, dict) else _default_snapshot_payload()
    if not isinstance(payload.get("items"), list):
        payload = {**payload, "items": []}
    with _cache_lock:
        _cached_snapshot = copy.deepcopy(payload)
        _cached_index = _snapshot_index(payload)
        _cache_ts = time.time()
        return copy.deepcopy(_cached_snapshot)


def _cache_valid(now: float) -> bool:
    return _cache_ts > 0 and (now - _cache_ts) < int(config.TOPIC_OVERVIEWS_CACHE_TTL_SECONDS)


def invalidate_topic_overviews_cache() -> None:
    global _cached_snapshot, _cached_index, _cache_ts, _state_cache, _last_refresh_diagnostics
    with _cache_lock:
        _cached_snapshot = _default_snapshot_payload()
        _cached_index = {}
        _cache_ts = 0.0
    _state_cache = {}
    _last_refresh_diagnostics = {}


def _support_gate(candidate: dict) -> bool:
    return (
        _as_int(candidate.get("evidenceCount"), 0) >= int(config.TOPIC_OVERVIEWS_MIN_EVIDENCE)
        and _as_int(candidate.get("distinctUsers"), 0) >= int(config.TOPIC_OVERVIEWS_MIN_USERS)
        and _as_int(candidate.get("distinctChannels"), 0) >= int(config.TOPIC_OVERVIEWS_MIN_CHANNELS)
    )


def _candidate_fingerprint(candidate: dict, ctx: DashboardDateContext) -> str:
    payload = {
        "topic": _as_str(candidate.get("topic")),
        "category": _as_str(candidate.get("category")),
        "mentions": _as_int(candidate.get("mentions"), 0),
        "previousMentions": _as_int(candidate.get("previousMentions"), 0),
        "growth": _as_int(candidate.get("growth"), 0),
        "distinctUsers": _as_int(candidate.get("distinctUsers"), 0),
        "distinctChannels": _as_int(candidate.get("distinctChannels"), 0),
        "sentimentPositive": _as_int(candidate.get("sentimentPositive"), 0),
        "sentimentNeutral": _as_int(candidate.get("sentimentNeutral"), 0),
        "sentimentNegative": _as_int(candidate.get("sentimentNegative"), 0),
        "topChannels": [_as_str(ch) for ch in (candidate.get("topChannels") or [])[:3]],
        "latestAt": _as_str(candidate.get("latestAt")),
        "window": _window_payload(ctx),
        "promptVersion": _runtime_topic_overviews_prompt_version(),
        "model": _runtime_topic_overviews_model(config.TOPIC_OVERVIEWS_MODEL),
        "evidence": [
            {
                "id": _as_str(ev.get("id")),
                "text": _trim_text(ev.get("text"), 220),
                "channel": _as_str(ev.get("channel")),
                "timestamp": _as_str(ev.get("timestamp")),
            }
            for ev in (candidate.get("evidence") or [])[: int(config.TOPIC_OVERVIEWS_EVIDENCE_PER_TOPIC)]
            if isinstance(ev, dict)
        ],
        "questionEvidence": [
            {
                "id": _as_str(ev.get("id")),
                "text": _trim_text(ev.get("text"), 180),
                "channel": _as_str(ev.get("channel")),
                "timestamp": _as_str(ev.get("timestamp")),
            }
            for ev in (candidate.get("questionEvidence") or [])[: int(config.TOPIC_OVERVIEWS_QUESTION_LIMIT)]
            if isinstance(ev, dict)
        ],
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _build_evidence_ids(candidate: dict) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for section in ("evidence", "questionEvidence"):
        for row in candidate.get(section) or []:
            if not isinstance(row, dict):
                continue
            evidence_id = _as_str(row.get("id"), "").strip()
            if evidence_id and evidence_id not in seen:
                seen.add(evidence_id)
                output.append(evidence_id)
    return output[:6]


def _insufficient_item(candidate: dict, ctx: DashboardDateContext) -> dict:
    return {
        "topic": _as_str(candidate.get("topic"), ""),
        "category": _as_str(candidate.get("category"), "General"),
        "status": "insufficient_evidence",
        "summaryEn": "",
        "summaryRu": "",
        "signalsEn": [],
        "signalsRu": [],
        "generatedAt": _now_iso(),
        "windowStart": ctx.from_date.isoformat(),
        "windowEnd": ctx.to_date.isoformat(),
        "windowDays": int(ctx.days),
        "evidenceIds": _build_evidence_ids(candidate),
    }


def _fallback_item(candidate: dict, ctx: DashboardDateContext) -> dict:
    topic = _as_str(candidate.get("topic"), "Topic")
    mentions = _as_int(candidate.get("mentions"), 0)
    previous_mentions = _as_int(candidate.get("previousMentions"), 0)
    growth = _as_int(candidate.get("growth"), 0)
    positive = _as_int(candidate.get("sentimentPositive"), 0)
    negative = _as_int(candidate.get("sentimentNegative"), 0)
    distinct_users = _as_int(candidate.get("distinctUsers"), 0)
    distinct_channels = _as_int(candidate.get("distinctChannels"), 0)
    top_channels = [_as_str(ch) for ch in (candidate.get("topChannels") or []) if _as_str(ch)]
    channel_label = ", ".join(top_channels[:2]) if top_channels else "multiple channels"
    channel_label_ru = ", ".join(top_channels[:2]) if top_channels else "нескольких каналах"
    _, sentiment_ru = _summarize_sentiment(candidate)
    question_count = len(candidate.get("questionEvidence") or [])
    latest_at = _as_str(candidate.get("latestAt"), "")[:10]

    summary_en = (
        f'"{topic}" remains an active topic in the rolling AI window, with {mentions} mentions '
        f'({growth:+d}% versus the previous comparison window). Discussion is concentrated in {channel_label} '
        f'and currently leans {"negative" if negative >= positive else "mixed-to-positive"}.'
    )
    summary_ru = (
        f'Тема "{topic}" остаётся заметной в скользящем AI-окне: {mentions} упоминаний '
        f'({growth:+d}% к предыдущему окну сравнения). Обсуждение сосредоточено в {channel_label_ru} '
        f'и сейчас имеет {sentiment_ru} тональность.'
    )
    signals_en = [
        f"Volume: {mentions} mentions versus {previous_mentions} in the previous window.",
        f"Sentiment: {negative}% negative, {positive}% positive, across {distinct_users} distinct participants.",
        (
            f"Questions are recurring in this topic ({question_count} recent question-style messages)."
            if question_count > 0
            else f"Breadth: activity spans {distinct_channels} channels; latest signal date {latest_at or 'recent'}."
        ),
    ]
    signals_ru = [
        f"Объём: {mentions} упоминаний против {previous_mentions} в предыдущем окне.",
        f"Тональность: {negative}% негатива и {positive}% позитива при {distinct_users} уникальных участниках.",
        (
            f"По теме регулярно появляются вопросы ({question_count} недавних сообщений с вопросительной формой)."
            if question_count > 0
            else f"Ширина сигнала: активность идёт в {distinct_channels} каналах; последний сигнал датирован {latest_at or 'недавним периодом'}."
        ),
    ]
    return {
        "topic": topic,
        "category": _as_str(candidate.get("category"), "General"),
        "status": "fallback",
        "summaryEn": _trim_text(summary_en, 320),
        "summaryRu": _trim_text(summary_ru, 360),
        "signalsEn": [_trim_text(item, 140) for item in signals_en[:3]],
        "signalsRu": [_trim_text(item, 160) for item in signals_ru[:3]],
        "generatedAt": _now_iso(),
        "windowStart": ctx.from_date.isoformat(),
        "windowEnd": ctx.to_date.isoformat(),
        "windowDays": int(ctx.days),
        "evidenceIds": _build_evidence_ids(candidate),
    }


def _normalize_ai_item(parsed: dict, candidate: dict, ctx: DashboardDateContext) -> dict | None:
    if not isinstance(parsed, dict):
        return None
    overview = parsed.get("overview")
    if not isinstance(overview, dict):
        return None

    summary_en = _trim_text(overview.get("summaryEn"), 320)
    summary_ru = _trim_text(overview.get("summaryRu"), 360)
    signals_en = overview.get("signalsEn") if isinstance(overview.get("signalsEn"), list) else []
    signals_ru = overview.get("signalsRu") if isinstance(overview.get("signalsRu"), list) else []
    clean_signals_en = [_trim_text(item, 140) for item in signals_en if _trim_text(item, 140)]
    clean_signals_ru = [_trim_text(item, 160) for item in signals_ru if _trim_text(item, 160)]

    if not summary_en or not summary_ru or len(clean_signals_en) < 3 or len(clean_signals_ru) < 3:
        return None

    return {
        "topic": _as_str(candidate.get("topic"), ""),
        "category": _as_str(candidate.get("category"), "General"),
        "status": "ready",
        "summaryEn": summary_en,
        "summaryRu": summary_ru,
        "signalsEn": clean_signals_en[:3],
        "signalsRu": clean_signals_ru[:3],
        "generatedAt": _now_iso(),
        "windowStart": ctx.from_date.isoformat(),
        "windowEnd": ctx.to_date.isoformat(),
        "windowDays": int(ctx.days),
        "evidenceIds": _build_evidence_ids(candidate),
    }


def _chat_json(*, model: str, max_tokens: int, system_prompt: str, user_payload: dict) -> dict:
    if not _client:
        return {}
    response = _client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        response_format={"type": "json_object"},
        max_completion_tokens=max_tokens,
        timeout=config.AI_REQUEST_TIMEOUT_SECONDS,
    )
    raw = _as_str(response.choices[0].message.content)
    return json.loads(raw) if raw else {}


def _generate_item(candidate: dict, ctx: DashboardDateContext) -> dict:
    if not _support_gate(candidate):
        return _insufficient_item(candidate, ctx)
    if not _client or not _runtime_topic_overviews_feature_enabled():
        return _fallback_item(candidate, ctx)

    payload = {
        "window": _window_payload(ctx),
        "topic": {
            "name": _as_str(candidate.get("topic"), ""),
            "category": _as_str(candidate.get("category"), "General"),
            "mentions": _as_int(candidate.get("mentions"), 0),
            "previousMentions": _as_int(candidate.get("previousMentions"), 0),
            "growthPct": _as_int(candidate.get("growth"), 0),
            "distinctUsers": _as_int(candidate.get("distinctUsers"), 0),
            "distinctChannels": _as_int(candidate.get("distinctChannels"), 0),
            "sentiment": {
                "positive": _as_int(candidate.get("sentimentPositive"), 0),
                "neutral": _as_int(candidate.get("sentimentNeutral"), 0),
                "negative": _as_int(candidate.get("sentimentNegative"), 0),
            },
            "topChannels": [_as_str(ch) for ch in (candidate.get("topChannels") or [])[:3]],
            "latestAt": _as_str(candidate.get("latestAt"), ""),
        },
        "evidence": [
            {
                "id": _as_str(ev.get("id"), ""),
                "text": _trim_text(ev.get("text"), 220),
                "channel": _as_str(ev.get("channel"), "unknown"),
                "timestamp": _as_str(ev.get("timestamp"), ""),
            }
            for ev in (candidate.get("evidence") or [])[: int(config.TOPIC_OVERVIEWS_EVIDENCE_PER_TOPIC)]
            if isinstance(ev, dict)
        ],
        "questionEvidence": [
            {
                "id": _as_str(ev.get("id"), ""),
                "text": _trim_text(ev.get("text"), 180),
                "channel": _as_str(ev.get("channel"), "unknown"),
                "timestamp": _as_str(ev.get("timestamp"), ""),
            }
            for ev in (candidate.get("questionEvidence") or [])[: int(config.TOPIC_OVERVIEWS_QUESTION_LIMIT)]
            if isinstance(ev, dict)
        ],
    }
    system_prompt = _runtime_prompt("topic_overviews.synthesis_prompt", TOPIC_OVERVIEWS_SYNTHESIS_PROMPT)
    try:
        parsed = _chat_json(
            model=_runtime_topic_overviews_model(config.TOPIC_OVERVIEWS_MODEL),
            max_tokens=int(config.TOPIC_OVERVIEWS_MAX_TOKENS),
            system_prompt=system_prompt,
            user_payload=payload,
        )
        item = _normalize_ai_item(parsed, candidate, ctx)
        if item is not None:
            return item
    except Exception as exc:
        logger.warning(f"Topic overview synthesis failed | topic={_as_str(candidate.get('topic'))} error={exc}")
    return _fallback_item(candidate, ctx)


def get_topic_overviews_diagnostics() -> dict:
    if not _last_refresh_diagnostics:
        return _new_refresh_diagnostics(force=False)
    diagnostics = copy.deepcopy(_last_refresh_diagnostics)
    diagnostics["itemsProduced"] = diagnostics.get("stages", {}).get("finalTopics", 0)
    return diagnostics


def refresh_topic_overviews_with_diagnostics(
    *,
    ctx: DashboardDateContext | None = None,
    force: bool = False,
) -> dict:
    refresh_topic_overviews(ctx=ctx, force=force)
    return get_topic_overviews_diagnostics()


def refresh_topic_overviews(
    *,
    ctx: DashboardDateContext | None = None,
    force: bool = False,
) -> dict:
    global _cache_ts

    resolved_ctx = ctx or comparative._default_detail_context()
    diagnostics = _new_refresh_diagnostics(force=force, ctx=resolved_ctx)
    with _cache_lock:
        last_good_snapshot = copy.deepcopy(_cached_snapshot)
    if not last_good_snapshot.get("items"):
        last_good_snapshot = _load_snapshot_payload()

    lease_ttl = max(300, _runtime_topic_overviews_refresh_minutes() * 60)
    if not force and not _acquire_refresh_lease(lease_ttl):
        logger.info("Topic overviews materialization skipped: another instance holds active lease")
        diagnostics["exitReason"] = "lease_skipped"
        payload = _load_snapshot_payload(diagnostics=diagnostics)
        _set_cached_snapshot(payload)
        _store_refresh_diagnostics(diagnostics)
        return payload

    try:
        candidates = comparative.get_topic_overview_candidates(
            resolved_ctx,
            limit=int(config.TOPIC_OVERVIEWS_MAX_TOPICS),
            evidence_limit=int(config.TOPIC_OVERVIEWS_EVIDENCE_PER_TOPIC),
            question_limit=int(config.TOPIC_OVERVIEWS_QUESTION_LIMIT),
        )
    except Exception as exc:
        diagnostics["exitReason"] = "candidate_error"
        diagnostics["error"] = str(exc)
        payload = _load_snapshot_payload(diagnostics=diagnostics)
        _set_cached_snapshot(payload)
        _store_refresh_diagnostics(diagnostics)
        return payload

    diagnostics["stages"]["candidateRows"] = len(candidates)
    diagnostics["stages"]["activeTopics"] = len(candidates)
    if not candidates:
        diagnostics["exitReason"] = "no_candidates"
        payload = _load_snapshot_payload(diagnostics=diagnostics)
        _set_cached_snapshot(payload)
        _store_refresh_diagnostics(diagnostics)
        return payload

    state = _load_state()
    topic_state = state.get("topics") if isinstance(state.get("topics"), dict) else {}
    active_keys = {
        _topic_key(_as_str(candidate.get("topic")), _as_str(candidate.get("category"), "General"))
        for candidate in candidates
    }

    fingerprints: dict[str, str] = {}
    changed_candidates: list[dict] = []
    for candidate in candidates:
        key = _topic_key(_as_str(candidate.get("topic")), _as_str(candidate.get("category"), "General"))
        if not key:
            continue
        fingerprint = _candidate_fingerprint(candidate, resolved_ctx)
        fingerprints[key] = fingerprint
        record = topic_state.get(key) if isinstance(topic_state, dict) else None
        if (not force) and isinstance(record, dict) and _as_str(record.get("fingerprint"), "") == fingerprint:
            continue
        changed_candidates.append(candidate)

    diagnostics["stages"]["changedTopics"] = len(changed_candidates)
    diagnostics["stages"]["reusedTopics"] = max(0, len(candidates) - len(changed_candidates))

    processed_records: dict[str, dict] = {}
    max_workers = max(1, min(int(config.TOPIC_OVERVIEWS_MAX_CONCURRENCY), len(changed_candidates) or 1))
    if changed_candidates:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="topic-overview") as executor:
            future_map = {
                executor.submit(_generate_item, candidate, resolved_ctx): candidate
                for candidate in changed_candidates
            }
            for future in as_completed(future_map):
                candidate = future_map[future]
                key = _topic_key(_as_str(candidate.get("topic")), _as_str(candidate.get("category"), "General"))
                try:
                    item = future.result()
                except Exception as exc:
                    logger.warning(f"Topic overview generation crashed | topic={_as_str(candidate.get('topic'))} error={exc}")
                    item = _fallback_item(candidate, resolved_ctx)

                status = _as_str(item.get("status"), "fallback")
                if status == "ready":
                    diagnostics["stages"]["readyTopics"] += 1
                elif status == "insufficient_evidence":
                    diagnostics["stages"]["insufficientTopics"] += 1
                else:
                    diagnostics["stages"]["fallbackTopics"] += 1

                processed_records[key] = {
                    "fingerprint": fingerprints.get(key, ""),
                    "status": status,
                    "updatedAt": _now_iso(),
                    "item": item,
                    "topic": _as_str(candidate.get("topic"), ""),
                }

    next_topic_state: dict[str, dict] = {}
    if isinstance(topic_state, dict):
        for key, record in topic_state.items():
            if key not in active_keys or force or not isinstance(record, dict):
                continue
            expected = fingerprints.get(key)
            if not expected or _as_str(record.get("fingerprint"), "") != expected:
                continue
            if not isinstance(record.get("item"), dict):
                continue
            next_topic_state[key] = record

    for key, record in processed_records.items():
        next_topic_state[key] = record

    final_items: list[dict] = []
    for candidate in candidates:
        key = _topic_key(_as_str(candidate.get("topic")), _as_str(candidate.get("category"), "General"))
        record = next_topic_state.get(key)
        if isinstance(record, dict) and isinstance(record.get("item"), dict):
            final_items.append(record["item"])

    diagnostics["stages"]["finalTopics"] = len(final_items)
    state["topics"] = next_topic_state

    state_saved = _save_state(state)
    snapshot_saved = False
    payload = {
        "generatedAt": _now_iso(),
        "source": "materialized",
        "window": _window_payload(resolved_ctx),
        "items": final_items,
    }
    if state_saved:
        snapshot_saved = _save_snapshot_payload(
            final_items,
            resolved_ctx,
            metadata={
                "activeTopics": len(candidates),
                "changedTopics": len(changed_candidates),
                "reusedTopics": diagnostics["stages"]["reusedTopics"],
                "finalTopics": len(final_items),
            },
            diagnostics=diagnostics,
        )
    if not state_saved:
        diagnostics["error"] = "Topic overviews state could not be persisted and verified"
    elif not snapshot_saved:
        diagnostics["error"] = "Topic overviews snapshot could not be persisted and verified"

    if state_saved and snapshot_saved:
        _set_cached_snapshot(payload)
        diagnostics["exitReason"] = "ok"
        result = payload
    else:
        diagnostics["exitReason"] = "persistence_verification_failed"
        result = last_good_snapshot if isinstance(last_good_snapshot, dict) else _default_snapshot_payload()
        if result.get("items"):
            _set_cached_snapshot(result)
            logger.warning("Topic overviews refresh kept last known good snapshot after persistence verification failed")
        else:
            logger.warning("Topic overviews refresh produced data but no last known good snapshot was available")

    _store_refresh_diagnostics(diagnostics)
    logger.info(
        "Topic overviews materialized | active_topics={} changed_topics={} ready={} fallback={} insufficient={} final_items={}".format(
            diagnostics["stages"]["activeTopics"],
            diagnostics["stages"]["changedTopics"],
            diagnostics["stages"]["readyTopics"],
            diagnostics["stages"]["fallbackTopics"],
            diagnostics["stages"]["insufficientTopics"],
            diagnostics["stages"]["finalTopics"],
        )
    )
    return result


def get_topic_overviews_snapshot(*, force_refresh: bool = False) -> dict:
    if force_refresh:
        return refresh_topic_overviews(force=True)

    now = time.time()
    with _cache_lock:
        if _cache_valid(now):
            return copy.deepcopy(_cached_snapshot)

    payload = _load_snapshot_payload()
    return _set_cached_snapshot(payload)


def get_topic_overview(topic_name: str, category: str | None = None) -> dict | None:
    clean_topic = _as_str(topic_name, "").strip()
    clean_category = _as_str(category, "General").strip() or "General"
    if not clean_topic:
        return None

    now = time.time()
    with _cache_lock:
        if _cache_valid(now):
            item = _cached_index.get(_topic_key(clean_topic, clean_category))
            return copy.deepcopy(item) if isinstance(item, dict) else None

    payload = _set_cached_snapshot(_load_snapshot_payload())
    index = _snapshot_index(payload)
    item = index.get(_topic_key(clean_topic, clean_category))
    return copy.deepcopy(item) if isinstance(item, dict) else None


def get_category_overview(category_name: str, topics: list[dict[str, Any]] | None = None) -> dict | None:
    clean_category = _as_str(category_name, "").strip()
    topic_rows = [row for row in (topics or []) if isinstance(row, dict) and _as_str(row.get("name"), "").strip()]
    if not clean_category or not topic_rows:
        return None

    payload = get_topic_overviews_snapshot()
    index = _snapshot_index(payload)
    generated_at = _as_str(payload.get("generatedAt"), "")

    matched_overviews: list[dict[str, Any]] = []
    for row in topic_rows:
        topic_name = _as_str(row.get("name"), "").strip()
        item = index.get(_topic_key(topic_name, clean_category))
        if isinstance(item, dict):
            matched_overviews.append(item)

    if not matched_overviews:
        return None

    lead_topics = [topic_name for topic_name in [_as_str(row.get("name"), "").strip() for row in topic_rows[:3]] if topic_name]
    lead_topic_phrase_en = ", ".join(lead_topics[:-1]) + (f", and {lead_topics[-1]}" if len(lead_topics) > 2 else (f" and {lead_topics[-1]}" if len(lead_topics) == 2 else (lead_topics[0] if lead_topics else "its main topics")))
    lead_topic_phrase_ru = ", ".join(lead_topics[:-1]) + (f" и {lead_topics[-1]}" if len(lead_topics) >= 2 else (lead_topics[0] if lead_topics else "ключевые темы"))

    def _unique_signals(key: str) -> list[str]:
        seen: set[str] = set()
        output: list[str] = []
        for item in matched_overviews:
            for signal in item.get(key) or []:
                text = _as_str(signal, "").strip()
                lowered = text.lower()
                if not text or lowered in seen:
                    continue
                seen.add(lowered)
                output.append(text)
                if len(output) >= 3:
                    return output
        return output

    signals_en = _unique_signals("signalsEn")
    signals_ru = _unique_signals("signalsRu")

    summary_fragments_en = [
        _as_str(item.get("summaryEn"), "").strip()
        for item in matched_overviews
        if _as_str(item.get("summaryEn"), "").strip()
    ]
    summary_fragments_ru = [
        _as_str(item.get("summaryRu"), "").strip()
        for item in matched_overviews
        if _as_str(item.get("summaryRu"), "").strip()
    ]

    summary_en = (
        f"{clean_category} is currently being shaped by {lead_topic_phrase_en}. "
        f"The strongest recurring patterns in this category point to {signals_en[0].rstrip('.')}."
        if signals_en
        else (summary_fragments_en[0] if summary_fragments_en else "")
    )
    if len(signals_en) > 1:
        summary_en += f" Analysts should also watch how {signals_en[1].rstrip('.').lower()}."

    summary_ru = (
        f"Сейчас категорию {clean_category} формируют {lead_topic_phrase_ru}. "
        f"Наиболее устойчивый повторяющийся сигнал здесь: {signals_ru[0].rstrip('.')}."
        if signals_ru
        else (summary_fragments_ru[0] if summary_fragments_ru else "")
    )
    if len(signals_ru) > 1:
        summary_ru += f" Дополнительно важно отслеживать, как {signals_ru[1].rstrip('.').lower()}."

    return {
        "category": clean_category,
        "status": "derived_from_topics",
        "summaryEn": summary_en.strip(),
        "summaryRu": summary_ru.strip(),
        "signalsEn": signals_en[:3],
        "signalsRu": signals_ru[:3],
        "generatedAt": generated_at,
        "sourceTopics": lead_topics,
    }
