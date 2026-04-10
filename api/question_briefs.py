"""AI-generated Question Cards with strict evidence grounding."""

from __future__ import annotations

import copy
import json
import hashlib
import re
import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any

from loguru import logger

import config
from api.admin_runtime import get_admin_prompt, get_admin_runtime_value
from api.queries import strategic
from buffer.supabase_writer import SupabaseWriter
from utils.ai_usage import log_openai_usage

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]


_cache_lock = threading.Lock()
_cached_cards: list[dict] = []
_cache_ts: float = 0.0
_state_cache: dict = {}
_last_refresh_diagnostics: dict = {}

_runtime_store_lock = threading.Lock()
_runtime_store: SupabaseWriter | None = None

_SNAPSHOT_FOLDER = "question_cards/snapshots"
_STATE_FOLDER = "question_cards/state"
_LOCK_FOLDER = "question_cards/locks"
_SCHEMA_VERSION = 1
_INSTANCE_ID = f"{os.getpid()}-{int(time.time())}"

_client = OpenAI(api_key=config.OPENAI_API_KEY) if (OpenAI and config.OPENAI_API_KEY) else None

QUESTION_BRIEFS_TRIAGE_PROMPT = """
You triage candidate societal question clusters for Question Cards.

Rules:
1) Use only provided evidence snippets and IDs.
2) Accept only if the cluster reflects a combined societal ask.
3) Reject rhetorical, emotional, mixed-topic, and low-grounded clusters.
4) Return JSON only; no extra keys.

Return schema:
{
  "decisions": [
    {
      "clusterId": "string",
      "status": "accepted|rejected",
      "confidence": "high|medium|low",
      "evidenceIds": ["id1", "id2"],
      "rejectionReason": "low_signal|rhetorical_or_emotional|single_user_or_non_societal|mixed_topics|insufficient_grounding|contradictory_evidence"
    }
  ]
}
""".strip()

QUESTION_BRIEFS_SYNTHESIS_PROMPT = """
You generate high-quality Question Cards from evidence clusters.

Rules:
1) Primary output MUST be a societal, cluster-level question in question form.
2) canonicalQuestionEn and canonicalQuestionRu must end with '?'.
3) Do NOT output statement headlines.
4) Do not summarize a single individual anecdote as a societal card.
5) Use only provided evidence text and IDs.
6) If grounding is weak, set confidence to low and omit the card from accepted output.
7) Russian text must be professional and natural.

Return JSON only:
{
  "card": {
    "clusterId": "string",
    "canonicalQuestionEn": "string?",
    "canonicalQuestionRu": "string?",
    "summaryEn": "string",
    "summaryRu": "string",
    "confidence": "high|medium|low",
    "confidenceScore": 0.0,
    "status": "needs_guide|partially_answered|well_covered",
    "resolvedPct": 0,
    "evidenceIds": ["id1", "id2"]
  }
}
""".strip()

ADMIN_PROMPT_DEFAULTS = {
    "question_briefs.triage_prompt": QUESTION_BRIEFS_TRIAGE_PROMPT,
    "question_briefs.synthesis_prompt": QUESTION_BRIEFS_SYNTHESIS_PROMPT,
}

_INTERROGATIVE_HINTS = (
    "how",
    "what",
    "where",
    "when",
    "which",
    "who",
    "why",
    "can",
    "could",
    "should",
    "do",
    "does",
    "is",
    "are",
    "will",
    "сколько",
    "какой",
    "какая",
    "какие",
    "как",
    "где",
    "когда",
    "почему",
    "зачем",
    "можно",
    "нужно",
    "кто",
)

_NOISE_MARKERS = (
    "и что",
    "ну и",
    "серьезно",
    "ага",
    "лол",
    "lmao",
    "omg",
    "wtf",
    "кто виноват",
    "что дальше",
)

_TOKEN_RE = re.compile(r"[a-zA-Z0-9а-яА-ЯёЁ]+")


def _new_refresh_diagnostics(*, force: bool = False) -> dict:
    return {
        "force": bool(force),
        "exitReason": "",
        "error": "",
        "runtime": {
            "hasOpenAIClient": bool(_client),
            "featureEnabled": bool(_runtime_question_briefs_feature_enabled()),
            "aiTriageEnabled": bool(config.QUESTION_BRIEFS_USE_AI_TRIAGE),
        },
        "config": {
            "minClusterMessages": int(config.QUESTION_BRIEFS_MIN_CLUSTER_MESSAGES),
            "minClusterUsers": int(config.QUESTION_BRIEFS_MIN_CLUSTER_USERS),
            "minClusterChannels": int(config.QUESTION_BRIEFS_MIN_CLUSTER_CHANNELS),
            "minConfidence": float(config.QUESTION_BRIEFS_MIN_CONFIDENCE),
            "maxBriefs": int(config.QUESTION_BRIEFS_MAX_BRIEFS),
            "windowDays": int(config.QUESTION_BRIEFS_WINDOW_DAYS),
        },
        "stages": {
            "candidateRows": 0,
            "signalCount": 0,
            "clustersBeforeGate": 0,
            "clustersAfterGate": 0,
            "changedClusters": 0,
            "acceptedClusters": 0,
            "synthesisEligibleClusters": 0,
            "synthesizedRows": 0,
            "cardsBeforeFilter": 0,
            "finalCards": 0,
            "reusedClusters": 0,
        },
        "rejections": {
            "triage": {},
            "materialization": {},
        },
        "snapshot": {
            "loadedCards": 0,
            "writeAttempted": False,
            "writeSucceeded": False,
            "readbackCards": 0,
        },
    }


def _store_refresh_diagnostics(diagnostics: dict) -> None:
    global _last_refresh_diagnostics
    _last_refresh_diagnostics = copy.deepcopy(diagnostics)


def _increment_bucket(target: dict, key: str, count: int = 1) -> None:
    bucket = str(key or "unknown")
    target[bucket] = int(target.get(bucket, 0)) + int(count)


def _first_bucket(buckets: dict) -> str:
    if not isinstance(buckets, dict) or not buckets:
        return ""
    return sorted(
        ((str(name), int(count)) for name, count in buckets.items()),
        key=lambda item: (-item[1], item[0]),
    )[0][0]


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


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clamp(num: float, low: float, high: float) -> float:
    return max(low, min(high, num))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_runtime_store() -> SupabaseWriter | None:
    global _runtime_store
    with _runtime_store_lock:
        if _runtime_store is not None:
            return _runtime_store
        try:
            _runtime_store = SupabaseWriter()
        except Exception as e:
            logger.warning(f"Question cards runtime store unavailable: {e}")
            _runtime_store = None
    return _runtime_store


def _load_runtime_json(path: str, default: dict | None = None) -> dict:
    fallback = default if isinstance(default, dict) else {}
    store = _get_runtime_store()
    if not store:
        return dict(fallback)
    return store.get_runtime_json(path, default=fallback)


def _save_runtime_json(path: str, payload: dict) -> bool:
    store = _get_runtime_store()
    if not store:
        return False
    return store.save_runtime_json(path, payload)


def get_admin_prompt_defaults() -> dict[str, str]:
    return dict(ADMIN_PROMPT_DEFAULTS)


def _runtime_prompt(key: str, default: str) -> str:
    return get_admin_prompt(key, default)


def _runtime_question_briefs_model(default: str) -> str:
    value = get_admin_runtime_value("questionBriefsModel", default)
    text = _as_str(value, "").strip()
    return text or default


def _runtime_question_briefs_prompt_version() -> str:
    value = get_admin_runtime_value("questionBriefsPromptVersion", getattr(config, "QUESTION_BRIEFS_PROMPT_VERSION", "qcards-v1"))
    text = _as_str(value, "").strip()
    return text or _as_str(getattr(config, "QUESTION_BRIEFS_PROMPT_VERSION", "qcards-v1"))


def _runtime_question_briefs_feature_enabled() -> bool:
    value = get_admin_runtime_value("featureQuestionBriefsAi", config.FEATURE_QUESTION_BRIEFS_AI)
    if isinstance(value, bool):
        return value
    return bool(config.FEATURE_QUESTION_BRIEFS_AI)


def _slugify(value: str) -> str:
    text = _as_str(value, "").lower()
    out = []
    last_dash = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            last_dash = False
        elif not last_dash:
            out.append("-")
            last_dash = True
    slug = "".join(out).strip("-")
    return slug or "question"


def _trim_text(value: Any, limit: int) -> str:
    text = " ".join(_as_str(value, "").split())
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


def _parse_ts(value: str) -> datetime:
    raw = _as_str(value, "")
    if not raw:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _tokenize(value: str) -> list[str]:
    return [t.lower() for t in _TOKEN_RE.findall(value)]


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a.intersection(b))
    union = len(a.union(b))
    if union == 0:
        return 0.0
    return inter / union


def _extract_question(text: str) -> str:
    normalized = " ".join(_as_str(text, "").split())
    if "?" not in normalized:
        return ""
    parts = [p.strip() for p in normalized.split("?") if p.strip()]
    candidates = [f"{p}?" for p in parts if len(p.strip()) >= 12]
    if not candidates:
        return ""

    def score(candidate: str) -> tuple[int, int]:
        low = candidate.lower()
        hint = 1 if any(f" {h} " in f" {low} " for h in _INTERROGATIVE_HINTS) else 0
        return (hint, len(candidate))

    candidates.sort(key=score, reverse=True)
    return candidates[0]


def _is_noise_question(question: str, parent_text: str) -> bool:
    q = _as_str(question, "").strip()
    if len(q) < 14:
        return True

    low = q.lower()
    if low.count("?") >= 3:
        return True
    if any(marker in low for marker in _NOISE_MARKERS):
        return True

    alpha_chars = sum(ch.isalpha() for ch in q)
    if alpha_chars / max(len(q), 1) < 0.45:
        return True

    has_hint = any(f" {h} " in f" {low} " for h in _INTERROGATIVE_HINTS)
    if not has_hint and len(q) < 28 and not _as_str(parent_text, "").strip():
        return True

    return False


def _trend_pct(current: int, previous: int) -> int:
    support = max(0, current) + max(0, previous)
    if support <= 0:
        return 0
    baseline = max(1, previous + 3)
    return int(round(_clamp(100.0 * (current - previous) / baseline, -100.0, 100.0)))


def _confidence_label(score: float) -> str:
    if score >= 0.78:
        return "high"
    if score >= max(0.5, float(config.QUESTION_BRIEFS_MIN_CONFIDENCE)):
        return "medium"
    return "low"


def _cluster_fingerprint(cluster: dict) -> str:
    payload = {
        "topic": _as_str(cluster.get("topic")),
        "category": _as_str(cluster.get("category")),
        "messages": _as_int(cluster.get("messages"), 0),
        "uniqueUsers": _as_int(cluster.get("uniqueUsers"), 0),
        "channels": _as_int(cluster.get("channels"), 0),
        "trend7dPct": _as_int(cluster.get("trend7dPct"), 0),
        "latestAt": _as_str(cluster.get("latestAt")),
        "promptVersion": _runtime_question_briefs_prompt_version(),
        "triageModel": _runtime_question_briefs_model(_as_str(config.QUESTION_BRIEFS_TRIAGE_MODEL)),
        "synthesisModel": _runtime_question_briefs_model(_as_str(config.QUESTION_BRIEFS_SYNTHESIS_MODEL)),
        "signals": [
            {
                "id": _as_str(s.get("id")),
                "message": _trim_text(s.get("message"), 220),
                "context": _trim_text(s.get("context"), 140),
                "channel": _as_str(s.get("channel")),
                "kind": _as_str(s.get("kind")),
                "timestamp": _as_str(s.get("timestamp")),
            }
            for s in (cluster.get("signals") or [])[: int(config.QUESTION_BRIEFS_EVIDENCE_PER_TOPIC)]
        ],
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_state() -> dict:
    global _state_cache
    if isinstance(_state_cache, dict) and _state_cache.get("schemaVersion") == _SCHEMA_VERSION:
        return _state_cache

    state = _read_latest_runtime_json(
        _STATE_FOLDER,
        default={
            "schemaVersion": _SCHEMA_VERSION,
            "updatedAt": None,
            "clusters": {},
        },
    )
    if not isinstance(state, dict):
        state = {"schemaVersion": _SCHEMA_VERSION, "updatedAt": None, "clusters": {}}
    if not isinstance(state.get("clusters"), dict):
        state["clusters"] = {}
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
    logger.error("Question cards state persistence failed verification")
    return False


def _load_snapshot_cards(*, diagnostics: dict | None = None, stage: str = "loadedCards") -> list[dict]:
    snapshot = _read_latest_runtime_json(_SNAPSHOT_FOLDER, default={"cards": []})
    cards = snapshot.get("cards") if isinstance(snapshot, dict) else []
    parsed = cards if isinstance(cards, list) else []
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("snapshot", {})[stage] = len(parsed)
    return parsed


def _save_snapshot_cards(cards: list[dict], metadata: dict | None = None, diagnostics: dict | None = None) -> bool:
    payload = {
        "generatedAt": _now_iso(),
        "source": "materialized",
        "cards": cards,
    }
    if isinstance(metadata, dict) and metadata:
        payload["meta"] = metadata
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("snapshot", {})["writeAttempted"] = True
    saved = _write_versioned_runtime_json(_SNAPSHOT_FOLDER, payload)
    readback_cards = _load_snapshot_cards(diagnostics=diagnostics, stage="readbackCards") if saved else []
    readback_ok = len(readback_cards) == len(cards)
    if saved and not readback_ok:
        logger.error(
            "Question cards snapshot write verified key but latest snapshot readback mismatched | expected_cards={} readback_cards={}",
            len(cards),
            len(readback_cards),
        )
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("snapshot", {})["writeSucceeded"] = bool(saved and readback_ok)
    return bool(saved and readback_ok)


def _read_latest_runtime_json(folder: str, default: dict | None = None) -> dict:
    fallback = default if isinstance(default, dict) else {}
    store = _get_runtime_store()
    if not store:
        return dict(fallback)

    rows = store.list_runtime_files(folder)
    if not rows:
        return dict(fallback)

    def key(row: dict) -> tuple[str, str]:
        return (_as_str(row.get("updated_at"), ""), _as_str(row.get("name"), ""))

    json_rows = [row for row in rows if _as_str(row.get("name"), "").endswith(".json")]
    if not json_rows:
        return dict(fallback)

    latest = sorted(json_rows, key=key, reverse=True)[0]
    name = _as_str(latest.get("name"), "")
    if not name:
        return dict(fallback)
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


def _build_signals(candidates: list[dict], diagnostics: dict | None = None) -> list[dict]:
    signals: list[dict] = []
    for row in candidates[: config.QUESTION_BRIEFS_MAX_TOPICS]:
        topic = _as_str(row.get("topic"), "").strip()
        if not topic:
            continue
        category = _as_str(row.get("category"), "General")
        for ev in (row.get("evidence") or [])[: config.QUESTION_BRIEFS_EVIDENCE_PER_TOPIC]:
            evidence_id = _as_str(ev.get("id"), "").strip()
            if not evidence_id:
                continue
            text = _as_str(ev.get("text"), "")
            parent_text = _as_str(ev.get("parentText"), "")
            question = _extract_question(text)
            if not question:
                continue
            if _is_noise_question(question, parent_text):
                continue

            signals.append(
                {
                    "topic": topic,
                    "category": category,
                    "id": evidence_id,
                    "kind": _as_str(ev.get("kind"), "message"),
                    "channel": _as_str(ev.get("channel"), "unknown"),
                    "userId": _as_str(ev.get("userId"), ""),
                    "timestamp": _as_str(ev.get("timestamp"), ""),
                    "ts": _parse_ts(_as_str(ev.get("timestamp"), "")),
                    "message": _trim_text(text, max(420, int(config.QUESTION_BRIEFS_MESSAGE_CHAR_LIMIT))),
                    "context": _trim_text(parent_text, max(220, int(config.QUESTION_BRIEFS_CONTEXT_CHAR_LIMIT))),
                    "question": _trim_text(question, 220),
                    "tokens": set(_tokenize(question)),
                }
            )
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["signalCount"] = len(signals)
    return signals


def _topic_seed_question(signals: list[dict]) -> str:
    if not signals:
        return ""

    def score(signal: dict) -> tuple[int, int, int]:
        question = _as_str(signal.get("question"), "")
        low = question.lower()
        hint = 1 if any(f" {h} " in f" {low} " for h in _INTERROGATIVE_HINTS) else 0
        return (hint, len(question), int(signal.get("ts", datetime.now(timezone.utc)).timestamp()))

    best = sorted(signals, key=score, reverse=True)[0]
    return _as_str(best.get("question"), "")


def _support_gate(cluster: dict) -> bool:
    messages = _as_int(cluster.get("messages"), 0)
    unique_users = _as_int(cluster.get("uniqueUsers"), 0)
    channels = _as_int(cluster.get("channels"), 0)
    trend = _as_int(cluster.get("trend7dPct"), 0)

    if (
        messages >= int(config.QUESTION_BRIEFS_MIN_CLUSTER_MESSAGES)
        and unique_users >= int(config.QUESTION_BRIEFS_MIN_CLUSTER_USERS)
        and channels >= int(config.QUESTION_BRIEFS_MIN_CLUSTER_CHANNELS)
    ):
        return True

    if (
        messages >= max(5, int(config.QUESTION_BRIEFS_MIN_CLUSTER_MESSAGES) - 2)
        and unique_users >= max(3, int(config.QUESTION_BRIEFS_MIN_CLUSTER_USERS) - 1)
        and channels >= int(config.QUESTION_BRIEFS_MIN_CLUSTER_CHANNELS)
        and trend >= 40
    ):
        return True

    return False


def _build_clusters(candidates: list[dict], diagnostics: dict | None = None) -> list[dict]:
    signals = _build_signals(candidates, diagnostics=diagnostics)
    by_topic: dict[str, list[dict]] = {}
    topic_meta: dict[str, dict] = {}

    for row in candidates:
        topic = _as_str(row.get("topic"), "").strip()
        if topic:
            topic_meta[topic.lower()] = {
                "topic": topic,
                "category": _as_str(row.get("category"), "General"),
            }

    for signal in signals:
        key = _as_str(signal.get("topic"), "").lower()
        if not key:
            continue
        by_topic.setdefault(key, []).append(signal)

    clusters: list[dict] = []
    now = datetime.now(timezone.utc)
    for topic_key, topic_signals in by_topic.items():
        meta = topic_meta.get(topic_key, {})
        topic = _as_str(meta.get("topic"), topic_signals[0].get("topic", "Topic"))
        category = _as_str(meta.get("category"), topic_signals[0].get("category", "General"))
        signals = sorted(topic_signals, key=lambda s: s.get("ts", datetime.now(timezone.utc)), reverse=True)
        channels = {s["channel"] for s in signals if _as_str(s.get("channel"), "").strip()}
        askers = {
            _as_str(s.get("userId"), "").strip() or f"channel:{_as_str(s.get('channel'), 'unknown').strip().lower()}"
            for s in signals
        }
        askers = {a for a in askers if a}
        sig7d = sum(1 for s in signals if (now - s["ts"]).days < 7)
        sig_prev7d = sum(1 for s in signals if 7 <= (now - s["ts"]).days < 14)

        clusters.append(
            {
                "clusterId": f"qc-{_slugify(topic)}",
                "topic": topic,
                "category": category,
                "messages": len(signals),
                "uniqueUsers": len(askers),
                "channels": len(channels),
                "signals7d": sig7d,
                "signalsPrev7d": sig_prev7d,
                "trend7dPct": _trend_pct(sig7d, sig_prev7d),
                "latestAt": _as_str(signals[0].get("timestamp"), "") if signals else "",
                "signals": signals,
                "seedQuestion": _topic_seed_question(signals),
            }
        )

    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["clustersBeforeGate"] = len(clusters)
    clusters = [c for c in clusters if _support_gate(c)]
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["clustersAfterGate"] = len(clusters)
    clusters.sort(key=lambda c: (c["messages"], c["uniqueUsers"], c["channels"]), reverse=True)
    return clusters[: int(config.QUESTION_BRIEFS_MAX_CLUSTER_CANDIDATES)]


def _chat_json(*, model: str, max_tokens: int, system_prompt: str, user_payload: dict) -> dict:
    if not _client:
        return {}
    request_started_at = time.perf_counter()
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
    log_openai_usage(
        feature="question_briefs",
        model=model,
        response=response,
        started_at=request_started_at,
        extra={"max_completion_tokens": max_tokens},
    )
    raw = _as_str(response.choices[0].message.content)
    return json.loads(raw) if raw else {}


def _triage_clusters(clusters: list[dict], diagnostics: dict | None = None) -> dict[str, dict]:
    if not clusters:
        return {}

    min_accept = max(1, min(int(getattr(config, "QUESTION_BRIEFS_MIN_ACCEPTED_CLUSTERS", 6)), len(clusters)))
    max_accept = max(min_accept, min(len(clusters), int(config.QUESTION_BRIEFS_MAX_BRIEFS) * 2))

    def deterministic() -> dict[str, dict]:
        ranked = sorted(
            clusters,
            key=lambda c: (
                _as_int(c.get("messages"), 0),
                _as_int(c.get("uniqueUsers"), 0),
                _as_int(c.get("channels"), 0),
                _as_int(c.get("trend7dPct"), 0),
            ),
            reverse=True,
        )
        accepted_ids = {
            _as_str(c.get("clusterId"), "")
            for c in ranked[:max_accept]
            if _as_str(c.get("clusterId"), "")
        }

        out: dict[str, dict] = {}
        for c in clusters:
            cid = _as_str(c.get("clusterId"), "")
            if not cid:
                continue
            evidence_ids = [_as_str(s.get("id")) for s in c.get("signals", [])[:3] if _as_str(s.get("id"))]
            if cid in accepted_ids:
                out[cid] = {
                    "status": "accepted",
                    "confidence": "medium",
                    "evidenceIds": evidence_ids,
                }
            else:
                out[cid] = {
                    "status": "rejected",
                    "confidence": "low",
                    "evidenceIds": evidence_ids[:1],
                    "rejectionReason": "low_signal",
                }
        if isinstance(diagnostics, dict):
            accepted = sum(1 for row in out.values() if _as_str(row.get("status")) == "accepted")
            diagnostics.setdefault("stages", {})["acceptedClusters"] = accepted
            rejected = diagnostics.setdefault("rejections", {}).setdefault("triage", {})
            for row in out.values():
                if _as_str(row.get("status")) == "rejected":
                    _increment_bucket(rejected, _as_str(row.get("rejectionReason"), "low_signal"))
        return out

    if not _client or not _runtime_question_briefs_feature_enabled() or not config.QUESTION_BRIEFS_USE_AI_TRIAGE:
        return deterministic()

    payload = {
        "clusters": [
            {
                "clusterId": c["clusterId"],
                "topic": c["topic"],
                "category": c["category"],
                "messages": c["messages"],
                "uniqueUsers": c["uniqueUsers"],
                "channels": c["channels"],
                "trend7dPct": c["trend7dPct"],
                "evidence": [
                    {
                        "id": s["id"],
                        "message": _trim_text(s["message"], 160),
                        "context": _trim_text(s["context"], 110),
                    }
                    for s in c.get("signals", [])[:4]
                ],
            }
            for c in clusters
        ]
    }

    system_prompt = _runtime_prompt("question_briefs.triage_prompt", QUESTION_BRIEFS_TRIAGE_PROMPT)

    try:
        parsed = _chat_json(
            model=_runtime_question_briefs_model(_as_str(config.QUESTION_BRIEFS_TRIAGE_MODEL)),
            max_tokens=int(config.QUESTION_BRIEFS_TRIAGE_MAX_TOKENS),
            system_prompt=system_prompt,
            user_payload=payload,
        )
        rows = parsed.get("decisions") if isinstance(parsed, dict) else None
        out: dict[str, dict] = {}
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                cid = _as_str(row.get("clusterId"), "").strip()
                if not cid:
                    continue
                status = _as_str(row.get("status"), "rejected").strip().lower()
                confidence = _as_str(row.get("confidence"), "low").strip().lower()
                evidence_ids = []
                for ev_id in row.get("evidenceIds") or []:
                    sid = _as_str(ev_id, "").strip()
                    if sid and sid not in evidence_ids:
                        evidence_ids.append(sid)
                out[cid] = {
                    "status": status if status in {"accepted", "rejected"} else "rejected",
                    "confidence": confidence if confidence in {"high", "medium", "low"} else "low",
                    "evidenceIds": evidence_ids,
                    "rejectionReason": _as_str(row.get("rejectionReason"), ""),
                }
        out = out or deterministic()

        accepted_count = sum(1 for v in out.values() if _as_str(v.get("status")) == "accepted")
        if accepted_count < min_accept:
            for c in clusters:
                cid = _as_str(c.get("clusterId"), "")
                if not cid:
                    continue
                existing = out.get(cid)
                if existing and _as_str(existing.get("status")) == "accepted":
                    continue
                evidence_ids = [_as_str(s.get("id")) for s in c.get("signals", [])[:3] if _as_str(s.get("id"))]
                out[cid] = {
                    "status": "accepted",
                    "confidence": "medium",
                    "evidenceIds": evidence_ids,
                    "rejectionReason": "",
                }
                accepted_count += 1
                if accepted_count >= min_accept:
                    break

        if isinstance(diagnostics, dict):
            accepted = sum(1 for row in out.values() if _as_str(row.get("status")) == "accepted")
            diagnostics.setdefault("stages", {})["acceptedClusters"] = accepted
            rejected = diagnostics.setdefault("rejections", {}).setdefault("triage", {})
            for row in out.values():
                if _as_str(row.get("status")) == "rejected":
                    _increment_bucket(rejected, _as_str(row.get("rejectionReason"), "rejected"))
        return out
    except Exception as e:
        logger.warning(f"Question cards triage failed: {e}")
        return deterministic()


def _synthesize_cards(clusters: list[dict], triage: dict[str, dict], diagnostics: dict | None = None) -> list[dict]:
    accepted = []
    for c in clusters:
        decision = triage.get(_as_str(c.get("clusterId"), ""), {})
        if _as_str(decision.get("status"), "") != "accepted":
            continue
        accepted.append((c, decision))
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["synthesisEligibleClusters"] = len(accepted)

    if not accepted:
        return []

    max_cards = int(config.QUESTION_BRIEFS_MAX_BRIEFS)
    accepted = accepted[: max_cards * 2]

    if not _client or not _runtime_question_briefs_feature_enabled():
        # Deterministic fallback with seed questions only.
        fallback: list[dict] = []
        for c, decision in accepted[:max_cards]:
            q = _trim_text(c.get("seedQuestion"), 180)
            if not q.endswith("?"):
                q = f"{q.rstrip('.! ')}?"
            evidence = c.get("signals", [])[:2]
            fallback.append(
                {
                    "clusterId": c["clusterId"],
                    "canonicalQuestionEn": q,
                    "canonicalQuestionRu": q,
                    "summaryEn": "Combined societal ask synthesized from clustered evidence.",
                    "summaryRu": "Сводный общественный запрос, сформированный из кластера доказательств.",
                    "confidence": "medium",
                    "status": "partially_answered",
                    "resolvedPct": 45,
                    "evidenceIds": [_as_str(ev.get("id")) for ev in evidence if _as_str(ev.get("id"))],
                }
            )
        if isinstance(diagnostics, dict):
            diagnostics.setdefault("stages", {})["synthesizedRows"] = len(fallback)
        return fallback

    payload_clusters = []
    for c, decision in accepted:
        allowed_ids = set(_as_str(i) for i in (decision.get("evidenceIds") or []))
        selected = []
        for signal in c.get("signals", []):
            sid = _as_str(signal.get("id"))
            if not sid:
                continue
            if allowed_ids and sid not in allowed_ids:
                continue
            selected.append(
                {
                    "id": sid,
                    "kind": _as_str(signal.get("kind"), "message"),
                    "channel": _as_str(signal.get("channel"), "unknown"),
                    "timestamp": _as_str(signal.get("timestamp"), ""),
                    "message": _trim_text(signal.get("message"), int(config.QUESTION_BRIEFS_MESSAGE_CHAR_LIMIT)),
                    "context": _trim_text(signal.get("context"), int(config.QUESTION_BRIEFS_CONTEXT_CHAR_LIMIT)),
                }
            )
            if len(selected) >= int(config.QUESTION_BRIEFS_SYNTH_EVIDENCE_LIMIT):
                break

        if len(selected) < 2:
            continue

        payload_clusters.append(
            {
                "clusterId": c["clusterId"],
                "topic": c["topic"],
                "category": c["category"],
                "messages": c["messages"],
                "uniqueUsers": c["uniqueUsers"],
                "channels": c["channels"],
                "trend7dPct": c["trend7dPct"],
                "latestAt": c["latestAt"],
                "evidence": selected,
            }
        )

    if not payload_clusters:
        if isinstance(diagnostics, dict):
            diagnostics.setdefault("stages", {})["synthesizedRows"] = 0
        return []

    system_prompt = _runtime_prompt("question_briefs.synthesis_prompt", QUESTION_BRIEFS_SYNTHESIS_PROMPT)

    rows: list[dict] = []
    for cluster_payload in payload_clusters[:max_cards]:
        try:
            parsed = _chat_json(
                model=_runtime_question_briefs_model(_as_str(config.QUESTION_BRIEFS_SYNTHESIS_MODEL)),
                max_tokens=int(config.QUESTION_BRIEFS_SYNTHESIS_MAX_TOKENS),
                system_prompt=system_prompt,
                user_payload={"cluster": cluster_payload},
            )
            card = parsed.get("card") if isinstance(parsed, dict) else None
            if isinstance(card, dict):
                rows.append(card)
        except Exception as e:
            logger.warning(f"Question cards synthesis failed for {cluster_payload.get('clusterId')}: {e}")

    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["synthesizedRows"] = len(rows)
    return rows


def _validate_question(text: str) -> bool:
    value = _as_str(text, "").strip()
    if len(value) < 12:
        return False
    if not value.endswith("?"):
        return False
    lowered = value.lower()
    if lowered.startswith("i ") or lowered.startswith("я "):
        return False
    return True


def _force_question_form(text: str) -> str:
    value = _as_str(text, "").strip()
    if not value:
        return ""
    if value.endswith("?"):
        return value
    low = value.lower()
    if any(f" {h} " in f" {low} " for h in _INTERROGATIVE_HINTS):
        return f"{value.rstrip('.! ')}?"
    return value


def _materialize_cards(clusters: list[dict], ai_rows: list[dict], diagnostics: dict | None = None) -> list[dict]:
    by_id = {_as_str(c.get("clusterId")): c for c in clusters}
    cards: list[dict] = []
    rejection_buckets = diagnostics.setdefault("rejections", {}).setdefault("materialization", {}) if isinstance(diagnostics, dict) else None

    for row in ai_rows:
        if not isinstance(row, dict):
            continue
        cluster_id = _as_str(row.get("clusterId"), "").strip()
        cluster = by_id.get(cluster_id)
        if not cluster:
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "unknown_cluster")
            continue

        q_en = _force_question_form(_trim_text(row.get("canonicalQuestionEn"), 180))
        q_ru = _force_question_form(_trim_text(row.get("canonicalQuestionRu"), 220))
        if not _validate_question(q_en) or not _validate_question(q_ru):
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "invalid_question_form")
            continue

        confidence_score = _clamp(_as_float(row.get("confidenceScore"), 0.0), 0.0, 1.0)
        confidence = _as_str(row.get("confidence"), "").strip().lower() or _confidence_label(confidence_score)
        if confidence not in {"high", "medium", "low"}:
            confidence = _confidence_label(confidence_score)
        if confidence == "low":
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "low_confidence_label")
            continue

        evidence_by_id = {_as_str(s.get("id")): s for s in cluster.get("signals", [])}
        selected_ids: list[str] = []
        for ev_id in row.get("evidenceIds") or []:
            sid = _as_str(ev_id, "").strip()
            if sid and sid in evidence_by_id and sid not in selected_ids:
                selected_ids.append(sid)
            if len(selected_ids) >= 6:
                break

        if len(selected_ids) < 2:
            for sid in evidence_by_id.keys():
                if sid and sid not in selected_ids:
                    selected_ids.append(sid)
                if len(selected_ids) >= 2:
                    break

        if len(selected_ids) < 2:
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "insufficient_evidence")
            continue

        status = _as_str(row.get("status"), "partially_answered").strip().lower()
        if status not in {"needs_guide", "partially_answered", "well_covered"}:
            status = "partially_answered"

        resolved_pct = int(round(_clamp(_as_float(row.get("resolvedPct"), 0.0), 0.0, 100.0)))

        evidence_payload = []
        for sid in selected_ids:
            signal = evidence_by_id.get(sid)
            if not signal:
                continue
            evidence_payload.append(
                {
                    "id": sid,
                    "quote": _trim_text(signal.get("message"), 680),
                    "channel": _as_str(signal.get("channel"), "unknown"),
                    "timestamp": _as_str(signal.get("timestamp"), ""),
                    "kind": _as_str(signal.get("kind"), "message"),
                }
            )

        cards.append(
            {
                "clusterId": cluster_id,
                "id": f"qc-{cluster_id}",
                "topic": cluster["topic"],
                "category": cluster["category"],
                "canonicalQuestionEn": q_en,
                "canonicalQuestionRu": q_ru,
                "summaryEn": _trim_text(row.get("summaryEn"), 240),
                "summaryRu": _trim_text(row.get("summaryRu"), 280),
                "confidence": confidence,
                "confidenceScore": round(confidence_score, 2),
                "status": status,
                "resolvedPct": resolved_pct,
                "demandSignals": {
                    "messages": _as_int(cluster.get("messages"), 0),
                    "uniqueUsers": _as_int(cluster.get("uniqueUsers"), 0),
                    "channels": _as_int(cluster.get("channels"), 0),
                    "trend7dPct": _as_int(cluster.get("trend7dPct"), 0),
                },
                "sampleEvidenceId": selected_ids[0],
                "latestAt": _as_str(cluster.get("latestAt"), ""),
                "evidence": evidence_payload,
            }
        )

    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["cardsBeforeFilter"] = len(cards)
    cards = [
        c
        for c in cards
        if _as_float(c.get("confidenceScore"), 0.0) >= float(config.QUESTION_BRIEFS_MIN_CONFIDENCE)
        and c.get("confidence") in {"high", "medium"}
    ]
    if isinstance(rejection_buckets, dict):
        filtered_out = max(0, diagnostics.get("stages", {}).get("cardsBeforeFilter", 0) - len(cards))
        if filtered_out:
            _increment_bucket(rejection_buckets, "below_confidence_threshold", filtered_out)
    cards.sort(
        key=lambda c: (
            _as_int(((c.get("demandSignals") or {}).get("messages")), 0),
            _as_float(c.get("confidenceScore"), 0.0),
        ),
        reverse=True,
    )
    return cards[: int(config.QUESTION_BRIEFS_MAX_BRIEFS)]


def _cache_valid(now: float) -> bool:
    return (now - _cache_ts) < max(300, int(config.QUESTION_BRIEFS_CACHE_TTL_SECONDS))


def invalidate_question_briefs_cache() -> None:
    """Clear in-process cache (persistent snapshot remains intact)."""
    global _cached_cards, _cache_ts
    with _cache_lock:
        _cached_cards = []
        _cache_ts = 0.0


def get_question_briefs_diagnostics() -> dict:
    """Return the last materialization diagnostics snapshot."""
    if not _last_refresh_diagnostics:
        return _new_refresh_diagnostics(force=False)

    diagnostics = copy.deepcopy(_last_refresh_diagnostics)
    diagnostics["firstRejectionBucket"] = _first_bucket(
        diagnostics.get("rejections", {}).get("materialization", {})
    ) or _first_bucket(diagnostics.get("rejections", {}).get("triage", {}))
    return diagnostics


def refresh_question_briefs_with_diagnostics(*, force: bool = False) -> dict:
    """Run refresh and return compact diagnostics for debugging."""
    refresh_question_briefs(force=force)
    diagnostics = get_question_briefs_diagnostics()
    diagnostics["cardsProduced"] = diagnostics.get("stages", {}).get("finalCards", 0)
    return diagnostics


def refresh_question_briefs(*, force: bool = False) -> list[dict]:
    """Materialize question cards and persist snapshot/state for request-time reads."""
    global _cached_cards, _cache_ts
    with _cache_lock:
        last_good_cards = list(_cached_cards)
    diagnostics = _new_refresh_diagnostics(force=force)
    logger.info(
        "Question cards refresh start | force={} ai_enabled={} ai_triage={} min_messages={} min_users={} min_channels={} min_confidence={}".format(
            force,
            diagnostics["runtime"]["featureEnabled"],
            diagnostics["runtime"]["aiTriageEnabled"],
            diagnostics["config"]["minClusterMessages"],
            diagnostics["config"]["minClusterUsers"],
            diagnostics["config"]["minClusterChannels"],
            diagnostics["config"]["minConfidence"],
        )
    )

    lease_ttl = max(300, int(config.QUESTION_BRIEFS_REFRESH_MINUTES) * 60)
    if not force and not _acquire_refresh_lease(lease_ttl):
        logger.info("Question cards materialization skipped: another instance holds active lease")
        diagnostics["exitReason"] = "lease_skipped"
        cards = _load_snapshot_cards(diagnostics=diagnostics)
        diagnostics["stages"]["finalCards"] = len(cards)
        _store_refresh_diagnostics(diagnostics)
        if cards:
            with _cache_lock:
                _cached_cards = cards
                _cache_ts = time.time()
            return cards
        return []

    try:
        candidates = strategic.get_question_brief_candidates(
            days=config.QUESTION_BRIEFS_WINDOW_DAYS,
            limit_topics=config.QUESTION_BRIEFS_MAX_TOPICS,
            evidence_per_topic=config.QUESTION_BRIEFS_EVIDENCE_PER_TOPIC,
        )
    except Exception as e:
        logger.warning(f"Question cards candidate retrieval failed: {e}")
        diagnostics["exitReason"] = "candidate_error"
        diagnostics["error"] = str(e)
        cards = _load_snapshot_cards(diagnostics=diagnostics)
        diagnostics["stages"]["finalCards"] = len(cards)
        _store_refresh_diagnostics(diagnostics)
        if cards:
            with _cache_lock:
                _cached_cards = cards
                _cache_ts = time.time()
            return cards
        return []
    diagnostics["stages"]["candidateRows"] = len(candidates)
    logger.info(f"Question cards candidates fetched | count={len(candidates)}")
    if not candidates:
        diagnostics["exitReason"] = "no_candidates"
        _store_refresh_diagnostics(diagnostics)
        return []

    clusters = _build_clusters(candidates, diagnostics=diagnostics)
    logger.info(
        "Question cards clustering | signals={} clusters_before_gate={} clusters_after_gate={}".format(
            diagnostics["stages"]["signalCount"],
            diagnostics["stages"]["clustersBeforeGate"],
            diagnostics["stages"]["clustersAfterGate"],
        )
    )
    if not clusters:
        diagnostics["exitReason"] = "no_clusters_after_support_gate"
        _store_refresh_diagnostics(diagnostics)
        return []

    state = _load_state()
    cluster_state = state.get("clusters") if isinstance(state.get("clusters"), dict) else {}
    active_ids = {_as_str(c.get("clusterId")) for c in clusters if _as_str(c.get("clusterId"))}

    changed_clusters: list[dict] = []
    fingerprints: dict[str, str] = {}

    for cluster in clusters:
        cid = _as_str(cluster.get("clusterId"), "")
        if not cid:
            continue
        fingerprint = _cluster_fingerprint(cluster)
        fingerprints[cid] = fingerprint

        record = cluster_state.get(cid) if isinstance(cluster_state, dict) else None
        if (not force) and isinstance(record, dict) and _as_str(record.get("fingerprint"), "") == fingerprint:
            continue

        changed_clusters.append(cluster)
    diagnostics["stages"]["changedClusters"] = len(changed_clusters)

    triage: dict[str, dict] = {}
    ai_rows: list[dict] = []
    new_cards: list[dict] = []
    if changed_clusters:
        triage = _triage_clusters(changed_clusters, diagnostics=diagnostics)
        ai_rows = _synthesize_cards(changed_clusters, triage, diagnostics=diagnostics)
        new_cards = _materialize_cards(changed_clusters, ai_rows, diagnostics=diagnostics)
    else:
        diagnostics["stages"]["acceptedClusters"] = 0
        diagnostics["stages"]["synthesisEligibleClusters"] = 0
        diagnostics["stages"]["synthesizedRows"] = 0
        diagnostics["stages"]["cardsBeforeFilter"] = 0

    cards_by_cluster = {
        _as_str(card.get("clusterId"), ""): card
        for card in new_cards
        if isinstance(card, dict) and _as_str(card.get("clusterId"), "")
    }

    next_cluster_state: dict[str, dict] = {}

    # Keep unchanged cluster records.
    if isinstance(cluster_state, dict):
        for cid, record in cluster_state.items():
            if cid not in active_ids:
                continue
            if not isinstance(record, dict):
                continue
            if force:
                continue
            expected = fingerprints.get(cid)
            if not expected:
                continue
            if _as_str(record.get("fingerprint"), "") != expected:
                continue
            next_cluster_state[cid] = record

    # Update changed cluster records.
    for cluster in changed_clusters:
        cid = _as_str(cluster.get("clusterId"), "")
        if not cid:
            continue
        fingerprint = fingerprints.get(cid, "")
        card = cards_by_cluster.get(cid)
        if card:
            next_cluster_state[cid] = {
                "fingerprint": fingerprint,
                "status": "accepted",
                "updatedAt": _now_iso(),
                "card": card,
                "topic": _as_str(cluster.get("topic"), ""),
            }
            continue

        decision = triage.get(cid, {})
        next_cluster_state[cid] = {
            "fingerprint": fingerprint,
            "status": "rejected",
            "updatedAt": _now_iso(),
            "rejectionReason": _as_str(decision.get("rejectionReason"), "insufficient_grounding"),
            "topic": _as_str(cluster.get("topic"), ""),
        }

    # Build final cards list from state for active clusters only.
    final_cards: list[dict] = []
    for cluster in clusters:
        cid = _as_str(cluster.get("clusterId"), "")
        record = next_cluster_state.get(cid)
        if not isinstance(record, dict):
            continue
        if _as_str(record.get("status"), "") != "accepted":
            continue
        card = record.get("card")
        if isinstance(card, dict):
            final_cards.append(card)

    final_cards.sort(
        key=lambda c: (
            _as_int(((c.get("demandSignals") or {}).get("messages")), 0),
            _as_float(c.get("confidenceScore"), 0.0),
        ),
        reverse=True,
    )
    final_cards = final_cards[: int(config.QUESTION_BRIEFS_MAX_BRIEFS)]
    diagnostics["stages"]["finalCards"] = len(final_cards)
    diagnostics["stages"]["reusedClusters"] = len(clusters) - len(changed_clusters)

    state["clusters"] = next_cluster_state
    state_saved = _save_state(state)
    snapshot_saved = False
    if state_saved:
        snapshot_saved = _save_snapshot_cards(
        final_cards,
        metadata={
            "activeClusters": len(clusters),
            "changedClusters": len(changed_clusters),
            "reusedClusters": len(clusters) - len(changed_clusters),
            "cards": len(final_cards),
        },
        diagnostics=diagnostics,
    )
    if not state_saved:
        diagnostics["error"] = "Question cards state could not be persisted and verified"
    elif not snapshot_saved:
        diagnostics["error"] = "Question cards snapshot could not be persisted and verified"

    if state_saved and snapshot_saved:
        with _cache_lock:
            _cached_cards = final_cards
            _cache_ts = time.time()
        diagnostics["exitReason"] = diagnostics["exitReason"] or ("ok" if final_cards else "zero_cards_after_materialization")
        result_cards = final_cards
    else:
        diagnostics["exitReason"] = "persistence_verification_failed"
        result_cards = last_good_cards
        if last_good_cards:
            logger.warning(
                "Question cards refresh kept last known good in-memory cache after persistence verification failed | cached_cards={}",
                len(last_good_cards),
            )
        else:
            logger.warning("Question cards refresh produced data but did not replace cache because persistence verification failed")

    _store_refresh_diagnostics(diagnostics)
    logger.info(
        "Question cards materialized | cards={} active_clusters={} changed_clusters={} reused_clusters={} accepted_clusters={} synthesized_rows={} first_rejection_bucket={}".format(
            len(result_cards),
            len(clusters),
            len(changed_clusters),
            len(clusters) - len(changed_clusters),
            diagnostics["stages"]["acceptedClusters"],
            diagnostics["stages"]["synthesizedRows"],
            _first_bucket(diagnostics["rejections"]["materialization"]) or _first_bucket(diagnostics["rejections"]["triage"]) or "none",
        )
    )
    return result_cards


def get_question_briefs(*, force_refresh: bool = False) -> list[dict]:
    """Read materialized question cards (no live LLM in request path)."""
    global _cached_cards, _cache_ts

    if force_refresh:
        return refresh_question_briefs(force=True)

    now = time.time()
    with _cache_lock:
        if _cached_cards and _cache_valid(now):
            return list(_cached_cards)

    cards = _load_snapshot_cards()
    with _cache_lock:
        if cards:
            _cached_cards = cards
            _cache_ts = now
            return list(_cached_cards)
    return []
