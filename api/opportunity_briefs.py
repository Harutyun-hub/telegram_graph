"""AI-generated business opportunity cards for the dashboard widget."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

import config
from api.admin_runtime import get_admin_prompt, get_admin_runtime_value
from api.queries import actionable
from buffer.supabase_writer import SupabaseWriter

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

_SNAPSHOT_FOLDER = "opportunity_cards/snapshots"
_STATE_FOLDER = "opportunity_cards/state"
_LOCK_FOLDER = "opportunity_cards/locks"
_SCHEMA_VERSION = 1
_INSTANCE_ID = f"{os.getpid()}-{int(time.time())}"

_client = OpenAI(api_key=config.OPENAI_API_KEY) if (OpenAI and config.OPENAI_API_KEY) else None

OPPORTUNITY_BRIEFS_TRIAGE_PROMPT = """
You triage candidate business opportunities grounded in community demand.

Rules:
1) Use only provided evidence text and IDs.
2) Accept only if the cluster shows repeated unmet need, market-gap language, or recurring demand that a business could serve.
3) Reject generic discussion, news chatter, hiring/investment-only talk, and weak single-user anecdotes.
4) Return JSON only.

Return schema:
{
  "decisions": [
    {
      "clusterId": "string",
      "status": "accepted|rejected",
      "confidence": "high|medium|low",
      "evidenceIds": ["id1", "id2"],
      "rejectionReason": "low_signal|not_demand_led|generic_topic_chatter|non_businessable|insufficient_grounding"
    }
  ]
}
""".strip()

OPPORTUNITY_BRIEFS_SYNTHESIS_PROMPT = """
You convert grounded demand clusters into business opportunity cards.

Rules:
1) Use only the provided evidence and IDs.
2) opportunityEn/opportunityRu must describe a concrete opportunity, not just a topic.
3) Summary must explain the unmet need and why it may be actionable now.
4) deliveryModel must be one of: service, product, marketplace, content, community_program.
5) readiness must be one of: pilot_ready, validate_now, watchlist.
6) If grounding is weak, return {"card": null}.
7) Do not invent numbers, customer segments, or solution details not supported by evidence.

Return JSON only:
{
  "card": null
  OR
  "card": {
    "clusterId": "string",
    "opportunityEn": "string",
    "opportunityRu": "string",
    "summaryEn": "string",
    "summaryRu": "string",
    "deliveryModel": "service|product|marketplace|content|community_program",
    "readiness": "pilot_ready|validate_now|watchlist",
    "confidence": "high|medium|low",
    "confidenceScore": 0.0,
    "evidenceIds": ["id1", "id2"]
  }
}
""".strip()

ADMIN_PROMPT_DEFAULTS = {
    "opportunity_briefs.triage_prompt": OPPORTUNITY_BRIEFS_TRIAGE_PROMPT,
    "opportunity_briefs.synthesis_prompt": OPPORTUNITY_BRIEFS_SYNTHESIS_PROMPT,
}

_DEMAND_MARKERS = (
    "need help",
    "looking for",
    "where can i",
    "where to find",
    "can anyone recommend",
    "подскажите",
    "помогите",
    "где найти",
    "ищу",
    "нужен",
    "нужна",
    "рекоменд",
)
_GAP_MARKERS = (
    "no good",
    "nothing reliable",
    "hard to find",
    "missing service",
    "market gap",
    "underserved",
    "нет нормаль",
    "нет хорош",
    "нет сервиса",
    "сложно найти",
    "не хватает",
)
_EXCLUDED_MARKERS = (
    "job opening",
    "hiring",
    "vacancy",
    "looking to hire",
    "investor",
    "investment",
    "real estate",
    "buy apartment",
    "ваканси",
    "нанима",
    "инвест",
    "недвиж",
)


def _new_refresh_diagnostics(*, force: bool = False) -> dict:
    return {
        "force": bool(force),
        "exitReason": "",
        "error": "",
        "runtime": {
            "hasOpenAIClient": bool(_client),
            "featureEnabled": bool(_runtime_opportunity_briefs_feature_enabled()),
        },
        "config": {
            "windowDays": int(config.OPPORTUNITY_BRIEFS_WINDOW_DAYS),
            "maxTopics": int(config.OPPORTUNITY_BRIEFS_MAX_TOPICS),
            "maxBriefs": int(config.OPPORTUNITY_BRIEFS_MAX_BRIEFS),
            "minMessages": int(config.OPPORTUNITY_BRIEFS_MIN_MESSAGES),
            "minUsers": int(config.OPPORTUNITY_BRIEFS_MIN_USERS),
            "minConfidence": float(config.OPPORTUNITY_BRIEFS_MIN_CONFIDENCE),
        },
        "stages": {
            "candidateRows": 0,
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
            logger.warning(f"Opportunity cards runtime store unavailable: {e}")
            _runtime_store = None
    return _runtime_store


def _save_runtime_json(path: str, payload: dict) -> bool:
    store = _get_runtime_store()
    if not store:
        return False
    return store.save_runtime_json(path, payload)


def _read_latest_runtime_json(folder: str, default: dict | None = None) -> dict:
    fallback = default if isinstance(default, dict) else {}
    store = _get_runtime_store()
    if not store:
        return dict(fallback)
    rows = store.list_runtime_files(folder)
    json_rows = [row for row in rows if _as_str(row.get("name"), "").endswith(".json")]
    if not json_rows:
        return dict(fallback)
    latest = sorted(
        json_rows,
        key=lambda row: (_as_str(row.get("updated_at"), ""), _as_str(row.get("name"), "")),
        reverse=True,
    )[0]
    name = _as_str(latest.get("name"), "")
    if not name:
        return dict(fallback)
    return store.get_runtime_json(f"{folder}/{name}", default=fallback)


def _prune_runtime_folder(folder: str, keep: int = 12) -> None:
    store = _get_runtime_store()
    if not store:
        return
    rows = store.list_runtime_files(folder)
    json_rows = [row for row in rows if _as_str(row.get("name"), "").endswith(".json")]
    if len(json_rows) <= keep:
        return
    stale = sorted(
        json_rows,
        key=lambda row: (_as_str(row.get("updated_at"), ""), _as_str(row.get("name"), "")),
        reverse=True,
    )[keep:]
    delete_paths = [f"{folder}/{_as_str(row.get('name'), '')}" for row in stale if _as_str(row.get("name"), "")]
    if delete_paths:
        store.delete_runtime_files(delete_paths)


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
    if not _save_runtime_json(key, payload):
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
            return expires_at <= now
        except Exception:
            return True
    return False


def get_admin_prompt_defaults() -> dict[str, str]:
    return dict(ADMIN_PROMPT_DEFAULTS)


def _runtime_prompt(key: str, default: str) -> str:
    return get_admin_prompt(key, default)


def _runtime_opportunity_briefs_model(default: str) -> str:
    value = get_admin_runtime_value("opportunityBriefsModel", default)
    text = _as_str(value, "").strip()
    return text or default


def _runtime_opportunity_briefs_prompt_version() -> str:
    value = get_admin_runtime_value("opportunityBriefsPromptVersion", config.OPPORTUNITY_BRIEFS_PROMPT_VERSION)
    text = _as_str(value, "").strip()
    return text or config.OPPORTUNITY_BRIEFS_PROMPT_VERSION


def _runtime_opportunity_briefs_feature_enabled() -> bool:
    value = get_admin_runtime_value("featureOpportunityBriefsAi", config.FEATURE_OPPORTUNITY_BRIEFS_AI)
    if isinstance(value, bool):
        return value
    return bool(config.FEATURE_OPPORTUNITY_BRIEFS_AI)


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


def _trend_pct(current: int, previous: int) -> int:
    support = max(0, current) + max(0, previous)
    if support <= 0:
        return 0
    baseline = max(1, previous + 3)
    return int(round(_clamp(100.0 * (current - previous) / baseline, -100.0, 100.0)))


def _confidence_label(score: float) -> str:
    if score >= 0.78:
        return "high"
    if score >= max(0.5, float(config.OPPORTUNITY_BRIEFS_MIN_CONFIDENCE)):
        return "medium"
    return "low"


def _readiness_rank(value: str) -> int:
    return {"pilot_ready": 3, "validate_now": 2, "watchlist": 1}.get(value, 0)


def _cluster_fingerprint(cluster: dict) -> str:
    payload = {
        "topic": _as_str(cluster.get("topic")),
        "category": _as_str(cluster.get("category")),
        "messages": _as_int(cluster.get("messages"), 0),
        "uniqueUsers": _as_int(cluster.get("uniqueUsers"), 0),
        "channels": _as_int(cluster.get("channels"), 0),
        "trend7dPct": _as_int(cluster.get("trend7dPct"), 0),
        "latestAt": _as_str(cluster.get("latestAt")),
        "promptVersion": _runtime_opportunity_briefs_prompt_version(),
        "triageModel": _runtime_opportunity_briefs_model(config.OPPORTUNITY_BRIEFS_TRIAGE_MODEL),
        "synthesisModel": _runtime_opportunity_briefs_model(config.OPPORTUNITY_BRIEFS_SYNTHESIS_MODEL),
        "signals": [
            {
                "id": _as_str(s.get("id")),
                "message": _trim_text(s.get("message"), 220),
                "context": _trim_text(s.get("context"), 140),
                "channel": _as_str(s.get("channel")),
                "kind": _as_str(s.get("kind")),
                "timestamp": _as_str(s.get("timestamp")),
            }
            for s in (cluster.get("signals") or [])[: int(config.OPPORTUNITY_BRIEFS_EVIDENCE_PER_TOPIC)]
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
        default={"schemaVersion": _SCHEMA_VERSION, "updatedAt": None, "clusters": {}},
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
    logger.error("Opportunity cards state persistence failed verification")
    return False


def _load_snapshot_cards(*, diagnostics: dict | None = None, stage: str = "loadedCards") -> list[dict]:
    snapshot = _read_latest_runtime_json(_SNAPSHOT_FOLDER, default={"cards": []})
    cards = snapshot.get("cards") if isinstance(snapshot, dict) else []
    parsed = cards if isinstance(cards, list) else []
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("snapshot", {})[stage] = len(parsed)
    return parsed


def _save_snapshot_cards(cards: list[dict], metadata: dict | None = None, diagnostics: dict | None = None) -> bool:
    payload: dict[str, Any] = {
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
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("snapshot", {})["writeSucceeded"] = bool(saved and readback_ok)
    return bool(saved and readback_ok)


def _cache_valid(now: float) -> bool:
    return bool(_cached_cards) and (now - _cache_ts) < int(config.OPPORTUNITY_BRIEFS_CACHE_TTL_SECONDS)


def _is_noise_or_excluded(text: str) -> bool:
    low = text.lower()
    return any(marker in low for marker in _EXCLUDED_MARKERS)


def _is_opportunity_evidence_aligned(message: str, context: str, row: dict) -> bool:
    text = f"{_as_str(message)} {_as_str(context)}".lower()
    if _is_noise_or_excluded(text):
        return False
    if len(text.strip()) < 20:
        return False
    demand_hit = any(marker in text for marker in _DEMAND_MARKERS)
    gap_hit = any(marker in text for marker in _GAP_MARKERS)
    ask_like = _as_int(row.get("askLike"), 0) > 0
    support_intent = _as_int(row.get("supportIntent"), 0) > 0
    opportunity_hint = _as_int(row.get("opportunityHint"), 0) > 0
    if not (demand_hit or gap_hit or ask_like or support_intent or opportunity_hint):
        return False
    if text.count("?") >= 3:
        return False
    return True


def _build_clusters(candidates: list[dict], diagnostics: dict | None = None) -> list[dict]:
    clusters: list[dict] = []
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["clustersBeforeGate"] = len(candidates)
    for row in candidates:
        if not isinstance(row, dict):
            continue
        topic = _as_str(row.get("topic"), "").strip()
        if not topic:
            continue
        evidence_rows = []
        for ev in row.get("evidence") or []:
            if not isinstance(ev, dict):
                continue
            evidence_id = _as_str(ev.get("id"), "").strip()
            message = _trim_text(ev.get("text"), 680)
            if not evidence_id or not message:
                continue
            context = _trim_text(ev.get("parentText"), 380)
            if not _is_opportunity_evidence_aligned(message, context, ev):
                continue
            evidence_rows.append(
                {
                    "id": evidence_id,
                    "kind": _as_str(ev.get("kind"), "message"),
                    "channel": _as_str(ev.get("channel"), "unknown"),
                    "userId": _as_str(ev.get("userId"), ""),
                    "timestamp": _as_str(ev.get("timestamp"), ""),
                    "message": message,
                    "context": context,
                    "askLike": 1 if _as_int(ev.get("askLike"), 0) > 0 else 0,
                    "gapHit": 1 if _as_int(ev.get("gapHit"), 0) > 0 else 0,
                    "supportIntent": 1 if _as_int(ev.get("supportIntent"), 0) > 0 else 0,
                    "recommendationHit": 1 if _as_int(ev.get("recommendationHit"), 0) > 0 else 0,
                    "opportunityHint": 1 if _as_int(ev.get("opportunityHint"), 0) > 0 else 0,
                    "ts": _parse_ts(_as_str(ev.get("timestamp"), "")),
                }
            )
        if len(evidence_rows) < int(config.OPPORTUNITY_BRIEFS_MIN_MESSAGES):
            continue
        users = {
            _as_str(ev.get("userId"), "").strip() or f"channel:{_as_str(ev.get('channel'), 'unknown').strip().lower()}"
            for ev in evidence_rows
        }
        users = {user for user in users if user}
        if len(users) < int(config.OPPORTUNITY_BRIEFS_MIN_USERS):
            continue
        channels = {_as_str(ev.get("channel"), "unknown").strip().lower() for ev in evidence_rows if _as_str(ev.get("channel"), "").strip()}
        clusters.append(
            {
                "clusterId": f"op-{topic.lower().replace(' ', '-').replace('/', '-')}",
                "topic": topic,
                "category": _as_str(row.get("category"), "General"),
                "messages": len(evidence_rows),
                "uniqueUsers": len(users),
                "channels": len(channels),
                "signals7d": _as_int(row.get("signals7d"), sum(1 for ev in evidence_rows if (datetime.now(timezone.utc) - ev["ts"]).days < 7)),
                "signalsPrev7d": _as_int(row.get("signalsPrev7d"), sum(1 for ev in evidence_rows if 7 <= (datetime.now(timezone.utc) - ev["ts"]).days < 14)),
                "trend7dPct": _as_int(row.get("trend7dPct"), _trend_pct(0, 0)),
                "latestAt": _as_str(row.get("latestAt"), evidence_rows[0].get("timestamp", "") if evidence_rows else ""),
                "signals": sorted(evidence_rows, key=lambda ev: ev["ts"], reverse=True),
            }
        )
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["clustersAfterGate"] = len(clusters)
    clusters.sort(
        key=lambda c: (_as_int(c.get("messages"), 0), _as_int(c.get("uniqueUsers"), 0), _as_int(c.get("channels"), 0), _as_int(c.get("trend7dPct"), 0)),
        reverse=True,
    )
    return clusters[: int(config.OPPORTUNITY_BRIEFS_MAX_TOPICS)]


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


def _triage_clusters(clusters: list[dict], diagnostics: dict | None = None) -> dict[str, dict]:
    if not clusters:
        return {}
    if not _client or not _runtime_opportunity_briefs_feature_enabled():
        if isinstance(diagnostics, dict):
            diagnostics.setdefault("stages", {})["acceptedClusters"] = 0
        return {}

    min_accept = max(1, min(int(config.OPPORTUNITY_BRIEFS_MIN_ACCEPTED_CLUSTERS), len(clusters)))
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
                        "message": _trim_text(s["message"], 170),
                        "context": _trim_text(s["context"], 110),
                    }
                    for s in c.get("signals", [])[:4]
                ],
            }
            for c in clusters
        ]
    }
    system_prompt = _runtime_prompt("opportunity_briefs.triage_prompt", OPPORTUNITY_BRIEFS_TRIAGE_PROMPT)
    out: dict[str, dict] = {}
    try:
        parsed = _chat_json(
            model=_runtime_opportunity_briefs_model(config.OPPORTUNITY_BRIEFS_TRIAGE_MODEL),
            max_tokens=int(config.OPPORTUNITY_BRIEFS_TRIAGE_MAX_TOKENS),
            system_prompt=system_prompt,
            user_payload=payload,
        )
        rows = parsed.get("decisions") if isinstance(parsed, dict) else None
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                cluster_id = _as_str(row.get("clusterId"), "").strip()
                if not cluster_id:
                    continue
                evidence_ids = []
                for item in row.get("evidenceIds") or []:
                    evidence_id = _as_str(item, "").strip()
                    if evidence_id and evidence_id not in evidence_ids:
                        evidence_ids.append(evidence_id)
                out[cluster_id] = {
                    "status": _as_str(row.get("status"), "rejected").strip().lower(),
                    "confidence": _as_str(row.get("confidence"), "low").strip().lower(),
                    "evidenceIds": evidence_ids,
                    "rejectionReason": _as_str(row.get("rejectionReason"), ""),
                }
    except Exception as e:
        logger.warning(f"Opportunity cards triage failed: {e}")
        out = {}

    accepted = sum(1 for row in out.values() if _as_str(row.get("status")) == "accepted")
    if accepted < min_accept:
        for cluster in clusters:
            cluster_id = _as_str(cluster.get("clusterId"), "")
            if not cluster_id or _as_str(out.get(cluster_id, {}).get("status")) == "accepted":
                continue
            evidence_ids = [_as_str(signal.get("id")) for signal in cluster.get("signals", [])[:3] if _as_str(signal.get("id"))]
            out[cluster_id] = {
                "status": "accepted",
                "confidence": "medium",
                "evidenceIds": evidence_ids,
                "rejectionReason": "",
            }
            accepted += 1
            if accepted >= min_accept:
                break

    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["acceptedClusters"] = accepted
        rejected = diagnostics.setdefault("rejections", {}).setdefault("triage", {})
        for row in out.values():
            if _as_str(row.get("status")) != "accepted":
                _increment_bucket(rejected, _as_str(row.get("rejectionReason"), "rejected"))
    return out


def _synthesize_cards(clusters: list[dict], triage: dict[str, dict], diagnostics: dict | None = None) -> list[dict]:
    accepted = []
    for cluster in clusters:
        decision = triage.get(_as_str(cluster.get("clusterId"), ""), {})
        if _as_str(decision.get("status")) != "accepted":
            continue
        accepted.append((cluster, decision))
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["synthesisEligibleClusters"] = len(accepted)
    if not accepted or not _client or not _runtime_opportunity_briefs_feature_enabled():
        return []

    system_prompt = _runtime_prompt("opportunity_briefs.synthesis_prompt", OPPORTUNITY_BRIEFS_SYNTHESIS_PROMPT)
    rows: list[dict] = []
    for cluster, decision in accepted[: int(config.OPPORTUNITY_BRIEFS_MAX_BRIEFS)]:
        allowed_ids = set(_as_str(item) for item in (decision.get("evidenceIds") or []))
        selected = []
        for signal in cluster.get("signals", []):
            signal_id = _as_str(signal.get("id"))
            if not signal_id:
                continue
            if allowed_ids and signal_id not in allowed_ids:
                continue
            selected.append(
                {
                    "id": signal_id,
                    "kind": _as_str(signal.get("kind"), "message"),
                    "channel": _as_str(signal.get("channel"), "unknown"),
                    "timestamp": _as_str(signal.get("timestamp"), ""),
                    "message": _trim_text(signal.get("message"), int(config.OPPORTUNITY_BRIEFS_MESSAGE_CHAR_LIMIT)),
                    "context": _trim_text(signal.get("context"), int(config.OPPORTUNITY_BRIEFS_CONTEXT_CHAR_LIMIT)),
                }
            )
            if len(selected) >= int(config.OPPORTUNITY_BRIEFS_SYNTH_EVIDENCE_LIMIT):
                break
        if len(selected) < 2:
            continue
        payload = {
            "cluster": {
                "clusterId": cluster["clusterId"],
                "topic": cluster["topic"],
                "category": cluster["category"],
                "messages": cluster["messages"],
                "uniqueUsers": cluster["uniqueUsers"],
                "channels": cluster["channels"],
                "trend7dPct": cluster["trend7dPct"],
                "latestAt": cluster["latestAt"],
                "evidence": selected,
            }
        }
        try:
            parsed = _chat_json(
                model=_runtime_opportunity_briefs_model(config.OPPORTUNITY_BRIEFS_SYNTHESIS_MODEL),
                max_tokens=int(config.OPPORTUNITY_BRIEFS_SYNTHESIS_MAX_TOKENS),
                system_prompt=system_prompt,
                user_payload=payload,
            )
            card = parsed.get("card") if isinstance(parsed, dict) else None
            if isinstance(card, dict):
                rows.append(card)
        except Exception as e:
            logger.warning(f"Opportunity cards synthesis failed for {cluster.get('clusterId')}: {e}")
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["synthesizedRows"] = len(rows)
    return rows


def _materialize_cards(clusters: list[dict], ai_rows: list[dict], diagnostics: dict | None = None) -> list[dict]:
    by_id = {_as_str(cluster.get("clusterId")): cluster for cluster in clusters}
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

        opportunity_en = _trim_text(row.get("opportunityEn"), 180)
        opportunity_ru = _trim_text(row.get("opportunityRu"), 220)
        summary_en = _trim_text(row.get("summaryEn"), 320)
        summary_ru = _trim_text(row.get("summaryRu"), 360)
        delivery_model = _as_str(row.get("deliveryModel"), "").strip().lower()
        readiness = _as_str(row.get("readiness"), "").strip().lower()
        confidence_score = _clamp(_as_float(row.get("confidenceScore"), 0.0), 0.0, 1.0)
        confidence = _as_str(row.get("confidence"), "").strip().lower() or _confidence_label(confidence_score)

        if not opportunity_en or not opportunity_ru:
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "missing_title")
            continue
        if delivery_model not in {"service", "product", "marketplace", "content", "community_program"}:
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "invalid_delivery_model")
            continue
        if readiness not in {"pilot_ready", "validate_now", "watchlist"}:
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "invalid_readiness")
            continue
        if confidence not in {"high", "medium", "low"}:
            confidence = _confidence_label(confidence_score)
        if confidence == "low":
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "low_confidence_label")
            continue
        if confidence_score < float(config.OPPORTUNITY_BRIEFS_MIN_CONFIDENCE):
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "low_confidence_score")
            continue

        evidence_by_id = {_as_str(signal.get("id")): signal for signal in cluster.get("signals", [])}
        selected_ids: list[str] = []
        for item in row.get("evidenceIds") or []:
            evidence_id = _as_str(item, "").strip()
            if evidence_id and evidence_id in evidence_by_id and evidence_id not in selected_ids:
                selected_ids.append(evidence_id)
            if len(selected_ids) >= 6:
                break
        if len(selected_ids) < 2:
            for evidence_id in evidence_by_id.keys():
                if evidence_id not in selected_ids:
                    selected_ids.append(evidence_id)
                if len(selected_ids) >= 2:
                    break
        if len(selected_ids) < 2:
            if isinstance(rejection_buckets, dict):
                _increment_bucket(rejection_buckets, "insufficient_evidence")
            continue

        evidence = [
            {
                "id": evidence_id,
                "quote": _trim_text(evidence_by_id[evidence_id].get("message"), 500),
                "channel": _as_str(evidence_by_id[evidence_id].get("channel"), "unknown"),
                "timestamp": _as_str(evidence_by_id[evidence_id].get("timestamp"), ""),
                "kind": _as_str(evidence_by_id[evidence_id].get("kind"), "message"),
            }
            for evidence_id in selected_ids
            if evidence_id in evidence_by_id
        ]
        cards.append(
            {
                "id": cluster_id,
                "clusterId": cluster_id,
                "topic": _as_str(cluster.get("topic"), ""),
                "category": _as_str(cluster.get("category"), "General"),
                "opportunityEn": opportunity_en,
                "opportunityRu": opportunity_ru,
                "summaryEn": summary_en,
                "summaryRu": summary_ru,
                "deliveryModel": delivery_model,
                "readiness": readiness,
                "confidence": confidence,
                "confidenceScore": confidence_score,
                "demandSignals": {
                    "messages": _as_int(cluster.get("messages"), 0),
                    "uniqueUsers": _as_int(cluster.get("uniqueUsers"), 0),
                    "channels": _as_int(cluster.get("channels"), 0),
                    "trend7dPct": _as_int(cluster.get("trend7dPct"), 0),
                },
                "sampleEvidenceId": selected_ids[0],
                "latestAt": _as_str(cluster.get("latestAt"), ""),
                "evidence": evidence,
            }
        )
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("stages", {})["cardsBeforeFilter"] = len(cards)
    return cards


def invalidate_opportunity_briefs_cache() -> None:
    global _cached_cards, _cache_ts, _state_cache, _last_refresh_diagnostics
    with _cache_lock:
        _cached_cards = []
        _cache_ts = 0.0
    _state_cache = {}
    _last_refresh_diagnostics = {}


def get_opportunity_briefs_diagnostics() -> dict:
    diagnostics = copy.deepcopy(_last_refresh_diagnostics) if isinstance(_last_refresh_diagnostics, dict) else {}
    diagnostics["cardsProduced"] = diagnostics.get("stages", {}).get("finalCards", 0)
    diagnostics["firstRejectionBucket"] = _first_bucket(diagnostics.get("rejections", {}).get("materialization", {})) or _first_bucket(diagnostics.get("rejections", {}).get("triage", {}))
    return diagnostics


def refresh_opportunity_briefs_with_diagnostics(*, force: bool = False) -> dict:
    refresh_opportunity_briefs(force=force)
    return get_opportunity_briefs_diagnostics()


def refresh_opportunity_briefs(*, force: bool = False) -> list[dict]:
    global _cached_cards, _cache_ts
    with _cache_lock:
        last_good_cards = list(_cached_cards)

    diagnostics = _new_refresh_diagnostics(force=force)
    lease_ttl = max(300, int(config.OPPORTUNITY_BRIEFS_REFRESH_MINUTES) * 60)

    if not force and not _acquire_refresh_lease(lease_ttl):
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

    if not _client or not _runtime_opportunity_briefs_feature_enabled():
        diagnostics["exitReason"] = "ai_disabled_or_unavailable"
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
        candidates = actionable.get_business_opportunity_brief_candidates(
            days=config.OPPORTUNITY_BRIEFS_WINDOW_DAYS,
            limit_topics=config.OPPORTUNITY_BRIEFS_MAX_TOPICS,
            evidence_per_topic=config.OPPORTUNITY_BRIEFS_EVIDENCE_PER_TOPIC,
        )
    except Exception as e:
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
    if not candidates:
        diagnostics["exitReason"] = "no_candidates"
        _store_refresh_diagnostics(diagnostics)
        return []

    clusters = _build_clusters(candidates, diagnostics=diagnostics)
    if not clusters:
        diagnostics["exitReason"] = "no_clusters_after_support_gate"
        _store_refresh_diagnostics(diagnostics)
        return []

    state = _load_state()
    cluster_state = state.get("clusters") if isinstance(state.get("clusters"), dict) else {}
    active_ids = {_as_str(cluster.get("clusterId")) for cluster in clusters if _as_str(cluster.get("clusterId"))}

    changed_clusters: list[dict] = []
    fingerprints: dict[str, str] = {}
    for cluster in clusters:
        cluster_id = _as_str(cluster.get("clusterId"), "")
        if not cluster_id:
            continue
        fingerprint = _cluster_fingerprint(cluster)
        fingerprints[cluster_id] = fingerprint
        record = cluster_state.get(cluster_id) if isinstance(cluster_state, dict) else None
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

    cards_by_cluster = {_as_str(card.get("clusterId"), ""): card for card in new_cards if _as_str(card.get("clusterId"), "")}
    next_cluster_state: dict[str, dict] = {}

    if isinstance(cluster_state, dict):
        for cluster_id, record in cluster_state.items():
            if cluster_id not in active_ids or not isinstance(record, dict) or force:
                continue
            expected = fingerprints.get(cluster_id)
            if expected and _as_str(record.get("fingerprint"), "") == expected:
                next_cluster_state[cluster_id] = record

    for cluster in changed_clusters:
        cluster_id = _as_str(cluster.get("clusterId"), "")
        if not cluster_id:
            continue
        card = cards_by_cluster.get(cluster_id)
        if card:
            next_cluster_state[cluster_id] = {
                "fingerprint": fingerprints.get(cluster_id, ""),
                "status": "accepted",
                "updatedAt": _now_iso(),
                "card": card,
                "topic": _as_str(cluster.get("topic"), ""),
            }
            continue
        decision = triage.get(cluster_id, {})
        next_cluster_state[cluster_id] = {
            "fingerprint": fingerprints.get(cluster_id, ""),
            "status": "rejected",
            "updatedAt": _now_iso(),
            "rejectionReason": _as_str(decision.get("rejectionReason"), "insufficient_grounding"),
            "topic": _as_str(cluster.get("topic"), ""),
        }

    final_cards: list[dict] = []
    for cluster in clusters:
        cluster_id = _as_str(cluster.get("clusterId"), "")
        record = next_cluster_state.get(cluster_id)
        if not isinstance(record, dict) or _as_str(record.get("status")) != "accepted":
            continue
        card = record.get("card")
        if isinstance(card, dict):
            final_cards.append(card)

    final_cards.sort(
        key=lambda card: (
            _readiness_rank(_as_str(card.get("readiness"), "")),
            _as_int(((card.get("demandSignals") or {}).get("messages")), 0),
            _as_int(((card.get("demandSignals") or {}).get("uniqueUsers")), 0),
            _as_float(card.get("confidenceScore"), 0.0),
            _as_int(((card.get("demandSignals") or {}).get("trend7dPct")), 0),
        ),
        reverse=True,
    )
    final_cards = final_cards[: int(config.OPPORTUNITY_BRIEFS_MAX_BRIEFS)]

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
    if state_saved and snapshot_saved:
        with _cache_lock:
            _cached_cards = final_cards
            _cache_ts = time.time()
        diagnostics["exitReason"] = diagnostics["exitReason"] or ("ok" if final_cards else "zero_cards_after_materialization")
        result_cards = final_cards
    else:
        diagnostics["exitReason"] = "persistence_verification_failed"
        result_cards = last_good_cards

    _store_refresh_diagnostics(diagnostics)
    logger.info(
        "Opportunity cards materialized | cards={} active_clusters={} changed_clusters={} reused_clusters={} accepted_clusters={} synthesized_rows={} first_rejection_bucket={}".format(
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


def get_business_opportunity_briefs(*, force_refresh: bool = False) -> list[dict]:
    global _cached_cards, _cache_ts
    if force_refresh:
        return refresh_opportunity_briefs(force=True)
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
