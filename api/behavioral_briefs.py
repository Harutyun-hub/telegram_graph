"""AI-generated behavioral cards for W8 (problems) and W9 (service gaps)."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger

import config
from api.admin_runtime import get_admin_prompt, get_admin_runtime_value
from api.queries import behavioral
from buffer.supabase_writer import SupabaseWriter

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]


_cache_lock = threading.Lock()
_cached_payload: dict = {"problemBriefs": [], "serviceGapBriefs": []}
_cache_ts: float = 0.0
_state_cache: dict = {}
_last_refresh_diagnostics: dict = {}

_runtime_store_lock = threading.Lock()
_runtime_store: SupabaseWriter | None = None

_SNAPSHOT_FOLDER = "behavioral_cards/snapshots"
_STATE_FOLDER = "behavioral_cards/state"
_LOCK_FOLDER = "behavioral_cards/locks"
_SCHEMA_VERSION = 1
_INSTANCE_ID = f"{os.getpid()}-{int(time.time())}"

_client = OpenAI(api_key=config.OPENAI_API_KEY) if (OpenAI and config.OPENAI_API_KEY) else None

BEHAVIORAL_PROBLEM_PROMPT = """
You generate Problem Tracker cards from evidence clusters.

Rules:
1) Use only the provided evidence text and evidence IDs.
2) Write one societal problem statement (not taxonomy title).
3) Keep wording concrete and human-readable for non-experts.
4) Do not invent causes, actors, or numbers.
5) If grounding is weak, set confidence to low.

Return JSON only:
{
  "card": {
    "clusterId": "string",
    "problemEn": "string",
    "problemRu": "string",
    "summaryEn": "string",
    "summaryRu": "string",
    "severity": "critical|high|medium|low",
    "confidence": "high|medium|low",
    "confidenceScore": 0.0,
    "evidenceIds": ["id1", "id2"]
  }
}
""".strip()

BEHAVIORAL_SERVICE_GAP_PROMPT = """
You identify hidden service-gap requests from evidence clusters.

Rules:
1) Use only provided evidence and IDs.
2) Infer a concrete service/help need from the messages, not a topic label.
3) Service need must be actionable and specific, for example legal help with residency paperwork, mental health counseling, job placement help, housing repair support.
4) Use the topic/category only as retrieval context; ignore them if the messages do not support a real service request.
5) Reject abstract grievances, political dissatisfaction, slogans, identity statements, and broad complaints that are not service requests.
6) Keep unmet reason factual, concise, and grounded in the evidence.
7) If grounding is weak or there is no real service request, return {"card": null}.

Return JSON only:
{
  "card": null
  OR
  "card": {
    "clusterId": "string",
    "serviceNeedEn": "string",
    "serviceNeedRu": "string",
    "unmetReasonEn": "string",
    "unmetReasonRu": "string",
    "urgency": "critical|high|medium|low",
    "unmetPct": 0,
    "confidence": "high|medium|low",
    "confidenceScore": 0.0,
    "evidenceIds": ["id1", "id2"]
  }
}
""".strip()

BEHAVIORAL_URGENCY_PROMPT = """
You analyze urgent community messages and generate Emotional Urgency cards.

Rules:
1) Summarize the specific crisis accurately based ONLY on the evidence.
2) Propose a concrete, actionable step for a community manager or moderator.
3) Keep wording concise (1 short sentence for message, 1 short action).
4) Do not invent details not present in the text.
5) Determine urgency as 'critical' (life/safety/immediate risk) or 'high'.

Return JSON only:
{
  "card": {
    "topicEn": "string",
    "topicRu": "string",
    "messageEn": "string",
    "messageRu": "string",
    "actionEn": "string",
    "actionRu": "string",
    "urgency": "critical|high"
  }
}
""".strip()

ADMIN_PROMPT_DEFAULTS = {
    "behavioral_briefs.problem_prompt": BEHAVIORAL_PROBLEM_PROMPT,
    "behavioral_briefs.service_gap_prompt": BEHAVIORAL_SERVICE_GAP_PROMPT,
    "behavioral_briefs.urgency_prompt": BEHAVIORAL_URGENCY_PROMPT,
}

_TOKEN_RE = re.compile(r"[a-zA-Z0-9а-яА-ЯёЁ]+")
_ASK_HINTS = (
    "need help",
    "looking for",
    "where can i",
    "how to get",
    "can anyone recommend",
    "please help",
    "нужна помощь",
    "нужен совет",
    "подскажите",
    "где найти",
    "как получить",
    "кто может помочь",
    "помогите",
    "подскаж",
    "услуг",
)
_GENERIC_SERVICE_KEYWORDS = (
    "service",
    "services",
    "help",
    "support",
    "assistance",
    "guidance",
    "consultation",
    "referral",
    "recommend",
    "appointment",
    "doctor",
    "clinic",
    "hospital",
    "medicine",
    "medication",
    "therapy",
    "therapist",
    "psycholog",
    "counsel",
    "lawyer",
    "legal",
    "attorney",
    "notary",
    "document",
    "documents",
    "paperwork",
    "passport",
    "visa",
    "residency",
    "registration",
    "translator",
    "translation",
    "housing",
    "rent",
    "apartment",
    "landlord",
    "shelter",
    "repair",
    "water",
    "electric",
    "utility",
    "job",
    "work",
    "vacancy",
    "resume",
    "cv",
    "interview",
    "course",
    "training",
    "school",
    "kindergarten",
    "teacher",
    "transport",
    "bus",
    "taxi",
    "ticket",
    "route",
    "benefit",
    "allowance",
    "aid",
    "subsidy",
    "portal",
    "website",
    "application",
    "license",
    "permit",
    "childcare",
    "caregiver",
    "врач",
    "клиник",
    "больниц",
    "лекар",
    "медицин",
    "терап",
    "психолог",
    "юрист",
    "адвокат",
    "нотари",
    "документ",
    "бумаг",
    "паспорт",
    "виза",
    "внж",
    "регистрац",
    "перевод",
    "жиль",
    "аренд",
    "квартир",
    "приют",
    "ремонт",
    "вода",
    "свет",
    "работ",
    "ваканс",
    "резюме",
    "курс",
    "обуч",
    "школ",
    "садик",
    "транспорт",
    "автобус",
    "такси",
    "билет",
    "пособ",
    "льгот",
    "субсид",
    "сайт",
    "портал",
    "заявлен",
    "лиценз",
    "разреш",
    "уход",
    "поддерж",
    "помощ",
)
_SERVICE_KEYWORDS_BY_CATEGORY = {
    "Healthcare": (
        "doctor",
        "clinic",
        "hospital",
        "medicine",
        "therapy",
        "health",
        "врач",
        "клиник",
        "больниц",
        "мед",
        "леч",
        "терап",
        "педиатр",
    ),
    "Housing & Infrastructure": (
        "housing",
        "rent",
        "landlord",
        "apartment",
        "water",
        "electric",
        "road",
        "infrastructure",
        "жиль",
        "аренд",
        "квартир",
        "дом",
        "свет",
        "вода",
        "дорог",
        "инфраструкт",
    ),
    "Education": (
        "school",
        "kindergarten",
        "university",
        "course",
        "teacher",
        "образов",
        "школ",
        "садик",
        "универс",
        "курс",
        "учител",
    ),
    "Transportation": (
        "transport",
        "bus",
        "metro",
        "taxi",
        "route",
        "ticket",
        "транспорт",
        "автобус",
        "метро",
        "такси",
        "маршрут",
        "билет",
    ),
    "Employment": (
        "job",
        "work",
        "salary",
        "vacancy",
        "employment",
        "работ",
        "ваканс",
        "зарплат",
        "трудоустр",
    ),
    "Cost Of Living": (
        "price",
        "cost",
        "expensive",
        "affordable",
        "inflation",
        "цен",
        "стоим",
        "дорог",
        "доступн",
        "инфляц",
    ),
    "Social Services": (
        "benefit",
        "support",
        "aid",
        "allowance",
        "social",
        "льгот",
        "пособ",
        "поддерж",
        "соц",
        "помощ",
    ),
    "Digital Services": (
        "online",
        "portal",
        "website",
        "app",
        "digital",
        "payment",
        "сайт",
        "портал",
        "прилож",
        "цифр",
        "оплат",
    ),
    "Business & Enterprise": (
        "business",
        "startup",
        "license",
        "register",
        "permit",
        "предпр",
        "бизн",
        "стартап",
        "лиценз",
        "регистра",
    ),
    "Family & Relationships": (
        "child",
        "childcare",
        "family",
        "parent",
        "kindergarten",
        "ребен",
        "дет",
        "сем",
        "родит",
        "садик",
    ),
    "Immigration To Armenia": (
        "visa",
        "residency",
        "document",
        "migration",
        "permit",
        "виза",
        "внж",
        "документ",
        "миграц",
        "регистра",
    ),
}


def _new_refresh_diagnostics(*, force: bool = False) -> dict:
    return {
        "force": bool(force),
        "exitReason": "",
        "error": "",
        "runtime": {
            "hasOpenAIClient": bool(_client),
            "featureEnabled": bool(_runtime_behavioral_feature_enabled()),
        },
        "stages": {
            "problemCandidateRows": 0,
            "serviceCandidateRows": 0,
            "urgencyCandidateRows": 0,
            "problemClusters": 0,
            "serviceClusters": 0,
            "changedProblemClusters": 0,
            "changedServiceClusters": 0,
            "problemCards": 0,
            "serviceCards": 0,
            "urgencyCards": 0,
        },
        "snapshot": {
            "loadedProblemCards": 0,
            "loadedServiceCards": 0,
            "loadedUrgencyCards": 0,
            "writeAttempted": False,
            "writeSucceeded": False,
            "readbackProblemCards": 0,
            "readbackServiceCards": 0,
            "readbackUrgencyCards": 0,
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


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _clamp(num: float, low: float, high: float) -> float:
    return max(low, min(high, num))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _trim_text(value: Any, limit: int) -> str:
    text = " ".join(_as_str(value, "").split())
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


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
    return slug or "brief"


def _tokenize(text: str) -> list[str]:
    return [t.lower() for t in _TOKEN_RE.findall(_as_str(text, ""))]


def _parse_ts(value: Any) -> datetime:
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


def _service_keywords_for_cluster(topic: str, category: str) -> tuple[str, ...]:
    base = list(_GENERIC_SERVICE_KEYWORDS)
    for keyword in _SERVICE_KEYWORDS_BY_CATEGORY.get(_as_str(category, ""), ()):
        if keyword not in base:
            base.append(keyword)
    topic_text = " ".join([_as_str(topic, ""), _as_str(category, "")]).lower()
    if any(keyword in topic_text for keyword in _SERVICE_KEYWORDS_BY_CATEGORY.get(_as_str(category, ""), ())):
        topic_tokens = [t for t in _tokenize(topic) if len(t) >= 4]
        for token in topic_tokens[:6]:
            if token not in base:
                base.append(token)
    return tuple(base)


def _is_service_evidence_aligned(
    *,
    topic: str,
    category: str,
    message: str,
    context: str,
    ask_like: int,
    support_intent: int,
) -> bool:
    txt = (_as_str(message, "") + " " + _as_str(context, "")).lower()
    if not txt.strip():
        return False

    keywords = _service_keywords_for_cluster(topic, category)
    keyword_hit = any(k in txt for k in keywords)
    ask_like_hit = int(ask_like) > 0 and any(
        h in txt for h in ("need", "help", "looking", "нуж", "ищ", "подскаж", "помог")
    )
    ask_hit = int(support_intent) > 0 or ask_like_hit or any(h in txt for h in _ASK_HINTS)
    concrete_need_hit = any(
        marker in txt
        for marker in (
            "need ",
            "help with",
            "recommend",
            "referral",
            "appointment",
            "apply",
            "register",
            "hire",
            "consult",
            "where can i",
            "how do i",
            "need a",
            "need an",
            "кто может",
            "где найти",
            "как получить",
            "нужен",
            "нужна",
            "нужно",
            "помощь с",
            "подскаж",
        )
    )
    return ask_hit and keyword_hit and concrete_need_hit


def _get_runtime_store() -> SupabaseWriter | None:
    global _runtime_store
    with _runtime_store_lock:
        if _runtime_store is not None:
            return _runtime_store
        try:
            _runtime_store = SupabaseWriter()
        except Exception as e:
            logger.warning(f"Behavioral cards runtime store unavailable: {e}")
            _runtime_store = None
    return _runtime_store


def _read_latest_runtime_json(folder: str, default: dict | None = None) -> dict:
    fallback = default if isinstance(default, dict) else {}
    store = _get_runtime_store()
    if not store:
        return dict(fallback)

    rows = store.list_runtime_files(folder)
    if not rows:
        return dict(fallback)

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


def get_admin_prompt_defaults() -> dict[str, str]:
    return dict(ADMIN_PROMPT_DEFAULTS)


def _runtime_prompt(key: str, default: str) -> str:
    return get_admin_prompt(key, default)


def _runtime_behavioral_briefs_model() -> str:
    value = get_admin_runtime_value("behavioralBriefsModel", config.BEHAVIORAL_BRIEFS_MODEL)
    text = _as_str(value, "").strip()
    return text or _as_str(config.BEHAVIORAL_BRIEFS_MODEL)


def _runtime_behavioral_prompt_version() -> str:
    value = get_admin_runtime_value("behavioralBriefsPromptVersion", getattr(config, "BEHAVIORAL_BRIEFS_PROMPT_VERSION", "behavior-v1"))
    text = _as_str(value, "").strip()
    return text or _as_str(getattr(config, "BEHAVIORAL_BRIEFS_PROMPT_VERSION", "behavior-v1"))


def _runtime_behavioral_feature_enabled() -> bool:
    value = get_admin_runtime_value("featureBehavioralBriefsAi", config.FEATURE_BEHAVIORAL_BRIEFS_AI)
    if isinstance(value, bool):
        return value
    return bool(config.FEATURE_BEHAVIORAL_BRIEFS_AI)


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

    own_name = key.rsplit("/", 1)[1]
    latest_name = _as_str(latest.get("name"), "")
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
            "problemClusters": {},
            "serviceClusters": {},
        },
    )
    if not isinstance(state, dict):
        state = {
            "schemaVersion": _SCHEMA_VERSION,
            "updatedAt": None,
            "problemClusters": {},
            "serviceClusters": {},
        }
    if not isinstance(state.get("problemClusters"), dict):
        state["problemClusters"] = {}
    if not isinstance(state.get("serviceClusters"), dict):
        state["serviceClusters"] = {}
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
    logger.error("Behavioral cards state persistence failed verification")
    return False


def _load_snapshot_payload(*, diagnostics: dict | None = None) -> dict:
    payload = _read_latest_runtime_json(
        _SNAPSHOT_FOLDER,
        default={"problemBriefs": [], "serviceGapBriefs": []},
    )
    if not isinstance(payload, dict):
        return {"problemBriefs": [], "serviceGapBriefs": [], "urgencyBriefs": []}
    problems = payload.get("problemBriefs") if isinstance(payload.get("problemBriefs"), list) else []
    services = payload.get("serviceGapBriefs") if isinstance(payload.get("serviceGapBriefs"), list) else []
    urgency = payload.get("urgencyBriefs") if isinstance(payload.get("urgencyBriefs"), list) else []
    if isinstance(diagnostics, dict):
        snapshot = diagnostics.setdefault("snapshot", {})
        snapshot["loadedProblemCards"] = len(problems)
        snapshot["loadedServiceCards"] = len(services)
        snapshot["loadedUrgencyCards"] = len(urgency)
    return {"problemBriefs": problems, "serviceGapBriefs": services, "urgencyBriefs": urgency}


def _save_snapshot_payload(payload: dict, metadata: dict | None = None, diagnostics: dict | None = None) -> bool:
    out = {
        "generatedAt": _now_iso(),
        "source": "materialized",
        "problemBriefs": payload.get("problemBriefs") if isinstance(payload.get("problemBriefs"), list) else [],
        "serviceGapBriefs": payload.get("serviceGapBriefs") if isinstance(payload.get("serviceGapBriefs"), list) else [],
        "urgencyBriefs": payload.get("urgencyBriefs") if isinstance(payload.get("urgencyBriefs"), list) else [],
    }
    if isinstance(metadata, dict) and metadata:
        out["meta"] = metadata
    if isinstance(diagnostics, dict):
        diagnostics.setdefault("snapshot", {})["writeAttempted"] = True
    saved = _write_versioned_runtime_json(_SNAPSHOT_FOLDER, out)
    readback = _load_snapshot_payload(diagnostics=diagnostics) if saved else {}
    readback_ok = (
        len(readback.get("problemBriefs") or []) == len(out["problemBriefs"])
        and len(readback.get("serviceGapBriefs") or []) == len(out["serviceGapBriefs"])
        and len(readback.get("urgencyBriefs") or []) == len(out["urgencyBriefs"])
    )
    if saved and not readback_ok:
        logger.error(
            "Behavioral cards snapshot write verified key but latest snapshot readback mismatched | expected_problems={} readback_problems={} expected_services={} readback_services={} expected_urgency={} readback_urgency={}",
            len(out["problemBriefs"]),
            len(readback.get("problemBriefs") or []),
            len(out["serviceGapBriefs"]),
            len(readback.get("serviceGapBriefs") or []),
            len(out["urgencyBriefs"]),
            len(readback.get("urgencyBriefs") or []),
        )
    if isinstance(diagnostics, dict):
        snapshot = diagnostics.setdefault("snapshot", {})
        snapshot["readbackProblemCards"] = len(readback.get("problemBriefs") or [])
        snapshot["readbackServiceCards"] = len(readback.get("serviceGapBriefs") or [])
        snapshot["readbackUrgencyCards"] = len(readback.get("urgencyBriefs") or [])
        snapshot["writeSucceeded"] = bool(saved and readback_ok)
    return bool(saved and readback_ok)


def _confidence_label(score: float) -> str:
    if score >= 0.78:
        return "high"
    if score >= max(0.5, float(config.BEHAVIORAL_BRIEFS_MIN_CONFIDENCE)):
        return "medium"
    return "low"


def _cluster_fingerprint(kind: str, cluster: dict) -> str:
    payload = {
        "kind": kind,
        "topic": _as_str(cluster.get("topic")),
        "category": _as_str(cluster.get("category")),
        "messages": _as_int(cluster.get("messages"), 0),
        "uniqueUsers": _as_int(cluster.get("uniqueUsers"), 0),
        "channels": _as_int(cluster.get("channels"), 0),
        "trend7dPct": _as_int(cluster.get("trend7dPct"), 0),
        "latestAt": _as_str(cluster.get("latestAt")),
        "severity": _as_str(cluster.get("severity")),
        "unmetPct": _as_int(cluster.get("unmetPct"), 0),
        "promptVersion": _runtime_behavioral_prompt_version(),
        "model": _runtime_behavioral_briefs_model(),
        "signals": [
            {
                "id": _as_str(s.get("id")),
                "message": _trim_text(s.get("message"), 220),
                "context": _trim_text(s.get("context"), 140),
                "channel": _as_str(s.get("channel")),
                "kind": _as_str(s.get("kind")),
                "timestamp": _as_str(s.get("timestamp")),
            }
            for s in (cluster.get("signals") or [])[: int(config.BEHAVIORAL_BRIEFS_EVIDENCE_PER_TOPIC)]
        ],
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _normalize_candidates(rows: list[dict], kind: str) -> list[dict]:
    clusters: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        topic = _as_str(row.get("topic"), "").strip()
        if not topic:
            continue

        evidence_rows = []
        for ev in row.get("evidence") or []:
            if not isinstance(ev, dict):
                continue
            sid = _as_str(ev.get("id"), "").strip()
            msg = _trim_text(ev.get("text"), 680)
            if not sid or not msg:
                continue
            context = _trim_text(ev.get("parentText"), 380)
            ask_like = 1 if _as_int(ev.get("askLike"), 0) > 0 else 0
            support_intent = 1 if _as_int(ev.get("supportIntent"), 0) > 0 else 0

            if kind == "service" and not _is_service_evidence_aligned(
                topic=topic,
                category=_as_str(row.get("category"), "General"),
                message=msg,
                context=context,
                ask_like=ask_like,
                support_intent=support_intent,
            ):
                continue

            evidence_rows.append(
                {
                    "id": sid,
                    "kind": _as_str(ev.get("kind"), "message"),
                    "channel": _as_str(ev.get("channel"), "unknown"),
                    "userId": _as_str(ev.get("userId"), ""),
                    "timestamp": _as_str(ev.get("timestamp"), ""),
                    "message": msg,
                    "context": context,
                    "label": _as_str(ev.get("label"), ""),
                    "distressHit": 1 if _as_int(ev.get("distressHit"), 0) > 0 else 0,
                    "askLike": ask_like,
                    "supportIntent": support_intent,
                }
            )
        if len(evidence_rows) < 2:
            continue

        parsed_timestamps = [_parse_ts(s.get("timestamp")) for s in evidence_rows if _as_str(s.get("timestamp"), "").strip()]
        row_latest_ts = _parse_ts(row.get("latestAt"))
        reference_now = max(
            [dt for dt in parsed_timestamps if dt is not None] + ([row_latest_ts] if row_latest_ts is not None else []),
            default=datetime.now(timezone.utc),
        )
        user_keys = {
            (_as_str(s.get("userId"), "").strip() or f"channel:{_as_str(s.get('channel'), 'unknown').strip().lower()}")
            for s in evidence_rows
        }
        user_keys = {u for u in user_keys if u}
        channels = {_as_str(s.get("channel"), "unknown").strip().lower() for s in evidence_rows if _as_str(s.get("channel"), "").strip()}
        signals7d = sum(1 for s in evidence_rows if (reference_now - _parse_ts(s.get("timestamp"))).days < 7)
        signals_prev7d = sum(1 for s in evidence_rows if 7 <= (reference_now - _parse_ts(s.get("timestamp"))).days < 14)

        latest_ts = ""
        if evidence_rows:
            latest_ts = max((_as_str(s.get("timestamp"), "") for s in evidence_rows), default="")

        base = {
            "clusterId": ("pb-" if kind == "problem" else "sg-") + _slugify(topic),
            "topic": topic,
            "category": _as_str(row.get("category"), "General"),
            "messages": len(evidence_rows),
            "uniqueUsers": len(user_keys),
            "channels": len(channels),
            "signals7d": signals7d,
            "signalsPrev7d": signals_prev7d,
            "trend7dPct": _trend_pct(signals7d, signals_prev7d),
            "latestAt": latest_ts or _as_str(row.get("latestAt"), ""),
            "signals": evidence_rows[: int(config.BEHAVIORAL_BRIEFS_EVIDENCE_PER_TOPIC)],
        }
        if kind == "problem":
            severity = _as_str(row.get("severity"), "medium").strip().lower()
            if severity not in {"critical", "high", "medium", "low"}:
                severity = "medium"
            base["severity"] = severity
        else:
            base["unmetPct"] = int(_clamp(round(_as_float(row.get("unmetPct"), 0.0)), 0, 100))
        clusters.append(base)

    clusters.sort(
        key=lambda c: (
            _as_int(c.get("messages"), 0),
            _as_int(c.get("uniqueUsers"), 0),
            _as_int(c.get("channels"), 0),
        ),
        reverse=True,
    )
    return clusters[: int(config.BEHAVIORAL_BRIEFS_MAX_TOPICS)]


def _support_gate(cluster: dict, kind: str) -> bool:
    min_messages = max(4, int(config.BEHAVIORAL_BRIEFS_MIN_MESSAGES))
    min_users = max(2, int(config.BEHAVIORAL_BRIEFS_MIN_USERS))
    min_channels = max(1, int(config.BEHAVIORAL_BRIEFS_MIN_CHANNELS))
    messages = _as_int(cluster.get("messages"), 0)
    users = _as_int(cluster.get("uniqueUsers"), 0)
    channels = _as_int(cluster.get("channels"), 0)
    trend = _as_int(cluster.get("trend7dPct"), 0)

    if kind == "service":
        service_ok = messages >= 3 and users >= 1 and channels >= 1
        return service_ok and _as_int(cluster.get("unmetPct"), 0) >= 45

    base_ok = messages >= min_messages and users >= min_users and channels >= min_channels
    momentum_ok = messages >= max(4, min_messages - 2) and users >= max(2, min_users - 1) and channels >= min_channels and trend >= 35
    if not (base_ok or momentum_ok):
        return False
    return True


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


def _severity_from_unmet(unmet_pct: int) -> str:
    if unmet_pct >= 85:
        return "critical"
    if unmet_pct >= 70:
        return "high"
    if unmet_pct >= 55:
        return "medium"
    return "low"


def _deterministic_problem_cards(clusters: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for cluster in clusters[: int(config.BEHAVIORAL_BRIEFS_MAX_CARDS)]:
        evidence = cluster.get("signals") or []
        evidence_ids = [_as_str(ev.get("id")) for ev in evidence[:3] if _as_str(ev.get("id"))]
        if len(evidence_ids) < 2:
            continue
        topic = _as_str(cluster.get("topic"), "Topic")
        severity = _as_str(cluster.get("severity"), "medium")
        rows.append(
            {
                "clusterId": _as_str(cluster.get("clusterId"), ""),
                "problemEn": f"Persistent stress around {topic}",
                "problemRu": f"Устойчивая проблема вокруг темы: {topic}",
                "summaryEn": "Recurring negative and urgent signals indicate a sustained community pain point.",
                "summaryRu": "Повторяющиеся негативные и срочные сигналы указывают на устойчивую боль сообщества.",
                "severity": severity,
                "confidence": "medium",
                "confidenceScore": 0.64,
                "evidenceIds": evidence_ids,
            }
        )
    return rows


def _deterministic_service_cards(clusters: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for cluster in clusters[: int(config.BEHAVIORAL_BRIEFS_MAX_CARDS)]:
        evidence = cluster.get("signals") or []
        evidence_ids = [_as_str(ev.get("id")) for ev in evidence[:3] if _as_str(ev.get("id"))]
        if len(evidence_ids) < 2:
            continue
        topic = _as_str(cluster.get("topic"), "Service")
        unmet_pct = _as_int(cluster.get("unmetPct"), 0)
        urgency = _severity_from_unmet(unmet_pct)
        rows.append(
            {
                "clusterId": _as_str(cluster.get("clusterId"), ""),
                "serviceNeedEn": f"Practical help needed in {topic}",
                "serviceNeedRu": f"Нужна практическая помощь по теме: {topic}",
                "unmetReasonEn": "People ask repeatedly but available help appears insufficient.",
                "unmetReasonRu": "Люди регулярно спрашивают, но доступной помощи недостаточно.",
                "urgency": urgency,
                "unmetPct": unmet_pct,
                "confidence": "medium",
                "confidenceScore": 0.62,
                "evidenceIds": evidence_ids,
            }
        )
    return rows


def _synthesize_problem_cards(clusters: list[dict]) -> list[dict]:
    if not clusters:
        return []
    if not _client or not _runtime_behavioral_feature_enabled():
        return _deterministic_problem_cards(clusters)
    system_prompt = _runtime_prompt("behavioral_briefs.problem_prompt", BEHAVIORAL_PROBLEM_PROMPT)

    out: list[dict] = []
    for cluster in clusters[: int(config.BEHAVIORAL_BRIEFS_MAX_CARDS) * 2]:
        payload = {
            "cluster": {
                "clusterId": cluster.get("clusterId"),
                "topic": cluster.get("topic"),
                "category": cluster.get("category"),
                "messages": cluster.get("messages"),
                "uniqueUsers": cluster.get("uniqueUsers"),
                "channels": cluster.get("channels"),
                "trend7dPct": cluster.get("trend7dPct"),
                "severity": cluster.get("severity"),
                "evidence": [
                    {
                        "id": s.get("id"),
                        "message": _trim_text(s.get("message"), 220),
                        "context": _trim_text(s.get("context"), 120),
                        "channel": s.get("channel"),
                        "timestamp": s.get("timestamp"),
                    }
                    for s in (cluster.get("signals") or [])[:6]
                ],
            }
        }
        try:
            parsed = _chat_json(
                model=_runtime_behavioral_briefs_model(),
                max_tokens=int(config.BEHAVIORAL_BRIEFS_MAX_TOKENS),
                system_prompt=system_prompt,
                user_payload=payload,
            )
            card = parsed.get("card") if isinstance(parsed, dict) else None
            if isinstance(card, dict):
                out.append(card)
        except Exception as e:
            logger.warning(f"Problem cards synthesis failed for {cluster.get('clusterId')}: {e}")

    if not out:
        return _deterministic_problem_cards(clusters)
    return out


def _synthesize_service_cards(clusters: list[dict]) -> list[dict]:
    if not clusters:
        return []
    if not _client or not _runtime_behavioral_feature_enabled():
        return []
    system_prompt = _runtime_prompt("behavioral_briefs.service_gap_prompt", BEHAVIORAL_SERVICE_GAP_PROMPT)

    out: list[dict] = []
    for cluster in clusters[: int(config.BEHAVIORAL_BRIEFS_MAX_CARDS) * 2]:
        payload = {
            "cluster": {
                "clusterId": cluster.get("clusterId"),
                "topic": cluster.get("topic"),
                "category": cluster.get("category"),
                "askSignals": cluster.get("messages"),
                "uniqueUsers": cluster.get("uniqueUsers"),
                "channels": cluster.get("channels"),
                "trend7dPct": cluster.get("trend7dPct"),
                "unmetPct": cluster.get("unmetPct"),
                "evidence": [
                    {
                        "id": s.get("id"),
                        "message": _trim_text(s.get("message"), 220),
                        "context": _trim_text(s.get("context"), 120),
                        "channel": s.get("channel"),
                        "timestamp": s.get("timestamp"),
                    }
                    for s in (cluster.get("signals") or [])[:6]
                ],
            }
        }
        try:
            parsed = _chat_json(
                model=_runtime_behavioral_briefs_model(),
                max_tokens=int(config.BEHAVIORAL_BRIEFS_MAX_TOKENS),
                system_prompt=system_prompt,
                user_payload=payload,
            )
            card = parsed.get("card") if isinstance(parsed, dict) else None
            if isinstance(card, dict):
                out.append(card)
        except Exception as e:
            logger.warning(f"Service gap cards synthesis failed for {cluster.get('clusterId')}: {e}")

    return out


def _synthesize_urgency_cards(clusters: list[dict]) -> list[dict]:
    if not clusters:
        return []
    if not _client or not _runtime_behavioral_feature_enabled():
        return []
    system_prompt = _runtime_prompt("behavioral_briefs.urgency_prompt", BEHAVIORAL_URGENCY_PROMPT)

    out: list[dict] = []
    for cluster in clusters[:4]:
        payload = {
            "cluster": {
                "clusterId": cluster.get("clusterId"),
                "topic": cluster.get("topic"),
                "messages": cluster.get("messages"),
                "uniqueUsers": cluster.get("uniqueUsers"),
                "evidence": [
                    {
                        "message": _trim_text(s.get("message"), 220),
                        "context": _trim_text(s.get("context"), 120),
                        "channel": s.get("channel"),
                    }
                    for s in (cluster.get("signals") or [])[:6]
                ],
            }
        }
        try:
            parsed = _chat_json(
                model=_runtime_behavioral_briefs_model(),
                max_tokens=int(config.BEHAVIORAL_BRIEFS_MAX_TOKENS),
                system_prompt=system_prompt,
                user_payload=payload,
            )
            card = parsed.get("card")
            if isinstance(card, dict):
                card["clusterId"] = cluster.get("clusterId")
                card["count"] = cluster.get("messages", 0)
                if card.get("urgency") not in {"critical", "high"}:
                    card["urgency"] = "high"
                out.append(card)
        except Exception as e:
            logger.warning(f"Urgency cards synthesis failed for {cluster.get('clusterId')}: {e}")

    return out


def _materialize_problem_cards(clusters: list[dict], ai_rows: list[dict]) -> list[dict]:
    by_id = {_as_str(c.get("clusterId")): c for c in clusters}
    cards: list[dict] = []
    for row in ai_rows:
        if not isinstance(row, dict):
            continue
        cid = _as_str(row.get("clusterId"), "")
        cluster = by_id.get(cid)
        if not cluster:
            continue

        confidence_score = _clamp(_as_float(row.get("confidenceScore"), 0.0), 0.0, 1.0)
        confidence = _as_str(row.get("confidence"), "").strip().lower() or _confidence_label(confidence_score)
        if confidence not in {"high", "medium", "low"}:
            confidence = _confidence_label(confidence_score)
        if confidence == "low" or confidence_score < float(config.BEHAVIORAL_BRIEFS_MIN_CONFIDENCE):
            continue

        severity = _as_str(row.get("severity"), _as_str(cluster.get("severity"), "medium")).strip().lower()
        if severity not in {"critical", "high", "medium", "low"}:
            severity = "medium"

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
            continue

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
                "clusterId": cid,
                "id": cid,
                "topic": cluster.get("topic"),
                "category": cluster.get("category"),
                "problemEn": _trim_text(row.get("problemEn"), 180),
                "problemRu": _trim_text(row.get("problemRu"), 220),
                "summaryEn": _trim_text(row.get("summaryEn"), 240),
                "summaryRu": _trim_text(row.get("summaryRu"), 280),
                "severity": severity,
                "confidence": confidence,
                "confidenceScore": round(confidence_score, 2),
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

    cards.sort(
        key=lambda c: (
            _as_int(((c.get("demandSignals") or {}).get("messages")), 0),
            _as_float(c.get("confidenceScore"), 0.0),
        ),
        reverse=True,
    )
    return cards[: int(config.BEHAVIORAL_BRIEFS_MAX_CARDS)]


def _materialize_service_cards(clusters: list[dict], ai_rows: list[dict]) -> list[dict]:
    by_id = {_as_str(c.get("clusterId")): c for c in clusters}
    cards: list[dict] = []
    for row in ai_rows:
        if not isinstance(row, dict):
            continue
        cid = _as_str(row.get("clusterId"), "")
        cluster = by_id.get(cid)
        if not cluster:
            continue

        confidence_score = _clamp(_as_float(row.get("confidenceScore"), 0.0), 0.0, 1.0)
        confidence = _as_str(row.get("confidence"), "").strip().lower() or _confidence_label(confidence_score)
        if confidence not in {"high", "medium", "low"}:
            confidence = _confidence_label(confidence_score)
        if confidence == "low" or confidence_score < float(config.BEHAVIORAL_BRIEFS_MIN_CONFIDENCE):
            continue

        unmet_pct = int(_clamp(_as_float(row.get("unmetPct"), _as_float(cluster.get("unmetPct"), 0.0)), 0, 100))
        urgency = _as_str(row.get("urgency"), _severity_from_unmet(unmet_pct)).strip().lower()
        if urgency not in {"critical", "high", "medium", "low"}:
            urgency = _severity_from_unmet(unmet_pct)

        evidence_by_id = {_as_str(s.get("id")): s for s in cluster.get("signals", [])}
        selected_ids: list[str] = []
        for ev_id in row.get("evidenceIds") or []:
            sid = _as_str(ev_id, "").strip()
            if sid and sid in evidence_by_id and sid not in selected_ids:
                selected_ids.append(sid)
            if len(selected_ids) >= 6:
                break
        if len(selected_ids) < 2:
            continue

        aligned_ids: list[str] = []
        for sid in selected_ids:
            signal = evidence_by_id.get(sid)
            if not signal:
                continue
            if not _is_service_evidence_aligned(
                topic=_as_str(cluster.get("topic"), ""),
                category=_as_str(cluster.get("category"), ""),
                message=_as_str(signal.get("message"), ""),
                context=_as_str(signal.get("context"), ""),
                ask_like=_as_int(signal.get("askLike"), 0),
                support_intent=_as_int(signal.get("supportIntent"), 0),
            ):
                continue
            aligned_ids.append(sid)
        if len(aligned_ids) < 2:
            continue

        evidence_payload = []
        for sid in aligned_ids:
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
                "clusterId": cid,
                "id": cid,
                "topic": cluster.get("topic"),
                "category": cluster.get("category"),
                "serviceNeedEn": _trim_text(row.get("serviceNeedEn"), 180),
                "serviceNeedRu": _trim_text(row.get("serviceNeedRu"), 220),
                "unmetReasonEn": _trim_text(row.get("unmetReasonEn"), 240),
                "unmetReasonRu": _trim_text(row.get("unmetReasonRu"), 280),
                "urgency": urgency,
                "unmetPct": unmet_pct,
                "confidence": confidence,
                "confidenceScore": round(confidence_score, 2),
                "demandSignals": {
                    "messages": _as_int(cluster.get("messages"), 0),
                    "uniqueUsers": _as_int(cluster.get("uniqueUsers"), 0),
                    "channels": _as_int(cluster.get("channels"), 0),
                    "trend7dPct": _as_int(cluster.get("trend7dPct"), 0),
                },
                "sampleEvidenceId": aligned_ids[0],
                "latestAt": _as_str(cluster.get("latestAt"), ""),
                "evidence": evidence_payload,
            }
        )

    cards.sort(
        key=lambda c: (
            _as_int(((c.get("demandSignals") or {}).get("messages")), 0),
            _as_int(c.get("unmetPct"), 0),
            _as_float(c.get("confidenceScore"), 0.0),
        ),
        reverse=True,
    )
    return cards[: int(config.BEHAVIORAL_BRIEFS_MAX_CARDS)]


def _refresh_kind(
    *,
    kind: str,
    clusters: list[dict],
    state_clusters: Any,
    force: bool,
) -> tuple[list[dict], dict, int]:
    active_ids = {_as_str(c.get("clusterId"), "") for c in clusters if _as_str(c.get("clusterId"), "")}
    changed_clusters: list[dict] = []
    fingerprints: dict[str, str] = {}

    for cluster in clusters:
        cid = _as_str(cluster.get("clusterId"), "")
        if not cid:
            continue
        fingerprint = _cluster_fingerprint(kind, cluster)
        fingerprints[cid] = fingerprint
        record = state_clusters.get(cid) if isinstance(state_clusters, dict) else None
        if (not force) and isinstance(record, dict) and _as_str(record.get("fingerprint"), "") == fingerprint:
            continue
        changed_clusters.append(cluster)

    ai_rows: list[dict] = []
    new_cards: list[dict] = []
    if changed_clusters:
        if kind == "problem":
            ai_rows = _synthesize_problem_cards(changed_clusters)
            new_cards = _materialize_problem_cards(changed_clusters, ai_rows)
            if not new_cards:
                new_cards = _materialize_problem_cards(changed_clusters, _deterministic_problem_cards(changed_clusters))
        else:
            ai_rows = _synthesize_service_cards(changed_clusters)
            new_cards = _materialize_service_cards(changed_clusters, ai_rows)

    cards_by_cluster = {
        _as_str(card.get("clusterId"), ""): card
        for card in new_cards
        if isinstance(card, dict) and _as_str(card.get("clusterId"), "")
    }

    next_cluster_state: dict[str, dict] = {}

    if isinstance(state_clusters, dict):
        for cid, record in state_clusters.items():
            if cid not in active_ids or not isinstance(record, dict):
                continue
            if force:
                continue
            expected = fingerprints.get(cid)
            if not expected:
                continue
            if _as_str(record.get("fingerprint"), "") != expected:
                continue
            next_cluster_state[cid] = record

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
        else:
            next_cluster_state[cid] = {
                "fingerprint": fingerprint,
                "status": "rejected",
                "updatedAt": _now_iso(),
                "rejectionReason": "insufficient_grounding",
                "topic": _as_str(cluster.get("topic"), ""),
            }

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
    final_cards = final_cards[: int(config.BEHAVIORAL_BRIEFS_MAX_CARDS)]

    return final_cards, next_cluster_state, len(changed_clusters)


def _cache_valid(now: float) -> bool:
    return (now - _cache_ts) < max(300, int(config.BEHAVIORAL_BRIEFS_CACHE_TTL_SECONDS))


def invalidate_behavioral_briefs_cache() -> None:
    """Clear in-process cache (persistent snapshot remains intact)."""
    global _cached_payload, _cache_ts
    with _cache_lock:
        _cached_payload = {"problemBriefs": [], "serviceGapBriefs": []}
        _cache_ts = 0.0


def get_behavioral_briefs_diagnostics() -> dict:
    """Return the last behavioral materialization diagnostics snapshot."""
    if not _last_refresh_diagnostics:
        return _new_refresh_diagnostics(force=False)
    return copy.deepcopy(_last_refresh_diagnostics)


def refresh_behavioral_briefs_with_diagnostics(*, force: bool = False) -> dict:
    """Run refresh and return diagnostics for debugging."""
    refresh_behavioral_briefs(force=force)
    return get_behavioral_briefs_diagnostics()


def refresh_behavioral_briefs(*, force: bool = False) -> dict:
    """Materialize W8/W9 AI cards and persist snapshot/state for request-time reads."""
    global _cached_payload, _cache_ts
    with _cache_lock:
        last_good_payload = {
            "problemBriefs": list(_cached_payload.get("problemBriefs") or []),
            "serviceGapBriefs": list(_cached_payload.get("serviceGapBriefs") or []),
            "urgencyBriefs": list(_cached_payload.get("urgencyBriefs") or []),
        }
    diagnostics = _new_refresh_diagnostics(force=force)

    lease_ttl = max(300, int(config.BEHAVIORAL_BRIEFS_REFRESH_MINUTES) * 60)
    if not force and not _acquire_refresh_lease(lease_ttl):
        logger.info("Behavioral cards materialization skipped: another instance holds active lease")
        diagnostics["exitReason"] = "lease_skipped"
        payload = _load_snapshot_payload(diagnostics=diagnostics)
        with _cache_lock:
            _cached_payload = payload
            _cache_ts = time.time()
        _store_refresh_diagnostics(diagnostics)
        return payload

    try:
        problem_candidates = behavioral.get_problem_brief_candidates(
            days=config.BEHAVIORAL_BRIEFS_WINDOW_DAYS,
            limit_topics=config.BEHAVIORAL_BRIEFS_MAX_TOPICS,
            evidence_per_topic=config.BEHAVIORAL_BRIEFS_EVIDENCE_PER_TOPIC,
        )
        service_candidates = behavioral.get_service_gap_brief_candidates(
            days=config.BEHAVIORAL_BRIEFS_WINDOW_DAYS,
            limit_topics=config.BEHAVIORAL_BRIEFS_MAX_TOPICS,
            evidence_per_topic=config.BEHAVIORAL_BRIEFS_EVIDENCE_PER_TOPIC,
        )
        urgency_candidates = behavioral.get_urgency_brief_candidates()
    except Exception as e:
        logger.warning(f"Behavioral cards candidate retrieval failed: {e}")
        diagnostics["exitReason"] = "candidate_error"
        diagnostics["error"] = str(e)
        payload = _load_snapshot_payload(diagnostics=diagnostics)
        with _cache_lock:
            _cached_payload = payload
            _cache_ts = time.time()
        _store_refresh_diagnostics(diagnostics)
        return payload
    diagnostics["stages"]["problemCandidateRows"] = len(problem_candidates)
    diagnostics["stages"]["serviceCandidateRows"] = len(service_candidates)
    diagnostics["stages"]["urgencyCandidateRows"] = len(urgency_candidates)

    problem_clusters = [c for c in _normalize_candidates(problem_candidates, "problem") if _support_gate(c, "problem")]
    service_clusters = [c for c in _normalize_candidates(service_candidates, "service") if _support_gate(c, "service")]
    diagnostics["stages"]["problemClusters"] = len(problem_clusters)
    diagnostics["stages"]["serviceClusters"] = len(service_clusters)

    state = _load_state()
    raw_problem_state = state.get("problemClusters")
    raw_service_state = state.get("serviceClusters")
    problem_state: dict = raw_problem_state if isinstance(raw_problem_state, dict) else {}
    service_state: dict = raw_service_state if isinstance(raw_service_state, dict) else {}

    problem_cards, next_problem_state, changed_problem = _refresh_kind(
        kind="problem",
        clusters=problem_clusters,
        state_clusters=problem_state,
        force=force,
    )
    service_cards, next_service_state, changed_service = _refresh_kind(
        kind="service",
        clusters=service_clusters,
        state_clusters=service_state,
        force=force,
    )
    diagnostics["stages"]["changedProblemClusters"] = changed_problem
    diagnostics["stages"]["changedServiceClusters"] = changed_service

    urgency_cards = _synthesize_urgency_cards(urgency_candidates)
    diagnostics["stages"]["problemCards"] = len(problem_cards)
    diagnostics["stages"]["serviceCards"] = len(service_cards)
    diagnostics["stages"]["urgencyCards"] = len(urgency_cards)

    payload = {
        "problemBriefs": problem_cards,
        "serviceGapBriefs": service_cards,
        "urgencyBriefs": urgency_cards,
    }

    state["problemClusters"] = next_problem_state
    state["serviceClusters"] = next_service_state
    state_saved = _save_state(state)
    snapshot_saved = False
    if state_saved:
        snapshot_saved = _save_snapshot_payload(
            payload,
            metadata={
                "activeProblemClusters": len(problem_clusters),
                "changedProblemClusters": changed_problem,
                "activeServiceClusters": len(service_clusters),
                "changedServiceClusters": changed_service,
                "problemCards": len(problem_cards),
                "serviceCards": len(service_cards),
            },
            diagnostics=diagnostics,
        )
    if not state_saved:
        diagnostics["error"] = "Behavioral cards state could not be persisted and verified"
    elif not snapshot_saved:
        diagnostics["error"] = "Behavioral cards snapshot could not be persisted and verified"

    if state_saved and snapshot_saved:
        with _cache_lock:
            _cached_payload = payload
            _cache_ts = time.time()
        diagnostics["exitReason"] = "ok"
        result_payload = payload
    else:
        diagnostics["exitReason"] = "persistence_verification_failed"
        result_payload = last_good_payload
        if any(result_payload.values()):
            logger.warning("Behavioral cards refresh kept last known good in-memory cache after persistence verification failed")
        else:
            logger.warning("Behavioral cards refresh produced data but did not replace cache because persistence verification failed")
    _store_refresh_diagnostics(diagnostics)

    logger.info(
        "Behavioral cards materialized | problems={} services={} active_problem_clusters={} active_service_clusters={}".format(
            len(result_payload.get("problemBriefs") or []),
            len(result_payload.get("serviceGapBriefs") or []),
            len(problem_clusters),
            len(service_clusters),
        )
    )
    return result_payload


def get_behavioral_briefs(*, force_refresh: bool = False) -> dict:
    """Read materialized behavioral cards (no live LLM in request path)."""
    global _cached_payload, _cache_ts

    if force_refresh:
        return refresh_behavioral_briefs(force=True)

    now = time.time()
    with _cache_lock:
        if _cached_payload and _cache_valid(now):
            return {
                "problemBriefs": list(_cached_payload.get("problemBriefs") or []),
                "serviceGapBriefs": list(_cached_payload.get("serviceGapBriefs") or []),
                "urgencyBriefs": list(_cached_payload.get("urgencyBriefs") or []),
            }

    payload = _load_snapshot_payload()
    with _cache_lock:
        _cached_payload = payload
        _cache_ts = now
    return {
        "problemBriefs": list(payload.get("problemBriefs") or []),
        "serviceGapBriefs": list(payload.get("serviceGapBriefs") or []),
        "urgencyBriefs": list(payload.get("urgencyBriefs") or []),
    }
